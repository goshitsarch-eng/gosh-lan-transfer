//! Checksum-verified, resumable sending with serializable plans and cancellation handles.
use crate::{
    EngineError, EngineEvent, EngineResult, TransferClient, TransferDecision, TransferDirection,
    TransferFile, TransferProgress, TransferRecord, TransferRequest, TransferStatus,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::{io::ReaderStream, sync::CancellationToken};

/// A file plus its immutable metadata and SHA-256 digest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedFile {
    /// Canonical local source path. Plans must be kept private.
    pub path: PathBuf,
    /// Metadata sent to the receiver.
    pub info: TransferFile,
    /// Lowercase hexadecimal SHA-256 of the complete source file.
    pub sha256: String,
}
/// Save this plan before starting a transfer; load and resend it to resume after restart.
/// Plans contain local paths but never bearer/upload tokens or private keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedTransfer {
    /// Target IP or hostname (without scheme or port).
    pub address: String,
    /// Target port.
    pub port: u16,
    /// Stable session ID, preserved across attempts and restarts.
    pub transfer_id: String,
    /// Friendly sender name.
    pub sender_name: Option<String>,
    /// Files and their checksums.
    pub files: Vec<PreparedFile>,
}
/// Version 2 registration wire format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferManifest {
    /// Original metadata envelope.
    pub request: TransferRequest,
    /// File ID to complete-file SHA-256 digest.
    pub sha256: HashMap<String, String>,
}
/// Durable receive offset for one file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileCheckpoint {
    /// File ID.
    pub file_id: String,
    /// Bytes already present at the receiver; next upload must use this offset.
    pub offset: u64,
    /// True only after SHA-256 verification and publication to the destination.
    pub complete: bool,
}
/// Version 2 approval and checkpoint response. Contains a secret upload token.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeStatus {
    /// Approval decision.
    pub status: TransferDecision,
    /// Upload capability (never include it in logs or URLs).
    pub token: Option<String>,
    /// Whether the transfer was explicitly cancelled.
    pub cancelled: bool,
    /// All files verified and received.
    pub complete: bool,
    /// Current per-file offsets.
    pub files: Vec<FileCheckpoint>,
}
impl PreparedTransfer {
    /// Atomically save this plan with owner-only permissions on Unix.
    pub async fn save(&self, path: impl AsRef<Path>) -> EngineResult<()> {
        self.manifest()?.validate()?;
        save_json(path.as_ref(), self).await
    }
    /// Load a saved plan. Source bytes are verified again before sending.
    pub async fn load(path: impl AsRef<Path>) -> EngineResult<Self> {
        let plan: Self = serde_json::from_slice(&tokio::fs::read(path).await?)?;
        plan.manifest()?.validate()?;
        Ok(plan)
    }
    /// Build the public registration envelope without exposing local source paths.
    pub fn manifest(&self) -> EngineResult<TransferManifest> {
        let total_size = self
            .files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.info.size))
            .ok_or_else(|| EngineError::InvalidConfig("Transfer size overflow".into()))?;
        Ok(TransferManifest {
            request: TransferRequest {
                transfer_id: self.transfer_id.clone(),
                sender_name: self.sender_name.clone(),
                files: self.files.iter().map(|f| f.info.clone()).collect(),
                total_size,
            },
            sha256: self
                .files
                .iter()
                .map(|f| (f.info.id.clone(), f.sha256.clone()))
                .collect(),
        })
    }
}
impl TransferManifest {
    pub(crate) fn validate(&self) -> EngineResult<()> {
        let invalid = || {
            EngineError::InvalidConfig("Invalid resumable manifest: require UUID IDs, unique files, sizes and SHA-256 checksums".into())
        };
        if uuid::Uuid::parse_str(&self.request.transfer_id)
            .map_err(|_| invalid())?
            .to_string()
            != self.request.transfer_id
        {
            return Err(invalid());
        }
        if self.request.files.is_empty() || self.request.files.len() > 10000 {
            return Err(invalid());
        }
        let mut ids = std::collections::HashSet::new();
        for f in &self.request.files {
            if uuid::Uuid::parse_str(&f.id)
                .map_err(|_| invalid())?
                .to_string()
                != f.id
            {
                return Err(invalid());
            }
            if !ids.insert(&f.id) {
                return Err(invalid());
            }
            let hash = self.sha256.get(&f.id).ok_or_else(invalid)?;
            if hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(invalid());
            }
        }
        if self.sha256.len() != ids.len()
            || self
                .request
                .files
                .iter()
                .try_fold(0u64, |n, f| n.checked_add(f.size))
                != Some(self.request.total_size)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
pub(crate) async fn hash_file(path: &Path) -> EngineResult<(String, u64)> {
    let mut file = tokio::fs::File::open(path).await?;
    if !file.metadata().await?.is_file() {
        return Err(EngineError::FileIo("Not a regular file".into()));
    }
    let mut hash = Sha256::new();
    let mut size = 0;
    let mut buf = vec![0u8; 65536];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
        size += n as u64;
    }
    Ok((format!("{:x}", hash.finalize()), size))
}
pub(crate) async fn save_json(path: &Path, value: &impl Serialize) -> EngineResult<()> {
    let bytes = serde_json::to_vec(value)?;
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::io::Write;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        temp.write_all(&bytes)?;
        temp.as_file().sync_all()?;
        temp.persist(&path).map_err(|e| e.error)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })
    .await
    .map_err(|e| EngineError::FileIo(e.to_string()))??;
    Ok(())
}
/// An outgoing transfer running in the background. Dropping it cancels the send.
pub struct TransferHandle {
    id: String,
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<EngineResult<()>>>,
}
impl TransferHandle {
    /// Stable transfer ID, also present in progress/history events.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Stop local work immediately and request receiver cancellation.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    /// Wait for success, failure or cancellation. Dropping this future also cancels.
    pub async fn wait(mut self) -> EngineResult<()> {
        // Keep the task in self while awaiting so dropping this future cancels it.
        let result = self
            .task
            .as_mut()
            .expect("transfer task")
            .await
            .map_err(|e| EngineError::Network(e.to_string()))?;
        self.task.take();
        result
    }
}
impl Drop for TransferHandle {
    fn drop(&mut self) {
        if self.task.is_some() {
            self.cancellation.cancel();
        }
    }
}
impl TransferClient {
    /// Hash files and create a stable plan that can be saved and resumed.
    pub async fn prepare_files(
        &self,
        address: &str,
        port: u16,
        paths: Vec<PathBuf>,
        sender_name: Option<String>,
    ) -> EngineResult<PreparedTransfer> {
        self.prepare_paths(
            address,
            port,
            paths.into_iter().map(|p| (p, None)).collect(),
            sender_name,
        )
        .await
    }
    /// Recursively prepare regular files; symlinks and empty directories are skipped.
    pub async fn prepare_directory(
        &self,
        address: &str,
        port: u16,
        path: &Path,
        sender_name: Option<String>,
    ) -> EngineResult<PreparedTransfer> {
        let mut files = Vec::new();
        Self::collect_directory_files_async(path, path, &mut files).await?;
        self.prepare_paths(
            address,
            port,
            files.into_iter().map(|(p, r)| (p, Some(r))).collect(),
            sender_name,
        )
        .await
    }
    async fn prepare_paths(
        &self,
        address: &str,
        port: u16,
        paths: Vec<(PathBuf, Option<String>)>,
        sender_name: Option<String>,
    ) -> EngineResult<PreparedTransfer> {
        if paths.is_empty() {
            return Err(EngineError::InvalidConfig("No files to send".into()));
        }
        let mut files = Vec::new();
        for (path, relative_path) in paths {
            let path = tokio::fs::canonicalize(path).await?;
            let (sha256, size) = hash_file(&path).await?;
            let name = path
                .file_name()
                .ok_or_else(|| EngineError::FileIo("Invalid filename".into()))?
                .to_string_lossy()
                .into_owned();
            files.push(PreparedFile {
                info: TransferFile {
                    id: uuid::Uuid::new_v4().to_string(),
                    name,
                    size,
                    mime_type: mime_guess::from_path(&path).first().map(|m| m.to_string()),
                    relative_path,
                },
                path,
                sha256,
            });
        }
        Ok(PreparedTransfer {
            address: address.into(),
            port,
            transfer_id: uuid::Uuid::new_v4().to_string(),
            sender_name,
            files,
        })
    }
    /// Start/resume a saved plan and return a cancellation handle.
    pub fn start_prepared(&self, plan: PreparedTransfer) -> TransferHandle {
        let client = self.clone();
        let cancellation = CancellationToken::new();
        let token = cancellation.clone();
        let id = plan.transfer_id.clone();
        let task = tokio::spawn(async move { client.send_prepared_cancel(plan, token).await });
        TransferHandle {
            id,
            cancellation,
            task: Some(task),
        }
    }
    /// Send/resume a plan without detaching it. Use `start_prepared` for cancellation.
    pub async fn send_prepared(&self, plan: PreparedTransfer) -> EngineResult<()> {
        self.send_prepared_cancel(plan, CancellationToken::new())
            .await
    }
    async fn send_prepared_cancel(
        &self,
        plan: PreparedTransfer,
        cancel: CancellationToken,
    ) -> EngineResult<()> {
        let started_at = chrono::Utc::now();
        let bytes = Arc::new(AtomicU64::new(0));
        let work = self.send_plan(&plan, bytes.clone());
        let result = tokio::select! { biased;
            _=cancel.cancelled()=>Err(EngineError::TransferCancelled),
            result=work=>result,
        };
        if matches!(result, Err(EngineError::TransferCancelled)) {
            if let Ok(http) = self.http() {
                let _ = http
                    .post(self.url(&plan.address, plan.port, "/v2/cancel"))
                    .query(&[("transfer_id", &plan.transfer_id)])
                    .timeout(Duration::from_secs(3))
                    .send()
                    .await;
            }
        }
        let total = plan
            .files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.info.size))
            .unwrap_or(0);
        let status = if result.is_ok() {
            TransferStatus::Completed
        } else if matches!(result, Err(EngineError::TransferRejected)) {
            TransferStatus::Rejected
        } else {
            TransferStatus::Failed
        };
        if let Some(history) = &self.history {
            let _ = history.add(TransferRecord {
                id: plan.transfer_id.clone(),
                direction: TransferDirection::Sent,
                status,
                peer_address: plan.address.clone(),
                files: plan.files.iter().map(|f| f.info.clone()).collect(),
                total_size: total,
                bytes_transferred: bytes.load(Ordering::SeqCst).min(total),
                started_at,
                completed_at: Some(chrono::Utc::now()),
                error: result.as_ref().err().map(ToString::to_string),
            });
        }
        self.event_handler.on_event(match &result {
            Ok(()) => EngineEvent::TransferComplete {
                transfer_id: plan.transfer_id,
            },
            Err(e) => EngineEvent::TransferFailed {
                transfer_id: plan.transfer_id,
                error: e.to_string(),
            },
        });
        result
    }
    async fn retry_request(
        &self,
        request: reqwest::RequestBuilder,
        id: &str,
    ) -> EngineResult<reqwest::Response> {
        for attempt in 0..=self.config.max_retries {
            let result = request
                .try_clone()
                .ok_or_else(|| EngineError::InvalidConfig("Request is not replayable".into()))?
                .send()
                .await;
            match result {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    if (status.is_server_error()
                        || status.as_u16() == 408
                        || status.as_u16() == 429)
                        && attempt < self.config.max_retries
                    {
                        self.retry_delay(id, attempt, format!("HTTP {status}"))
                            .await;
                    } else {
                        return Err(http_error(status));
                    }
                }
                Err(error) if attempt < self.config.max_retries => {
                    self.retry_delay(id, attempt, error.without_url().to_string())
                        .await
                }
                Err(error) => return Err(EngineError::Network(error.without_url().to_string())),
            }
        }
        unreachable!()
    }
    async fn retry_delay(&self, id: &str, attempt: u32, error: String) {
        self.event_handler.on_event(EngineEvent::TransferRetry {
            transfer_id: id.into(),
            attempt: attempt + 1,
            max_attempts: self.config.max_retries,
            error,
        });
        let delay = self
            .config
            .retry_delay_ms
            .saturating_mul(2u64.saturating_pow(attempt))
            .min(60_000);
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    async fn checkpoint(&self, plan: &PreparedTransfer) -> EngineResult<ResumeStatus> {
        let status: ResumeStatus = self
            .retry_request(
                self.http()?
                    .get(self.url(&plan.address, plan.port, "/v2/status"))
                    .query(&[("transfer_id", &plan.transfer_id)]),
                &plan.transfer_id,
            )
            .await?
            .json()
            .await?;
        let mut ids = std::collections::HashSet::new();
        if (status.complete && status.files.iter().any(|point| !point.complete))
            || status.files.len() != plan.files.len()
            || status.files.iter().any(|point| {
                !ids.insert(&point.file_id)
                    || !plan.files.iter().any(|f| {
                        f.info.id == point.file_id
                            && point.offset <= f.info.size
                            && (!point.complete || point.offset == f.info.size)
                    })
            })
        {
            return Err(EngineError::Network(
                "Peer returned invalid file checkpoints".into(),
            ));
        }
        Ok(status)
    }

    async fn send_plan(
        &self,
        plan: &PreparedTransfer,
        progress: Arc<AtomicU64>,
    ) -> EngineResult<()> {
        if self.config.receive_only {
            return Err(EngineError::InvalidConfig(
                "Sending is disabled in receive-only mode".into(),
            ));
        }
        let manifest = plan.manifest()?;
        manifest.validate()?;
        for file in &plan.files {
            let (hash, size) = hash_file(&file.path).await?;
            if hash != file.sha256 || size != file.info.size {
                return Err(EngineError::FileIo(
                    "Source file changed since the transfer was prepared".into(),
                ));
            }
        }
        self.retry_request(
            self.http()?
                .post(self.url(&plan.address, plan.port, "/v2/transfer"))
                .json(&manifest),
            &plan.transfer_id,
        )
        .await?;
        let approved = tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let status = self.checkpoint(plan).await?;
                if status.cancelled {
                    return Err(EngineError::TransferCancelled);
                }
                match status.status {
                    TransferDecision::Accepted => return Ok(status),
                    TransferDecision::Rejected => return Err(EngineError::TransferRejected),
                    TransferDecision::NotFound => {
                        return Err(EngineError::TransferNotFound(plan.transfer_id.clone()))
                    }
                    TransferDecision::Pending => {
                        tokio::time::sleep(Duration::from_millis(100)).await
                    }
                }
            }
        })
        .await
        .map_err(|_| EngineError::TransferTimeout)??;
        if approved.complete {
            progress.store(manifest.request.total_size, Ordering::SeqCst);
            return Ok(());
        }
        let token = approved
            .token
            .ok_or_else(|| EngineError::Network("Missing upload token".into()))?;
        let started = std::time::Instant::now();
        for file in &plan.files {
            for attempt in 0..=self.config.max_retries {
                let status = self.checkpoint(plan).await?;
                if status.cancelled {
                    return Err(EngineError::TransferCancelled);
                }
                let checkpoint = status
                    .files
                    .iter()
                    .find(|c| c.file_id == file.info.id)
                    .ok_or_else(|| EngineError::Network("Missing file checkpoint".into()))?;
                let baseline = status.files.iter().map(|f| f.offset).sum::<u64>();
                progress.store(baseline, Ordering::SeqCst);
                if checkpoint.complete {
                    break;
                }
                if checkpoint.offset > file.info.size {
                    return Err(EngineError::Network("Invalid receive offset".into()));
                }
                let mut source = tokio::fs::File::open(&file.path).await?;
                source
                    .seek(std::io::SeekFrom::Start(checkpoint.offset))
                    .await?;
                let handler = self.event_handler.clone();
                let id = plan.transfer_id.clone();
                let name = file.info.name.clone();
                let total = manifest.request.total_size;
                let counter = progress.clone();
                let rate = self.config.bandwidth_limit_bps.unwrap_or(0);
                let throttle = Arc::new(tokio::sync::Mutex::new(crate::throttle::Throttle::new(
                    rate,
                )));
                let last_update = Arc::new(AtomicU64::new(baseline));
                let stream = ReaderStream::new(source)
                    .then(move |chunk| {
                        let throttle = throttle.clone();
                        async move {
                            if let Ok(bytes) = &chunk {
                                throttle.lock().await.pace(bytes.len()).await;
                            }
                            chunk
                        }
                    })
                    .inspect(move |chunk| {
                        if let Ok(chunk) = chunk {
                            let n = counter.fetch_add(chunk.len() as u64, Ordering::SeqCst)
                                + chunk.len() as u64;
                            let last = last_update.load(Ordering::SeqCst);
                            if n.saturating_sub(last) < 32768 && n != total {
                                return;
                            }
                            last_update.store(n, Ordering::SeqCst);
                            handler.on_event(EngineEvent::TransferProgress(TransferProgress {
                                transfer_id: id.clone(),
                                current_file: Some(name.clone()),
                                bytes_transferred: n.min(total),
                                total_bytes: total,
                                speed_bps: (n as f64 / started.elapsed().as_secs_f64().max(0.001))
                                    as u64,
                            }));
                        }
                    });
                let result = self
                    .http()?
                    .post(self.url(&plan.address, plan.port, "/v2/chunk"))
                    .query(&[
                        ("transfer_id", plan.transfer_id.clone()),
                        ("file_id", file.info.id.clone()),
                        ("offset", checkpoint.offset.to_string()),
                    ])
                    .header("X-Transfer-Token", &token)
                    .header("Content-Length", file.info.size - checkpoint.offset)
                    .body(reqwest::Body::wrap_stream(stream))
                    .send()
                    .await;
                match result {
                    Ok(response) if response.status().is_success() => break,
                    Ok(response) => {
                        let status = response.status();
                        if (status.is_server_error()
                            || status.as_u16() == 408
                            || status.as_u16() == 409)
                            && attempt < self.config.max_retries
                        {
                            self.retry_delay(&plan.transfer_id, attempt, format!("HTTP {status}"))
                                .await;
                        } else {
                            return Err(http_error(status));
                        }
                    }
                    Err(error) if attempt < self.config.max_retries => {
                        self.retry_delay(
                            &plan.transfer_id,
                            attempt,
                            error.without_url().to_string(),
                        )
                        .await
                    }
                    Err(error) => {
                        return Err(EngineError::Network(error.without_url().to_string()))
                    }
                }
            }
        }
        let final_status = self.checkpoint(plan).await?;
        if !final_status.complete {
            return Err(EngineError::Network(
                "Receiver has not verified completion".into(),
            ));
        }
        progress.store(manifest.request.total_size, Ordering::SeqCst);
        Ok(())
    }
}
fn http_error(status: reqwest::StatusCode) -> EngineError {
    match status.as_u16(){410=>EngineError::TransferCancelled,422=>EngineError::FileIo("Receiver rejected a checksum mismatch".into()),401|403=>EngineError::InvalidConfig("Peer authentication/authorization failed".into()),404=>EngineError::Network("Peer does not support this resumable session/protocol; use explicit legacy sending for older peers".into()),_=>EngineError::Network(format!("Peer returned HTTP {status}"))}
}
