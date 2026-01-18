// SPDX-License-Identifier: MIT
// gosh-lan-transfer - HTTP client for sending file transfers
//
// The client explicitly resolves hostnames and attempts all IPs.
// This ensures reliable connections over LAN, Tailscale, and VPNs.

use crate::config::EngineConfig;
use crate::error::{EngineError, EngineResult};
use crate::events::EventHandler;
use crate::history::HistoryPersistence;
use crate::protocol::{
    EngineEvent, TransferApprovalStatus, TransferDecision, TransferFile, TransferProgress,
    TransferRequest, TransferResponse,
};
use crate::types::{
    NetworkInterface, ResolveResult, TransferDirection, TransferRecord, TransferStatus,
};
use futures::StreamExt;
use reqwest::{Body, Client};
use std::{
    net::ToSocketAddrs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs::File,
    time::{sleep, Instant},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

/// Client for sending files to a peer
pub struct TransferClient {
    http_client: Client,
    event_handler: Arc<dyn EventHandler>,
    history: Option<Arc<dyn HistoryPersistence>>,
    /// Maximum retry attempts
    max_retries: u32,
    /// Base delay between retries in milliseconds
    retry_delay_ms: u64,
}

/// Parameters for sending a single file
struct SendFileParams<'a> {
    address: &'a str,
    port: u16,
    transfer_id: &'a str,
    token: &'a str,
    file_id: &'a str,
    file_path: &'a Path,
    total_transfer_size: u64,
    bytes_sent_so_far: Arc<AtomicU64>,
    transfer_start_time: Instant,
}

impl TransferClient {
    /// Create a new transfer client with the given event handler
    pub fn new(event_handler: Arc<dyn EventHandler>) -> Self {
        let config = EngineConfig::default();
        Self::new_with_config(event_handler, &config)
    }

    /// Create a new transfer client with config
    pub fn new_with_config(event_handler: Arc<dyn EventHandler>, config: &EngineConfig) -> Self {
        let http_client = Client::builder()
            // No global timeout - large file transfers can take a long time
            // Use read_timeout to detect stalled connections instead
            .read_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            event_handler,
            history: None,
            max_retries: config.max_retries,
            retry_delay_ms: config.retry_delay_ms,
        }
    }

    /// Create a new transfer client with history persistence
    pub fn new_with_history(
        event_handler: Arc<dyn EventHandler>,
        history: Arc<dyn HistoryPersistence>,
    ) -> Self {
        let config = EngineConfig::default();
        Self::new_with_history_and_config(event_handler, history, &config)
    }

    /// Create a new transfer client with history and config
    pub fn new_with_history_and_config(
        event_handler: Arc<dyn EventHandler>,
        history: Arc<dyn HistoryPersistence>,
        config: &EngineConfig,
    ) -> Self {
        let http_client = Client::builder()
            .read_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            http_client,
            event_handler,
            history: Some(history),
            max_retries: config.max_retries,
            retry_delay_ms: config.retry_delay_ms,
        }
    }

    /// Update retry settings from config
    pub fn update_config(&mut self, config: &EngineConfig) {
        self.max_retries = config.max_retries;
        self.retry_delay_ms = config.retry_delay_ms;
    }

    /// Check if an error is transient and should be retried
    fn is_transient_error(error: &EngineError) -> bool {
        matches!(
            error,
            EngineError::Network(_) | EngineError::ConnectionRefused(_)
        )
    }

    /// Resolve a hostname or IP to all available addresses
    pub fn resolve_address(address: &str) -> ResolveResult {
        // First, check if it's already an IP address
        if let Ok(ip) = address.parse::<std::net::IpAddr>() {
            return ResolveResult {
                hostname: address.to_string(),
                ips: vec![ip.to_string()],
                success: true,
                error: None,
            };
        }

        // Attempt DNS resolution
        let addr_with_port = format!("{}:0", address);
        match addr_with_port.to_socket_addrs() {
            Ok(addrs) => {
                let ips: Vec<String> = addrs.map(|a| a.ip().to_string()).collect();

                if ips.is_empty() {
                    ResolveResult {
                        hostname: address.to_string(),
                        ips: Vec::new(),
                        success: false,
                        error: Some("No IP addresses found".to_string()),
                    }
                } else {
                    tracing::info!("Resolved {} to {:?}", address, ips);
                    ResolveResult {
                        hostname: address.to_string(),
                        ips,
                        success: true,
                        error: None,
                    }
                }
            }
            Err(e) => ResolveResult {
                hostname: address.to_string(),
                ips: Vec::new(),
                success: false,
                error: Some(format!("DNS resolution failed: {}", e)),
            },
        }
    }

    /// Resolve a hostname or IP, returning an error if resolution fails
    pub fn resolve_address_or_err(address: &str) -> EngineResult<Vec<String>> {
        let result = Self::resolve_address(address);
        if result.success {
            Ok(result.ips)
        } else {
            Err(EngineError::DnsResolution(result.error.unwrap_or_else(
                || format!("Failed to resolve {}", address),
            )))
        }
    }

    /// Check if a peer is reachable by hitting the /health endpoint
    pub async fn check_peer(&self, address: &str, port: u16) -> EngineResult<bool> {
        let url = format!("http://{}:{}/health", address, port);

        match self.http_client.get(&url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(true)
                } else {
                    Err(EngineError::Network(format!(
                        "Peer returned status {}",
                        response.status()
                    )))
                }
            }
            Err(e) => {
                if e.is_connect() {
                    Err(EngineError::ConnectionRefused(format!(
                        "Cannot connect to {}:{} - {}",
                        address, port, e
                    )))
                } else if e.is_timeout() {
                    Err(EngineError::Network(format!(
                        "Connection timed out to {}:{}",
                        address, port
                    )))
                } else {
                    Err(EngineError::Network(format!("Request failed: {}", e)))
                }
            }
        }
    }

    /// Get peer info
    pub async fn get_peer_info(&self, address: &str, port: u16) -> EngineResult<serde_json::Value> {
        let url = format!("http://{}:{}/info", address, port);

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| EngineError::Network(format!("Failed to get peer info: {}", e)))?;

        response
            .json()
            .await
            .map_err(|e| EngineError::Serialization(format!("Failed to parse peer info: {}", e)))
    }

    /// Initiate a transfer request to a peer
    pub async fn request_transfer(
        &self,
        address: &str,
        port: u16,
        transfer_id: &str,
        files: Vec<TransferFile>,
        sender_name: Option<String>,
    ) -> EngineResult<TransferResponse> {
        let total_size: u64 = files.iter().map(|f| f.size).sum();

        let request = TransferRequest {
            transfer_id: transfer_id.to_string(),
            sender_name,
            files,
            total_size,
        };

        let url = format!("http://{}:{}/transfer", address, port);

        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            let result = self.http_client.post(&url).json(&request).send().await;

            match result {
                Ok(response) => {
                    let transfer_response: TransferResponse =
                        response.json().await.map_err(|e| {
                            EngineError::Serialization(format!("Failed to parse response: {}", e))
                        })?;
                    return Ok(transfer_response);
                }
                Err(e) => {
                    let error = if e.is_connect() {
                        EngineError::ConnectionRefused(format!(
                            "Cannot connect to {}:{} - {}",
                            address, port, e
                        ))
                    } else {
                        EngineError::Network(format!("Transfer request failed: {}", e))
                    };

                    // Only retry transient errors
                    if !Self::is_transient_error(&error) || attempt == self.max_retries {
                        return Err(error);
                    }

                    // Emit retry event
                    self.event_handler.on_event(EngineEvent::TransferRetry {
                        transfer_id: transfer_id.to_string(),
                        attempt: attempt + 1,
                        max_attempts: self.max_retries,
                        error: error.to_string(),
                    });

                    // Exponential backoff
                    let delay = self.retry_delay_ms * 2u64.pow(attempt);
                    sleep(Duration::from_millis(delay)).await;

                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| EngineError::Network("Unknown error".to_string())))
    }

    async fn wait_for_approval(
        &self,
        address: &str,
        port: u16,
        transfer_id: &str,
    ) -> EngineResult<TransferApprovalStatus> {
        let url = format!(
            "http://{}:{}/transfer/status?transfer_id={}",
            address, port, transfer_id
        );
        let timeout = Duration::from_secs(120);
        let poll_interval = Duration::from_millis(500);
        let started = Instant::now();

        loop {
            let response = self.http_client.get(&url).send().await.map_err(|e| {
                EngineError::Network(format!("Failed to check transfer status: {}", e))
            })?;

            if !response.status().is_success() {
                return Err(EngineError::Network(format!(
                    "Status check failed: {}",
                    response.status()
                )));
            }

            let status: TransferApprovalStatus = response.json().await.map_err(|e| {
                EngineError::Serialization(format!("Failed to parse status: {}", e))
            })?;

            match status.status {
                TransferDecision::Pending => {
                    if started.elapsed() > timeout {
                        return Err(EngineError::TransferTimeout);
                    }
                    sleep(poll_interval).await;
                }
                TransferDecision::Accepted => return Ok(status),
                TransferDecision::Rejected => return Err(EngineError::TransferRejected),
                TransferDecision::NotFound => {
                    return Err(EngineError::TransferNotFound(transfer_id.to_string()))
                }
            }
        }
    }

    /// Send a file to a peer (after transfer is accepted)
    async fn send_file(&self, params: SendFileParams<'_>) -> EngineResult<()> {
        let url = format!(
            "http://{}:{}/chunk?transfer_id={}&file_id={}&token={}",
            params.address, params.port, params.transfer_id, params.file_id, params.token
        );

        // Open and read the file
        let file = File::open(params.file_path)
            .await
            .map_err(|e| EngineError::FileIo(format!("Failed to open file: {}", e)))?;

        let metadata = file
            .metadata()
            .await
            .map_err(|e| EngineError::FileIo(format!("Failed to get file metadata: {}", e)))?;

        let file_size = metadata.len();

        // Create progress-tracking stream
        let event_handler = self.event_handler.clone();
        let transfer_id_owned = params.transfer_id.to_string();
        let file_name = params
            .file_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let last_update = Arc::new(AtomicU64::new(0));
        let total_transfer_size = params.total_transfer_size;

        let stream = ReaderStream::new(file).inspect({
            let event_handler = event_handler.clone();
            let transfer_id = transfer_id_owned.clone();
            let file_name = file_name.clone();
            let bytes_sent = params.bytes_sent_so_far.clone();
            let last_update = last_update.clone();
            let start_time = params.transfer_start_time;

            move |chunk_result| {
                if let Ok(chunk) = chunk_result {
                    let new_total = bytes_sent.fetch_add(chunk.len() as u64, Ordering::SeqCst)
                        + chunk.len() as u64;
                    let last = last_update.load(Ordering::SeqCst);

                    // Throttle updates to every 32KB to avoid flooding
                    if new_total - last >= 32768 || new_total == total_transfer_size {
                        last_update.store(new_total, Ordering::SeqCst);

                        // Calculate speed based on elapsed time
                        let elapsed_secs = start_time.elapsed().as_secs_f64();
                        let speed_bps = if elapsed_secs > 0.0 {
                            (new_total as f64 / elapsed_secs) as u64
                        } else {
                            0
                        };

                        event_handler.on_event(EngineEvent::TransferProgress(TransferProgress {
                            transfer_id: transfer_id.clone(),
                            current_file: Some(file_name.clone()),
                            bytes_transferred: new_total,
                            total_bytes: total_transfer_size,
                            speed_bps,
                        }));
                    }
                }
            }
        });

        // Send the file
        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", file_size)
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .map_err(|e| EngineError::Network(format!("Failed to send file: {}", e)))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(EngineError::Network(format!(
                "Server returned error: {}",
                error_text
            )));
        }

        // Send final progress update for this file
        let final_bytes = params.bytes_sent_so_far.load(Ordering::SeqCst);
        let elapsed_secs = params.transfer_start_time.elapsed().as_secs_f64();
        let speed_bps = if elapsed_secs > 0.0 {
            (final_bytes as f64 / elapsed_secs) as u64
        } else {
            0
        };
        self.event_handler
            .on_event(EngineEvent::TransferProgress(TransferProgress {
                transfer_id: transfer_id_owned,
                current_file: Some(file_name),
                bytes_transferred: final_bytes,
                total_bytes: total_transfer_size,
                speed_bps,
            }));

        Ok(())
    }

    /// Send multiple files to a peer
    pub async fn send_files(
        &self,
        address: &str,
        port: u16,
        file_paths: Vec<PathBuf>,
        sender_name: Option<String>,
    ) -> EngineResult<()> {
        let transfer_id = Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now();

        // Build file list with metadata
        let mut files = Vec::new();
        for path in &file_paths {
            let metadata = tokio::fs::metadata(path)
                .await
                .map_err(|e| EngineError::FileIo(format!("Failed to get file info: {}", e)))?;

            let name = path
                .file_name()
                .ok_or_else(|| EngineError::FileIo("Invalid file path".to_string()))?
                .to_string_lossy()
                .to_string();

            let mime_type = mime_guess::from_path(path).first().map(|m| m.to_string());

            files.push(TransferFile {
                id: Uuid::new_v4().to_string(),
                name,
                size: metadata.len(),
                mime_type,
                relative_path: None,
            });
        }

        // Calculate total transfer size
        let total_transfer_size: u64 = files.iter().map(|f| f.size).sum();

        // Helper closure to record history
        let record_history = |history: &Arc<dyn HistoryPersistence>,
                              files: &[TransferFile],
                              status: TransferStatus,
                              bytes: u64,
                              error: Option<String>| {
            let record = TransferRecord {
                id: transfer_id.clone(),
                direction: TransferDirection::Sent,
                status,
                peer_address: address.to_string(),
                files: files.to_vec(),
                total_size: total_transfer_size,
                bytes_transferred: bytes,
                started_at,
                completed_at: Some(chrono::Utc::now()),
                error,
            };
            if let Err(e) = history.add(record) {
                tracing::warn!("Failed to record transfer history: {}", e);
            }
        };

        // Request transfer
        let response = match self
            .request_transfer(address, port, &transfer_id, files.clone(), sender_name)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // Record failed transfer to history
                if let Some(ref history) = self.history {
                    record_history(
                        history,
                        &files,
                        TransferStatus::Failed,
                        0,
                        Some(e.to_string()),
                    );
                }
                self.event_handler.on_event(EngineEvent::TransferFailed {
                    transfer_id: transfer_id.clone(),
                    error: e.to_string(),
                });
                return Err(e);
            }
        };

        let token = if response.accepted {
            match response.token {
                Some(t) => t,
                None => {
                    let err = EngineError::Network("No token received".to_string());
                    if let Some(ref history) = self.history {
                        record_history(
                            history,
                            &files,
                            TransferStatus::Failed,
                            0,
                            Some(err.to_string()),
                        );
                    }
                    self.event_handler.on_event(EngineEvent::TransferFailed {
                        transfer_id: transfer_id.clone(),
                        error: err.to_string(),
                    });
                    return Err(err);
                }
            }
        } else {
            match self.wait_for_approval(address, port, &transfer_id).await {
                Ok(status) => match status.token {
                    Some(t) => t,
                    None => {
                        let err = EngineError::Network("No token received".to_string());
                        if let Some(ref history) = self.history {
                            record_history(
                                history,
                                &files,
                                TransferStatus::Failed,
                                0,
                                Some(err.to_string()),
                            );
                        }
                        self.event_handler.on_event(EngineEvent::TransferFailed {
                            transfer_id: transfer_id.clone(),
                            error: err.to_string(),
                        });
                        return Err(err);
                    }
                },
                Err(e) => {
                    // Record rejected/timed out transfer
                    let status = if matches!(e, EngineError::TransferRejected) {
                        TransferStatus::Rejected
                    } else {
                        TransferStatus::Failed
                    };
                    if let Some(ref history) = self.history {
                        record_history(history, &files, status, 0, Some(e.to_string()));
                    }
                    self.event_handler.on_event(EngineEvent::TransferFailed {
                        transfer_id: transfer_id.clone(),
                        error: e.to_string(),
                    });
                    return Err(e);
                }
            }
        };

        let bytes_sent_so_far = Arc::new(AtomicU64::new(0));
        let transfer_start_time = Instant::now();

        // Send each file
        for (file, path) in files.iter().zip(file_paths.iter()) {
            if let Err(e) = self
                .send_file(SendFileParams {
                    address,
                    port,
                    transfer_id: &transfer_id,
                    token: &token,
                    file_id: &file.id,
                    file_path: path,
                    total_transfer_size,
                    bytes_sent_so_far: bytes_sent_so_far.clone(),
                    transfer_start_time,
                })
                .await
            {
                // Record failed transfer
                let bytes = bytes_sent_so_far.load(Ordering::SeqCst);
                if let Some(ref history) = self.history {
                    record_history(
                        history,
                        &files,
                        TransferStatus::Failed,
                        bytes,
                        Some(e.to_string()),
                    );
                }
                self.event_handler.on_event(EngineEvent::TransferFailed {
                    transfer_id: transfer_id.clone(),
                    error: e.to_string(),
                });
                return Err(e);
            }

            tracing::info!("Sent file: {}", file.name);
        }

        // Record successful transfer
        if let Some(ref history) = self.history {
            record_history(
                history,
                &files,
                TransferStatus::Completed,
                total_transfer_size,
                None,
            );
        }

        // Emit completion event
        self.event_handler
            .on_event(EngineEvent::TransferComplete { transfer_id });

        Ok(())
    }

    /// Send a directory and all its contents to a peer
    ///
    /// Recursively enumerates all files in the directory and sends them
    /// with their relative paths preserved.
    pub async fn send_directory(
        &self,
        address: &str,
        port: u16,
        dir_path: impl AsRef<Path>,
        sender_name: Option<String>,
    ) -> EngineResult<()> {
        let dir_path = dir_path.as_ref();

        let metadata = tokio::fs::metadata(dir_path)
            .await
            .map_err(|e| EngineError::FileIo(format!("Failed to access path: {}", e)))?;

        if !metadata.is_dir() {
            return Err(EngineError::FileIo(format!(
                "Path is not a directory: {}",
                dir_path.display()
            )));
        }

        // Collect all files recursively
        let mut files_to_send: Vec<(PathBuf, String)> = Vec::new();
        Self::collect_directory_files_async(dir_path, dir_path, &mut files_to_send).await?;

        if files_to_send.is_empty() {
            return Err(EngineError::FileIo("Directory is empty".to_string()));
        }

        let transfer_id = Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now();

        // Build file list with metadata and relative paths
        let mut files = Vec::new();
        let mut file_paths = Vec::new();

        for (path, relative_path) in &files_to_send {
            let metadata = tokio::fs::metadata(path)
                .await
                .map_err(|e| EngineError::FileIo(format!("Failed to get file info: {}", e)))?;

            let name = path
                .file_name()
                .ok_or_else(|| EngineError::FileIo("Invalid file path".to_string()))?
                .to_string_lossy()
                .to_string();

            let mime_type = mime_guess::from_path(path).first().map(|m| m.to_string());

            files.push(TransferFile {
                id: Uuid::new_v4().to_string(),
                name,
                size: metadata.len(),
                mime_type,
                relative_path: Some(relative_path.clone()),
            });
            file_paths.push(path.clone());
        }

        // Calculate total transfer size
        let total_transfer_size: u64 = files.iter().map(|f| f.size).sum();

        // Helper closure to record history
        let record_history = |history: &Arc<dyn HistoryPersistence>,
                              files: &[TransferFile],
                              status: TransferStatus,
                              bytes: u64,
                              error: Option<String>| {
            let record = TransferRecord {
                id: transfer_id.clone(),
                direction: TransferDirection::Sent,
                status,
                peer_address: address.to_string(),
                files: files.to_vec(),
                total_size: total_transfer_size,
                bytes_transferred: bytes,
                started_at,
                completed_at: Some(chrono::Utc::now()),
                error,
            };
            if let Err(e) = history.add(record) {
                tracing::warn!("Failed to record transfer history: {}", e);
            }
        };

        // Request transfer
        let response = match self
            .request_transfer(address, port, &transfer_id, files.clone(), sender_name)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if let Some(ref history) = self.history {
                    record_history(
                        history,
                        &files,
                        TransferStatus::Failed,
                        0,
                        Some(e.to_string()),
                    );
                }
                self.event_handler.on_event(EngineEvent::TransferFailed {
                    transfer_id: transfer_id.clone(),
                    error: e.to_string(),
                });
                return Err(e);
            }
        };

        let token = if response.accepted {
            match response.token {
                Some(t) => t,
                None => {
                    let err = EngineError::Network("No token received".to_string());
                    if let Some(ref history) = self.history {
                        record_history(
                            history,
                            &files,
                            TransferStatus::Failed,
                            0,
                            Some(err.to_string()),
                        );
                    }
                    self.event_handler.on_event(EngineEvent::TransferFailed {
                        transfer_id: transfer_id.clone(),
                        error: err.to_string(),
                    });
                    return Err(err);
                }
            }
        } else {
            match self.wait_for_approval(address, port, &transfer_id).await {
                Ok(status) => match status.token {
                    Some(t) => t,
                    None => {
                        let err = EngineError::Network("No token received".to_string());
                        if let Some(ref history) = self.history {
                            record_history(
                                history,
                                &files,
                                TransferStatus::Failed,
                                0,
                                Some(err.to_string()),
                            );
                        }
                        self.event_handler.on_event(EngineEvent::TransferFailed {
                            transfer_id: transfer_id.clone(),
                            error: err.to_string(),
                        });
                        return Err(err);
                    }
                },
                Err(e) => {
                    let status = if matches!(e, EngineError::TransferRejected) {
                        TransferStatus::Rejected
                    } else {
                        TransferStatus::Failed
                    };
                    if let Some(ref history) = self.history {
                        record_history(history, &files, status, 0, Some(e.to_string()));
                    }
                    self.event_handler.on_event(EngineEvent::TransferFailed {
                        transfer_id: transfer_id.clone(),
                        error: e.to_string(),
                    });
                    return Err(e);
                }
            }
        };

        let bytes_sent_so_far = Arc::new(AtomicU64::new(0));
        let transfer_start_time = Instant::now();

        // Send each file
        for (file, path) in files.iter().zip(file_paths.iter()) {
            if let Err(e) = self
                .send_file(SendFileParams {
                    address,
                    port,
                    transfer_id: &transfer_id,
                    token: &token,
                    file_id: &file.id,
                    file_path: path,
                    total_transfer_size,
                    bytes_sent_so_far: bytes_sent_so_far.clone(),
                    transfer_start_time,
                })
                .await
            {
                let bytes = bytes_sent_so_far.load(Ordering::SeqCst);
                if let Some(ref history) = self.history {
                    record_history(
                        history,
                        &files,
                        TransferStatus::Failed,
                        bytes,
                        Some(e.to_string()),
                    );
                }
                self.event_handler.on_event(EngineEvent::TransferFailed {
                    transfer_id: transfer_id.clone(),
                    error: e.to_string(),
                });
                return Err(e);
            }

            tracing::info!(
                "Sent file: {} ({})",
                file.name,
                file.relative_path.as_deref().unwrap_or("")
            );
        }

        // Record successful transfer
        if let Some(ref history) = self.history {
            record_history(
                history,
                &files,
                TransferStatus::Completed,
                total_transfer_size,
                None,
            );
        }

        // Emit completion event
        self.event_handler
            .on_event(EngineEvent::TransferComplete { transfer_id });

        Ok(())
    }

    /// Recursively collect all files in a directory with their relative paths (async version)
    async fn collect_directory_files_async(
        base_path: &Path,
        current_path: &Path,
        files: &mut Vec<(PathBuf, String)>,
    ) -> EngineResult<()> {
        let mut entries = tokio::fs::read_dir(current_path)
            .await
            .map_err(|e| EngineError::FileIo(format!("Failed to read directory: {}", e)))?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            EngineError::FileIo(format!("Failed to read directory entry: {}", e))
        })? {
            let path = entry.path();
            let file_type = entry.file_type().await.map_err(|e| {
                EngineError::FileIo(format!("Failed to get file type: {}", e))
            })?;

            if file_type.is_file() {
                let relative = path
                    .strip_prefix(base_path)
                    .map_err(|_| EngineError::FileIo("Failed to calculate relative path".to_string()))?
                    .to_string_lossy()
                    .to_string();
                files.push((path, relative));
            } else if file_type.is_dir() {
                Box::pin(Self::collect_directory_files_async(base_path, &path, files)).await?;
            }
        }

        Ok(())
    }
}

/// Get all network interfaces with their IP addresses
pub fn get_network_interfaces() -> Vec<NetworkInterface> {
    let mut interfaces = Vec::new();

    if let Ok(addrs) = local_ip_address::list_afinet_netifas() {
        for (name, ip) in addrs {
            let is_loopback = ip.is_loopback();
            interfaces.push(NetworkInterface {
                name,
                ip: ip.to_string(),
                is_loopback,
            });
        }
    }

    interfaces
}
