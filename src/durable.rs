//! Durable v2 receive sessions. Only verified files become visible at final destinations.
use crate::resumable::{hash_file, save_json, FileCheckpoint, ResumeStatus, TransferManifest};
use crate::security::Principal;
use crate::{
    EngineError, EngineEvent, EngineResult, PendingTransfer, ServerState, TransferDecision,
    TransferProgress, TransferStatus,
};
use axum::{
    body::Body,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    manifest: TransferManifest,
    pending: PendingTransfer,
    owner: String,
    token: String,
    decision: TransferDecision,
    cancelled: bool,
    complete: bool,
    published: HashMap<String, PathBuf>,
    targets: HashMap<String, PathBuf>,
}
struct Session {
    id: String,
    owner: String,
    finished: std::sync::atomic::AtomicBool,
    record: Mutex<Record>,
    cancel: CancellationToken,
    root: PathBuf,
}
#[derive(Default)]
pub(crate) struct DurableStore {
    sessions: RwLock<HashMap<String, Arc<Session>>>,
    root: Mutex<Option<(PathBuf, std::fs::File)>>,
    registration: Mutex<()>,
}
impl DurableStore {
    pub async fn contains(&self, id: &str) -> bool {
        self.sessions.read().await.contains_key(id)
    }
    async fn session(&self, id: &str) -> EngineResult<Arc<Session>> {
        self.sessions
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| EngineError::TransferNotFound(id.into()))
    }
    async fn ensure_root(&self, state: &ServerState) -> EngineResult<PathBuf> {
        let mut root = self.root.lock().await;
        if let Some((path, _)) = &*root {
            return Ok(path.clone());
        }
        let download = state.config.read().await.download_dir.clone();
        tokio::fs::create_dir_all(&download).await?;
        let download = tokio::fs::canonicalize(download).await?;
        let path = download.join(".gosh-transfer");
        match tokio::fs::create_dir(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        if tokio::fs::symlink_metadata(&path)
            .await?
            .file_type()
            .is_symlink()
        {
            return Err(EngineError::FileIo(
                "State directory must not be a symlink".into(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).await?;
        }
        let lock_path = path.join("receiver.lock");
        if tokio::fs::symlink_metadata(&lock_path)
            .await
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            return Err(EngineError::FileIo("Unsafe state lock".into()));
        }
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock().map_err(|_| {
            EngineError::InvalidConfig("Another receiver owns this download directory".into())
        })?;
        *root = Some((path.clone(), lock));
        Ok(path)
    }
    pub async fn load(&self, state: &ServerState) -> EngineResult<()> {
        let download = state.config.read().await.download_dir.clone();
        if !download.join(".gosh-transfer").exists() {
            return Ok(());
        }
        let root = self.ensure_root(state).await?;
        if !self.sessions.read().await.is_empty() {
            return Ok(());
        }
        let mut entries = tokio::fs::read_dir(&root).await?;
        let mut records = HashMap::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            if !entry.file_type().await?.is_file() {
                return Err(EngineError::FileIo("Unsafe session journal".into()));
            }
            let record: Record = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
            record.manifest.validate()?;
            if path.file_stem().and_then(|s| s.to_str())
                != Some(record.manifest.request.transfer_id.as_str())
                || record.pending.id != record.manifest.request.transfer_id
            {
                return Err(EngineError::FileIo(
                    "Invalid session journal identity".into(),
                ));
            }
            for target in record.targets.values().chain(record.published.values()) {
                if target
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
                {
                    return Err(EngineError::FileIo("Unsafe journal destination".into()));
                }
            }
            let cancel = CancellationToken::new();
            if record.cancelled {
                cancel.cancel();
            }
            records.insert(
                record.pending.id.clone(),
                Arc::new(Session {
                    id: record.pending.id.clone(),
                    owner: record.owner.clone(),
                    finished: std::sync::atomic::AtomicBool::new(record.complete),
                    record: Mutex::new(record),
                    cancel,
                    root: root.clone(),
                }),
            );
        }
        *self.sessions.write().await = records;
        Ok(())
    }
    pub async fn forget(&self, id: &str) -> EngineResult<()> {
        let _registration = self.registration.lock().await;
        let session = self.session(id).await?;
        let r = session.record.lock().await;
        if !r.complete && !r.cancelled && r.decision != TransferDecision::Rejected {
            return Err(EngineError::InvalidConfig(
                "Cancel or finish a session before forgetting it".into(),
            ));
        }
        for f in &r.manifest.request.files {
            let _ = tokio::fs::remove_file(part_path(&session, &f.id)).await;
        }
        tokio::fs::remove_file(session.root.join(format!("{id}.json"))).await?;
        self.sessions.write().await.remove(id);
        Ok(())
    }
    pub async fn close(&self) {
        self.sessions.write().await.clear();
        self.root.lock().await.take();
    }
    pub async fn pending(&self) -> Vec<PendingTransfer> {
        let sessions: Vec<_> = self.sessions.read().await.values().cloned().collect();
        let mut pending = Vec::new();
        for session in sessions {
            let r = session.record.lock().await;
            if r.decision == TransferDecision::Pending && !r.cancelled {
                pending.push(r.pending.clone());
            }
        }
        pending
    }
    pub async fn accept_transfer(&self, _state: &ServerState, id: &str) -> EngineResult<String> {
        let session = self.session(id).await?;
        let mut r = session.record.lock().await;
        if r.cancelled {
            return Err(EngineError::TransferCancelled);
        }
        if r.complete {
            return Err(EngineError::TransferNotFound(id.into()));
        }
        if r.decision == TransferDecision::Rejected {
            return Err(EngineError::TransferRejected);
        }
        r.decision = TransferDecision::Accepted;
        persist(&session, &r).await?;
        Ok(r.token.clone())
    }
    pub async fn reject_transfer(&self, state: &ServerState, id: &str) -> EngineResult<()> {
        let session = self.session(id).await?;
        let mut r = session.record.lock().await;
        if r.decision != TransferDecision::Pending {
            return Err(EngineError::InvalidConfig(
                "Use cancellation for an accepted transfer".into(),
            ));
        }
        r.decision = TransferDecision::Rejected;
        persist(&session, &r).await?;
        state.record_receive_history(
            &r.pending,
            TransferStatus::Rejected,
            0,
            Some("Rejected by user".into()),
        );
        Ok(())
    }
    pub async fn cancel_transfer(&self, state: &ServerState, id: &str) -> EngineResult<()> {
        let session = self.session(id).await?;
        if session.finished.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(EngineError::TransferNotFound(id.into()));
        }
        session.cancel.cancel();
        let mut r = session.record.lock().await;
        if r.complete {
            return Err(EngineError::TransferNotFound(id.into()));
        }
        if r.cancelled {
            return Ok(());
        }
        let bytes = checkpoints(&session, &r)
            .await?
            .iter()
            .map(|c| c.offset)
            .sum();
        r.cancelled = true;
        persist(&session, &r).await?;
        for file in &r.manifest.request.files {
            let _ = tokio::fs::remove_file(part_path(&session, &file.id)).await;
        }
        state.emit_event(EngineEvent::TransferFailed {
            transfer_id: id.into(),
            error: "Transfer cancelled".into(),
        });
        state.record_receive_history(
            &r.pending,
            TransferStatus::Failed,
            bytes,
            Some("Transfer cancelled".into()),
        );
        Ok(())
    }
}
fn owner(addr: SocketAddr, principal: Option<Extension<Principal>>) -> String {
    principal
        .map(|p| p.0 .0)
        .unwrap_or_else(|| format!("ip:{}", crate::server::normalize_ip(addr.ip())))
}
async fn persist(session: &Session, record: &Record) -> EngineResult<()> {
    save_json(
        &session.root.join(format!("{}.json", record.pending.id)),
        record,
    )
    .await
}
fn part_path(session: &Session, id: &str) -> PathBuf {
    session.root.join(format!("{}-{id}.part", session.id))
}
async fn checkpoints(session: &Session, r: &Record) -> EngineResult<Vec<FileCheckpoint>> {
    let mut files = Vec::new();
    for f in &r.manifest.request.files {
        let complete = r.published.contains_key(&f.id);
        let offset = if complete {
            f.size
        } else {
            match tokio::fs::symlink_metadata(part_path(session, &f.id)).await {
                Ok(m) if m.is_file() && !m.file_type().is_symlink() => m.len(),
                Ok(_) => return Err(EngineError::FileIo("Unsafe partial file".into())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e.into()),
            }
        };
        if offset > f.size {
            return Err(EngineError::FileIo(
                "Partial file exceeds declared size".into(),
            ));
        }
        files.push(FileCheckpoint {
            file_id: f.id.clone(),
            offset,
            complete,
        });
    }
    Ok(files)
}
async fn snapshot(session: &Session, r: &Record) -> EngineResult<ResumeStatus> {
    Ok(ResumeStatus {
        status: r.decision.clone(),
        token: if r.decision == TransferDecision::Accepted && !r.cancelled {
            Some(r.token.clone())
        } else {
            None
        },
        cancelled: r.cancelled,
        complete: r.complete,
        files: checkpoints(session, r).await?,
    })
}
fn error_response(error: EngineError) -> Response {
    let status = match error {
        EngineError::TransferNotFound(_) => StatusCode::NOT_FOUND,
        EngineError::InvalidConfig(_) => StatusCode::BAD_REQUEST,
        EngineError::TransferCancelled => StatusCode::GONE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string()).into_response()
}
#[derive(Deserialize)]
pub(crate) struct SessionQuery {
    transfer_id: String,
}
#[derive(Deserialize)]
pub(crate) struct UploadQuery {
    transfer_id: String,
    file_id: String,
    offset: u64,
}
pub(crate) async fn register(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    principal: Option<Extension<Principal>>,
    Json(manifest): Json<TransferManifest>,
) -> Response {
    if let Err(e) = manifest.validate() {
        return error_response(e);
    }
    let _registration = state.durable.registration.lock().await;
    let identity = owner(addr, principal);
    let id = &manifest.request.transfer_id;
    if state.pending_transfers.read().await.contains_key(id) {
        return (StatusCode::CONFLICT, "ID belongs to a legacy session").into_response();
    }
    if let Ok(session) = state.durable.session(id).await {
        let r = session.record.lock().await;
        if r.owner != identity
            || serde_json::to_value(&r.manifest).ok() != serde_json::to_value(&manifest).ok()
        {
            return (
                StatusCode::CONFLICT,
                "Manifest conflicts with an existing session",
            )
                .into_response();
        }
        return match snapshot(&session, &r).await {
            Ok(s) => Json(s).into_response(),
            Err(e) => error_response(e),
        };
    }
    if state.durable.sessions.read().await.len() >= 1024 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Durable session limit reached; remove completed sessions through forget_received_transfer",
        )
            .into_response();
    }
    let root = match state.durable.ensure_root(&state).await {
        Ok(r) => r,
        Err(e) => return error_response(e),
    };
    let config = state.config.read().await;
    let source = crate::server::normalize_ip(addr.ip());
    let trusted = config.trusted_hosts.iter().any(|h| {
        h.parse::<std::net::IpAddr>()
            .map(crate::server::normalize_ip)
            .unwrap_or_else(|_| h.trim().into())
            == source
    });
    drop(config);
    let pending = PendingTransfer {
        id: id.clone(),
        source_ip: source,
        sender_name: manifest.request.sender_name.clone(),
        files: manifest.request.files.clone(),
        total_size: manifest.request.total_size,
        received_at: chrono::Utc::now(),
    };
    let record = Record {
        manifest,
        pending: pending.clone(),
        owner: identity,
        token: uuid::Uuid::new_v4().to_string(),
        decision: if trusted {
            TransferDecision::Accepted
        } else {
            TransferDecision::Pending
        },
        cancelled: false,
        complete: false,
        published: HashMap::new(),
        targets: HashMap::new(),
    };
    let session = Arc::new(Session {
        id: record.pending.id.clone(),
        owner: record.owner.clone(),
        finished: std::sync::atomic::AtomicBool::new(false),
        record: Mutex::new(record.clone()),
        cancel: CancellationToken::new(),
        root,
    });
    if let Err(e) = persist(&session, &record).await {
        return error_response(e);
    }
    state
        .durable
        .sessions
        .write()
        .await
        .insert(pending.id.clone(), session.clone());
    if !trusted {
        state.emit_event(EngineEvent::TransferRequest(pending));
    }
    match snapshot(&session, &record).await {
        Ok(s) => Json(s).into_response(),
        Err(e) => error_response(e),
    }
}
pub(crate) async fn status(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    principal: Option<Extension<Principal>>,
    Query(q): Query<SessionQuery>,
) -> Response {
    let session = match state.durable.session(&q.transfer_id).await {
        Ok(s) => s,
        Err(e) => return error_response(e),
    };
    let r = session.record.lock().await;
    if r.owner != owner(addr, principal) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match snapshot(&session, &r).await {
        Ok(s) => Json(s).into_response(),
        Err(e) => error_response(e),
    }
}
pub(crate) async fn cancel(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    principal: Option<Extension<Principal>>,
    Query(q): Query<SessionQuery>,
) -> Response {
    let session = match state.durable.session(&q.transfer_id).await {
        Ok(s) => s,
        Err(e) => return error_response(e),
    };
    // Owner check must not wait behind a stalled upload.
    let identity = owner(addr, principal);
    // Identity is immutable; copied separately in Session below to allow prompt cancellation.
    if session_owner(&session).await != identity {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.durable.cancel_transfer(&state, &q.transfer_id).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => error_response(e),
    }
}
async fn session_owner(session: &Session) -> String {
    session.owner.clone()
}

pub(crate) async fn upload(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    principal: Option<Extension<Principal>>,
    Query(q): Query<UploadQuery>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match state.durable.session(&q.transfer_id).await {
        Ok(s) => s,
        Err(e) => return error_response(e),
    };
    let mut r = session.record.lock().await;
    if r.owner != owner(addr, principal) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if r.cancelled || session.cancel.is_cancelled() {
        return StatusCode::GONE.into_response();
    }
    let token = headers
        .get("X-Transfer-Token")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if r.decision != TransferDecision::Accepted
        || !bool::from(token.as_bytes().ct_eq(r.token.as_bytes()))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let file = match r.manifest.request.files.iter().find(|f| f.id == q.file_id) {
        Some(f) => f.clone(),
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    if r.published.contains_key(&file.id) {
        return StatusCode::OK.into_response();
    }
    let points = match checkpoints(&session, &r).await {
        Ok(p) => p,
        Err(e) => return error_response(e),
    };
    let current = points.iter().find(|p| p.file_id == file.id).unwrap().offset;
    if q.offset != current {
        return (StatusCode::CONFLICT, "Offset changed; query status").into_response();
    }
    let path = part_path(&session, &file.id);
    let mut destination = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(f) => f,
        Err(e) => return error_response(e.into()),
    };
    let baseline = points.iter().map(|p| p.offset).sum::<u64>();
    let mut offset = current;
    let mut last = offset;
    let started = std::time::Instant::now();
    let mut stream = body.into_data_stream();
    loop {
        let next = tokio::select! {biased;
            _=session.cancel.cancelled()=>{drop(destination);let _=tokio::fs::remove_file(&path).await;return StatusCode::GONE.into_response();},
            next=tokio::time::timeout(Duration::from_secs(120),stream.next())=>next,
        };
        let chunk = match next {
            Ok(Some(Ok(c))) => c,
            Ok(None) => break,
            other => {
                // Flushed partial bytes remain resumable on transport failure or timeout.
                if let Err(e) = destination.sync_data().await {
                    return error_response(e.into());
                }
                return (
                    if other.is_err() {
                        StatusCode::REQUEST_TIMEOUT
                    } else {
                        StatusCode::BAD_REQUEST
                    },
                    "Upload interrupted; partial data retained",
                )
                    .into_response();
            }
        };
        if chunk.len() as u64 > file.size - offset {
            drop(destination);
            let _ = tokio::fs::remove_file(&path).await;
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
        if let Err(e) = destination.write_all(&chunk).await {
            return error_response(e.into());
        }
        offset += chunk.len() as u64;
        if offset - last >= 32768 || offset == file.size {
            last = offset;
            let n = baseline + offset - current;
            state.emit_event(EngineEvent::TransferProgress(TransferProgress {
                transfer_id: q.transfer_id.clone(),
                current_file: Some(file.name.clone()),
                bytes_transferred: n,
                total_bytes: r.pending.total_size,
                speed_bps: ((offset - current) as f64 / started.elapsed().as_secs_f64().max(0.001))
                    as u64,
            }));
        }
    }
    if let Err(e) = destination.sync_all().await {
        return error_response(e.into());
    }
    drop(destination);
    if offset != file.size {
        return (
            StatusCode::BAD_REQUEST,
            "Partial upload retained; resume at the current offset",
        )
            .into_response();
    }
    let expected = &r.manifest.sha256[&file.id];
    match hash_file(&path).await {
        Ok((hash, size)) if hash == *expected && size == file.size => {}
        Ok(_) => {
            let _ = tokio::fs::remove_file(&path).await;
            state.emit_event(EngineEvent::TransferFailed {
                transfer_id: q.transfer_id.clone(),
                error: "SHA-256 checksum mismatch; file discarded".into(),
            });
            return StatusCode::UNPROCESSABLE_ENTITY.into_response();
        }
        Err(e) => return error_response(e),
    }
    if session.cancel.is_cancelled() {
        let _ = tokio::fs::remove_file(&path).await;
        return StatusCode::GONE.into_response();
    }
    let download = session.root.parent().unwrap();
    let (parent, base) = crate::server::receive_target(download, &file);
    if crate::server::reserved_target(download, &parent.join(&base)) {
        return (StatusCode::BAD_REQUEST, "Reserved state directory").into_response();
    }
    if let Err(e) = crate::server::create_receive_parent(download, &parent).await {
        return error_response(e.into());
    }
    let mut published = false;
    for index in 0..1000 {
        let target = if let Some(target) = r.targets.get(&file.id) {
            download.join(target)
        } else {
            parent.join(if index == 0 {
                base.clone()
            } else {
                let p = Path::new(&base);
                let stem = p.file_stem().unwrap_or_default().to_string_lossy();
                match p.extension() {
                    Some(ext) => format!("{stem} ({index}).{}", ext.to_string_lossy()),
                    None => format!("{base} ({index})"),
                }
            })
        };
        r.targets.insert(
            file.id.clone(),
            target.strip_prefix(download).unwrap().to_path_buf(),
        );
        if let Err(e) = persist(&session, &r).await {
            return error_response(e);
        }
        match tokio::fs::hard_link(&path, &target).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Recover a crash between publication and the journal commit, without overwriting.
                let safe = tokio::fs::symlink_metadata(&target)
                    .await
                    .is_ok_and(|m| m.is_file() && !m.file_type().is_symlink());
                if !safe
                    || !hash_file(&target)
                        .await
                        .is_ok_and(|(h, n)| h == r.manifest.sha256[&file.id] && n == file.size)
                {
                    r.targets.remove(&file.id);
                    continue;
                }
            }
            Err(e) => return error_response(e.into()),
        }
        // Order destination durability before the completion receipt on Unix.
        #[cfg(unix)]
        {
            let parent = parent.clone();
            let synced =
                tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all()).await;
            match synced {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return error_response(e.into()),
                Err(e) => return error_response(crate::EngineError::FileIo(e.to_string())),
            }
        }
        r.published.insert(
            file.id.clone(),
            target.strip_prefix(download).unwrap().to_path_buf(),
        );
        published = true;
        break;
    }
    if !published {
        return (StatusCode::CONFLICT, "Too many filename conflicts").into_response();
    }
    r.complete = r.published.len() == r.manifest.request.files.len();
    if let Err(e) = persist(&session, &r).await {
        return error_response(e);
    }
    session
        .finished
        .store(r.complete, std::sync::atomic::Ordering::SeqCst);
    let _ = tokio::fs::remove_file(&path).await;
    state.emit_event(EngineEvent::TransferProgress(TransferProgress {
        transfer_id: q.transfer_id.clone(),
        current_file: Some(file.name),
        bytes_transferred: baseline + offset - current,
        total_bytes: r.pending.total_size,
        speed_bps: 0,
    }));
    if r.complete {
        state.record_receive_history(
            &r.pending,
            TransferStatus::Completed,
            r.pending.total_size,
            None,
        );
        state.emit_event(EngineEvent::TransferComplete {
            transfer_id: q.transfer_id,
        });
    }
    StatusCode::OK.into_response()
}
