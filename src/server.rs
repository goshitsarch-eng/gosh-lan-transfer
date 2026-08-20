// SPDX-License-Identifier: MIT
// gosh-lan-transfer - HTTP server for receiving file transfers
//
// The server binds to 0.0.0.0 and :: to accept connections from any interface.
// This ensures it works reliably on LAN, Tailscale, and VPNs.

use axum::{
    body::Body,
    extract::{ConnectInfo, Query, State},
    http::{Method, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    io::ErrorKind,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::{broadcast, oneshot, RwLock},
};
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

use crate::config::EngineConfig;
use crate::error::{EngineError, EngineResult};
use crate::events::EventHandler;
use crate::history::HistoryPersistence;
use crate::protocol::{
    EngineEvent, PendingTransfer, TransferApprovalStatus, TransferDecision, TransferProgress,
    TransferRequest, TransferResponse,
};
use crate::types::{TransferDirection, TransferRecord, TransferStatus};

/// Server state shared across handlers
pub struct ServerState {
    /// Engine configuration
    pub config: RwLock<EngineConfig>,
    /// Pending transfers awaiting user approval
    pub pending_transfers: RwLock<HashMap<String, PendingTransfer>>,
    /// Approved transfer tokens (transfer_id -> token)
    pub approved_tokens: RwLock<HashMap<String, String>>,
    /// Rejected transfers (transfer_id -> reason)
    pub rejected_transfers: RwLock<HashMap<String, String>>,
    /// Cancelled transfers (transfer_id)
    pub cancelled_transfers: RwLock<HashSet<String>>,
    /// Received files per transfer (transfer_id -> set of file_ids)
    pub received_files: RwLock<HashMap<String, HashSet<String>>>,
    /// Bytes received per transfer (transfer_id -> total bytes received so far)
    pub transfer_bytes: RwLock<HashMap<String, u64>>,
    /// Transfer start times (transfer_id -> start instant)
    pub transfer_start_times: RwLock<HashMap<String, std::time::Instant>>,
    /// Channel for internal SSE events
    internal_event_tx: broadcast::Sender<InternalEvent>,
    /// Event handler for engine events
    event_handler: Arc<dyn EventHandler>,
    /// Optional history persistence
    history: Option<Arc<dyn HistoryPersistence>>,
}

/// Internal events for SSE streaming (serializable)
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum InternalEvent {
    TransferRequest {
        transfer: PendingTransfer,
    },
    TransferProgress {
        progress: TransferProgress,
    },
    TransferComplete {
        #[serde(rename = "transferId")]
        transfer_id: String,
    },
    TransferFailed {
        #[serde(rename = "transferId")]
        transfer_id: String,
        error: String,
    },
    PortChanged {
        #[serde(rename = "oldPort")]
        old_port: u16,
        #[serde(rename = "newPort")]
        new_port: u16,
    },
}

impl ServerState {
    /// Create a new server state with the given configuration and event handler
    pub fn new(config: EngineConfig, event_handler: Arc<dyn EventHandler>) -> Self {
        let (internal_event_tx, _) = broadcast::channel(100);

        Self {
            config: RwLock::new(config),
            pending_transfers: RwLock::new(HashMap::new()),
            approved_tokens: RwLock::new(HashMap::new()),
            rejected_transfers: RwLock::new(HashMap::new()),
            cancelled_transfers: RwLock::new(HashSet::new()),
            received_files: RwLock::new(HashMap::new()),
            transfer_bytes: RwLock::new(HashMap::new()),
            transfer_start_times: RwLock::new(HashMap::new()),
            internal_event_tx,
            event_handler,
            history: None,
        }
    }

    /// Create a new server state with history persistence
    pub fn new_with_history(
        config: EngineConfig,
        event_handler: Arc<dyn EventHandler>,
        history: Arc<dyn HistoryPersistence>,
    ) -> Self {
        let (internal_event_tx, _) = broadcast::channel(100);

        Self {
            config: RwLock::new(config),
            pending_transfers: RwLock::new(HashMap::new()),
            approved_tokens: RwLock::new(HashMap::new()),
            rejected_transfers: RwLock::new(HashMap::new()),
            cancelled_transfers: RwLock::new(HashSet::new()),
            received_files: RwLock::new(HashMap::new()),
            transfer_bytes: RwLock::new(HashMap::new()),
            transfer_start_times: RwLock::new(HashMap::new()),
            internal_event_tx,
            event_handler,
            history: Some(history),
        }
    }

    /// Emit an event to both the event handler and internal SSE channel
    fn emit_event(&self, event: EngineEvent) {
        // Send to the event handler
        self.event_handler.on_event(event.clone());

        // Also send to internal SSE channel
        let internal = match event {
            EngineEvent::TransferRequest(transfer) => InternalEvent::TransferRequest { transfer },
            EngineEvent::TransferProgress(progress) => InternalEvent::TransferProgress { progress },
            EngineEvent::TransferComplete { transfer_id } => {
                InternalEvent::TransferComplete { transfer_id }
            }
            EngineEvent::TransferFailed { transfer_id, error } => {
                InternalEvent::TransferFailed { transfer_id, error }
            }
            EngineEvent::PortChanged { old_port, new_port } => {
                InternalEvent::PortChanged { old_port, new_port }
            }
            _ => return, // Don't send server start/stop events to SSE
        };
        let _ = self.internal_event_tx.send(internal);
    }

    /// Record a completed or failed receive transfer to history
    fn record_receive_history(
        &self,
        transfer: &PendingTransfer,
        status: TransferStatus,
        bytes_transferred: u64,
        error: Option<String>,
    ) {
        if let Some(ref history) = self.history {
            let record = TransferRecord {
                id: transfer.id.clone(),
                direction: TransferDirection::Received,
                status,
                peer_address: transfer.source_ip.clone(),
                files: transfer.files.clone(),
                total_size: transfer.total_size,
                bytes_transferred,
                started_at: transfer.received_at,
                completed_at: Some(chrono::Utc::now()),
                error,
            };
            if let Err(e) = history.add(record) {
                tracing::warn!("Failed to record transfer history: {}", e);
            }
        }
    }

    /// Accept a pending transfer
    pub async fn accept_transfer(&self, transfer_id: &str) -> EngineResult<String> {
        if self.cancelled_transfers.read().await.contains(transfer_id) {
            return Err(EngineError::TransferCancelled);
        }

        // Idempotent: re-accepting an in-flight transfer must not rotate the
        // upload token (that would 401 the sender mid-transfer).
        {
            let approved = self.approved_tokens.read().await;
            if let Some(token) = approved.get(transfer_id) {
                return Ok(token.clone());
            }
        }

        // Check if transfer exists
        let pending = self.pending_transfers.read().await;
        if !pending.contains_key(transfer_id) {
            return Err(EngineError::TransferNotFound(transfer_id.to_string()));
        }
        drop(pending);

        // Generate token and approve
        let token = Uuid::new_v4().to_string();
        self.approved_tokens
            .write()
            .await
            .insert(transfer_id.to_string(), token.clone());
        self.rejected_transfers.write().await.remove(transfer_id);

        Ok(token)
    }

    /// Reject a pending transfer
    pub async fn reject_transfer(&self, transfer_id: &str) -> EngineResult<()> {
        if self.cancelled_transfers.read().await.contains(transfer_id) {
            return Err(EngineError::TransferCancelled);
        }

        // Rejecting an already-accepted transfer would yank the token out from
        // under an in-progress upload without emitting TransferFailed. Callers
        // must use cancel_transfer() for that.
        if self.approved_tokens.read().await.contains_key(transfer_id) {
            return Err(EngineError::InvalidConfig(
                "Transfer already accepted; use cancel_transfer to stop it".to_string(),
            ));
        }

        // Check if transfer exists
        let pending = self.pending_transfers.read().await;
        if !pending.contains_key(transfer_id) {
            return Err(EngineError::TransferNotFound(transfer_id.to_string()));
        }
        drop(pending);

        // Mark as rejected and drop the pending record so it no longer appears
        // in get_pending_transfers() (status polling still sees rejected_transfers).
        self.rejected_transfers
            .write()
            .await
            .insert(transfer_id.to_string(), "Rejected by user".to_string());
        self.approved_tokens.write().await.remove(transfer_id);
        self.pending_transfers.write().await.remove(transfer_id);

        Ok(())
    }

    /// Cancel an in-progress transfer
    ///
    /// This will cause subsequent chunk uploads to be rejected.
    pub async fn cancel_transfer(&self, transfer_id: &str) -> EngineResult<()> {
        // Check if transfer exists (either pending or approved) and get transfer info for history
        let pending = self.pending_transfers.read().await;
        let approved = self.approved_tokens.read().await;
        if !pending.contains_key(transfer_id) && !approved.contains_key(transfer_id) {
            return Err(EngineError::TransferNotFound(transfer_id.to_string()));
        }
        let transfer_info = pending.get(transfer_id).cloned();
        drop(pending);
        drop(approved);

        // Get bytes transferred so far
        let bytes_transferred = *self
            .transfer_bytes
            .read()
            .await
            .get(transfer_id)
            .unwrap_or(&0);

        // Mark as cancelled
        self.cancelled_transfers
            .write()
            .await
            .insert(transfer_id.to_string());

        // Clean up the transfer state
        self.pending_transfers.write().await.remove(transfer_id);
        self.approved_tokens.write().await.remove(transfer_id);
        self.received_files.write().await.remove(transfer_id);
        self.transfer_bytes.write().await.remove(transfer_id);
        self.transfer_start_times.write().await.remove(transfer_id);

        // Emit cancellation event
        self.emit_event(EngineEvent::TransferFailed {
            transfer_id: transfer_id.to_string(),
            error: "Transfer cancelled".to_string(),
        });

        // Record to history if we have transfer info
        if let Some(transfer) = transfer_info {
            self.record_receive_history(
                &transfer,
                TransferStatus::Failed,
                bytes_transferred,
                Some("Transfer cancelled".to_string()),
            );
        }

        Ok(())
    }

    /// Check if a transfer has been cancelled
    pub async fn is_transfer_cancelled(&self, transfer_id: &str) -> bool {
        self.cancelled_transfers.read().await.contains(transfer_id)
    }

    /// Get transfers that are still awaiting user approval.
    ///
    /// Accepted (in-progress) and rejected transfers are excluded so UIs do
    /// not keep showing them in the pending section.
    pub async fn get_pending_transfers(&self) -> Vec<PendingTransfer> {
        let pending = self.pending_transfers.read().await;
        let approved = self.approved_tokens.read().await;
        let rejected = self.rejected_transfers.read().await;
        pending
            .values()
            .filter(|t| !approved.contains_key(&t.id) && !rejected.contains_key(&t.id))
            .cloned()
            .collect()
    }

    /// Update the configuration
    pub async fn update_config(&self, config: EngineConfig) {
        *self.config.write().await = config;
    }

    /// Roll back a failed file's bytes from the cumulative transfer counter
    /// so a retried file doesn't inflate transfer-wide progress.
    async fn rollback_file_bytes(&self, transfer_id: &str, bytes: u64) {
        if let Some(total) = self.transfer_bytes.write().await.get_mut(transfer_id) {
            *total = total.saturating_sub(bytes);
        }
    }
}

/// Handle for controlling a running server
pub struct ServerHandle {
    shutdown_tx: oneshot::Sender<()>,
}

impl ServerHandle {
    /// Shutdown the server gracefully
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Query parameters for file chunk uploads
#[derive(Debug, Deserialize)]
pub struct ChunkParams {
    transfer_id: String,
    file_id: String,
    token: String,
}

#[derive(Debug, Deserialize)]
pub struct TransferStatusParams {
    transfer_id: String,
}

/// Create the Axum router for the file transfer server
pub fn create_router(state: Arc<ServerState>) -> Router {
    Router::new()
        // Health check - useful for testing connectivity
        .route("/health", get(health_handler))
        // Server info - returns device name and version
        .route("/info", get(info_handler))
        // Transfer request - initiate a new transfer
        .route("/transfer", post(transfer_request_handler))
        // Transfer approval status
        .route("/transfer/status", get(transfer_status_handler))
        // Chunk upload - stream file data
        .route("/chunk", post(chunk_upload_handler))
        // SSE endpoint for transfer progress
        .route("/events", get(events_handler))
        .layer(
            // Permissive CORS so browser UIs can call /info, /events, and
            // the transfer endpoints. Credentials are intentionally omitted:
            // `*` + credentials is invalid and would hide responses.
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers(Any),
        )
        .with_state(state)
}

fn sanitize_file_name(name: &str, fallback: &str) -> String {
    let stripped: String = name.chars().filter(|c| !c.is_control()).collect();
    let trimmed = stripped.trim();
    let file_name = Path::new(trimmed)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.trim())
        .filter(|n| !n.is_empty() && *n != "." && *n != "..");

    file_name.map(|n| n.to_string()).unwrap_or_else(|| {
        let fb: String = fallback.chars().filter(|c| !c.is_control()).collect();
        let fb = fb.trim();
        if fb.is_empty() || fb == "." || fb == ".." {
            "file".to_string()
        } else {
            fb.to_string()
        }
    })
}

fn split_file_name(name: &str) -> (&str, &str) {
    if let Some((stem, ext)) = name.rsplit_once('.') {
        if !stem.is_empty() {
            return (stem, ext);
        }
    }
    (name, "")
}

/// Sanitize a relative path to prevent directory traversal attacks.
///
/// Both `/` and `\` are treated as separators so a Windows sender talking to
/// a Unix receiver still recreates the tree instead of writing a single
/// oddly-named file. `..`, `.`, empty, and drive-letter components are dropped.
fn sanitize_relative_path(path: &str) -> PathBuf {
    let mut result = PathBuf::new();

    for component in path.split(['/', '\\']) {
        if component.is_empty() || component == "." || component == ".." {
            continue;
        }
        // Skip Windows drive prefixes ("C:")
        let bytes = component.as_bytes();
        if bytes.len() == 2 && bytes[1] == b':' {
            continue;
        }
        let safe_name = sanitize_file_name(component, "file");
        if safe_name != "." && safe_name != ".." && !safe_name.is_empty() {
            result.push(safe_name);
        }
    }

    result
}

/// Directory to write into and the unique-file base name, always under `download_dir`.
///
/// A relative path that sanitizes to empty (e.g. `../../../`) used to join to
/// `download_dir` itself; using `.parent()` then wrote *outside* the download
/// folder. That both escaped the sandbox and made files "disappear" from the
/// expected download section.
fn receive_target(
    download_dir: &Path,
    file_info: &crate::protocol::TransferFile,
) -> (PathBuf, String) {
    let fallback_name = sanitize_file_name(&file_info.name, &file_info.id);

    if let Some(ref relative_path) = file_info.relative_path {
        let sanitized = sanitize_relative_path(relative_path);
        if !sanitized.as_os_str().is_empty() {
            let target = download_dir.join(&sanitized);
            if target.starts_with(download_dir) && target != download_dir {
                if let Some(parent) = target.parent() {
                    if parent.starts_with(download_dir) {
                        let base_name = target
                            .file_name()
                            .and_then(|n| n.to_str())
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| fallback_name.clone());
                        return (parent.to_path_buf(), base_name);
                    }
                }
            }
        }
    }

    (download_dir.to_path_buf(), fallback_name)
}

/// Normalize a peer IP for trusted-host matching.
/// Dual-stack listeners report IPv4 clients as `::ffff:a.b.c.d`.
fn normalize_ip(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
        other => other.to_string(),
    }
}

fn normalize_trusted_host(host: &str) -> String {
    let host = host.trim();
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        normalize_ip(ip)
    } else {
        host.to_string()
    }
}

async fn open_unique_file(
    download_dir: &Path,
    base_name: &str,
) -> Result<(PathBuf, File), std::io::Error> {
    let (stem, ext) = split_file_name(base_name);

    for index in 0..1000 {
        let candidate = if index == 0 {
            base_name.to_string()
        } else if ext.is_empty() {
            format!("{} ({})", stem, index)
        } else {
            format!("{} ({}).{}", stem, index, ext)
        };

        let path = download_dir.join(&candidate);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }

    Err(std::io::Error::new(
        ErrorKind::AlreadyExists,
        "Too many filename conflicts",
    ))
}

/// Health check endpoint
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "app": "gosh-lan-transfer",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// Server info endpoint
async fn info_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let config = state.config.read().await;

    Json(serde_json::json!({
        "name": config.device_name,
        "deviceName": config.device_name,
        "version": env!("CARGO_PKG_VERSION"),
        "app": "gosh-lan-transfer"
    }))
}

/// Handle incoming transfer request
async fn transfer_request_handler(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(request): Json<TransferRequest>,
) -> impl IntoResponse {
    if request.files.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(TransferResponse {
                accepted: false,
                message: Some("No files in transfer request".to_string()),
                token: None,
            }),
        )
            .into_response();
    }

    let computed_total: u64 = request.files.iter().map(|f| f.size).sum();

    if computed_total != request.total_size {
        tracing::warn!(
            "Transfer total mismatch for {}: client {}, computed {}",
            request.transfer_id,
            request.total_size,
            computed_total
        );
    }

    tracing::info!(
        "Received transfer request: {} files, {} bytes",
        request.files.len(),
        computed_total
    );

    // Normalize IPv4-mapped IPv6 addresses (the dual-stack listener reports
    // IPv4 clients as ::ffff:a.b.c.d, which would never match trusted hosts)
    let source_ip = normalize_ip(addr.ip());

    // Create a pending transfer record
    let pending = PendingTransfer {
        id: request.transfer_id.clone(),
        source_ip: source_ip.clone(),
        sender_name: request.sender_name.clone(),
        files: request.files.clone(),
        total_size: computed_total,
        received_at: chrono::Utc::now(),
    };

    // Check if sender is in trusted hosts (normalize entries so ::ffff:x.x.x.x
    // in the config also matches a plain IPv4 source).
    let config = state.config.read().await;
    let is_trusted = config
        .trusted_hosts
        .iter()
        .any(|host| normalize_trusted_host(host) == source_ip || host == &source_ip);

    state
        .pending_transfers
        .write()
        .await
        .insert(request.transfer_id.clone(), pending.clone());
    state
        .rejected_transfers
        .write()
        .await
        .remove(&request.transfer_id);

    if is_trusted {
        // Auto-accept from trusted hosts
        let token = Uuid::new_v4().to_string();
        state
            .approved_tokens
            .write()
            .await
            .insert(request.transfer_id.clone(), token.clone());

        state
            .rejected_transfers
            .write()
            .await
            .remove(&request.transfer_id);

        return Json(TransferResponse {
            accepted: true,
            message: Some("Auto-accepted from trusted host".to_string()),
            token: Some(token),
        })
        .into_response();
    }

    // Notify about the incoming request via event handler
    state.emit_event(EngineEvent::TransferRequest(pending));

    // Return pending status - UI will call accept/reject
    Json(TransferResponse {
        accepted: false,
        message: Some("Awaiting user approval".to_string()),
        token: None,
    })
    .into_response()
}

/// Check transfer approval status
async fn transfer_status_handler(
    State(state): State<Arc<ServerState>>,
    Query(params): Query<TransferStatusParams>,
) -> impl IntoResponse {
    let approved = state.approved_tokens.read().await;
    if let Some(token) = approved.get(&params.transfer_id) {
        return Json(TransferApprovalStatus {
            status: TransferDecision::Accepted,
            token: Some(token.clone()),
            message: Some("Accepted".to_string()),
        });
    }
    drop(approved);

    let rejected = state.rejected_transfers.read().await;
    if let Some(reason) = rejected.get(&params.transfer_id) {
        return Json(TransferApprovalStatus {
            status: TransferDecision::Rejected,
            token: None,
            message: Some(reason.clone()),
        });
    }
    drop(rejected);

    let pending = state.pending_transfers.read().await;
    if pending.contains_key(&params.transfer_id) {
        return Json(TransferApprovalStatus {
            status: TransferDecision::Pending,
            token: None,
            message: Some("Awaiting user approval".to_string()),
        });
    }

    Json(TransferApprovalStatus {
        status: TransferDecision::NotFound,
        token: None,
        message: Some("Transfer not found".to_string()),
    })
}

/// Handle file chunk upload
async fn chunk_upload_handler(
    State(state): State<Arc<ServerState>>,
    Query(params): Query<ChunkParams>,
    body: Body,
) -> impl IntoResponse {
    // Check if transfer was cancelled
    if state.is_transfer_cancelled(&params.transfer_id).await {
        return (
            StatusCode::GONE,
            Json(serde_json::json!({"error": "Transfer was cancelled"})),
        );
    }

    // Verify the token
    let approved = state.approved_tokens.read().await;
    let expected_token = approved.get(&params.transfer_id);

    if expected_token != Some(&params.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Invalid or expired token"})),
        );
    }
    drop(approved);

    // Get download directory
    let download_dir = state.config.read().await.download_dir.clone();
    if let Err(e) = tokio::fs::create_dir_all(&download_dir).await {
        tracing::error!("Failed to create download directory: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                serde_json::json!({"error": format!("Failed to create download directory: {}", e)}),
            ),
        );
    }

    // Find the file info from pending transfers
    let pending = state.pending_transfers.read().await;
    let transfer = match pending.get(&params.transfer_id) {
        Some(t) => t.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "Transfer not found"})),
            );
        }
    };
    drop(pending);

    let file_info = match transfer.files.iter().find(|f| f.id == params.file_id) {
        Some(f) => f.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "File not found in transfer"})),
            );
        }
    };

    // Resolve a destination that is always inside download_dir, even when the
    // relative path is empty or made entirely of `..` components.
    let (parent_dir, base_name) = receive_target(&download_dir, &file_info);
    if let Err(e) = tokio::fs::create_dir_all(&parent_dir).await {
        tracing::error!("Failed to create directory structure: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to create directories: {}", e)})),
        );
    }

    let (file_path, mut file) = match open_unique_file(&parent_dir, &base_name).await {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("Failed to create file: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to create file: {}", e)})),
            );
        }
    };

    let stored_name = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&file_info.id)
        .to_string();

    // Initialize or get transfer start time and cumulative bytes
    {
        let mut start_times = state.transfer_start_times.write().await;
        start_times
            .entry(params.transfer_id.clone())
            .or_insert_with(std::time::Instant::now);
    }

    // Stream the body to the file
    let mut bytes_received: u64 = 0;
    let mut stream = body.into_data_stream();
    let mut last_progress_bytes: u64 = 0;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => {
                let next_size = bytes_received + data.len() as u64;
                if next_size > file_info.size {
                    tracing::error!("Received more data than expected for {}", file_info.name);
                    drop(file);
                    let _ = tokio::fs::remove_file(&file_path).await;
                    state
                        .rollback_file_bytes(&params.transfer_id, bytes_received)
                        .await;
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(serde_json::json!({"error": "Received more data than expected"})),
                    );
                }

                if let Err(e) = file.write_all(&data).await {
                    tracing::error!("Failed to write chunk: {}", e);
                    drop(file);
                    let _ = tokio::fs::remove_file(&file_path).await;
                    state
                        .rollback_file_bytes(&params.transfer_id, bytes_received)
                        .await;
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("Failed to write: {}", e)})),
                    );
                }

                bytes_received = next_size;

                // Update cumulative transfer bytes
                let cumulative_bytes = {
                    let mut transfer_bytes = state.transfer_bytes.write().await;
                    let total = transfer_bytes
                        .entry(params.transfer_id.clone())
                        .or_insert(0);
                    *total += data.len() as u64;
                    *total
                };

                // Throttle progress updates to every 32KB
                if bytes_received - last_progress_bytes >= 32768 || bytes_received == file_info.size
                {
                    last_progress_bytes = bytes_received;

                    // Calculate speed based on elapsed time
                    let speed_bps = {
                        let start_times = state.transfer_start_times.read().await;
                        if let Some(start_time) = start_times.get(&params.transfer_id) {
                            let elapsed_secs = start_time.elapsed().as_secs_f64();
                            if elapsed_secs > 0.0 {
                                (cumulative_bytes as f64 / elapsed_secs) as u64
                            } else {
                                0
                            }
                        } else {
                            0
                        }
                    };

                    // Send transfer-wide progress (matches sender-side semantics)
                    state.emit_event(EngineEvent::TransferProgress(TransferProgress {
                        transfer_id: params.transfer_id.clone(),
                        current_file: Some(stored_name.clone()),
                        bytes_transferred: cumulative_bytes,
                        total_bytes: transfer.total_size,
                        speed_bps,
                    }));
                }
            }
            Err(e) => {
                tracing::error!("Error reading chunk: {}", e);
                drop(file);
                let _ = tokio::fs::remove_file(&file_path).await;
                state
                    .rollback_file_bytes(&params.transfer_id, bytes_received)
                    .await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Stream error: {}", e)})),
                );
            }
        }
    }

    // Ensure all data is flushed; a failed flush means the file may be
    // incomplete on disk, so treat it as a failed upload.
    if let Err(e) = file.flush().await {
        tracing::error!("Failed to flush file: {}", e);
        drop(file);
        let _ = tokio::fs::remove_file(&file_path).await;
        state
            .rollback_file_bytes(&params.transfer_id, bytes_received)
            .await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("Failed to flush file: {}", e)})),
        );
    }

    if bytes_received != file_info.size {
        tracing::warn!(
            "Size mismatch for {}: expected {}, received {}",
            file_info.name,
            file_info.size,
            bytes_received
        );
        let _ = tokio::fs::remove_file(&file_path).await;
        state
            .rollback_file_bytes(&params.transfer_id, bytes_received)
            .await;
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Incomplete file received"})),
        );
    }

    tracing::info!(
        "File received: {} ({} bytes)",
        file_path.display(),
        bytes_received
    );

    // Always emit a progress event after each successful file so zero-byte
    // files (which never enter the stream loop) still update the UI.
    {
        let cumulative_bytes = *state
            .transfer_bytes
            .read()
            .await
            .get(&params.transfer_id)
            .unwrap_or(&bytes_received);
        let speed_bps = {
            let start_times = state.transfer_start_times.read().await;
            if let Some(start_time) = start_times.get(&params.transfer_id) {
                let elapsed_secs = start_time.elapsed().as_secs_f64();
                if elapsed_secs > 0.0 {
                    (cumulative_bytes as f64 / elapsed_secs) as u64
                } else {
                    0
                }
            } else {
                0
            }
        };
        state.emit_event(EngineEvent::TransferProgress(TransferProgress {
            transfer_id: params.transfer_id.clone(),
            current_file: Some(stored_name.clone()),
            bytes_transferred: cumulative_bytes,
            total_bytes: transfer.total_size,
            speed_bps,
        }));
    }

    // Track received file and check if transfer is complete
    let transfer_id = params.transfer_id.clone();
    let file_id = params.file_id.clone();
    let expected_count = transfer.files.len();

    {
        let mut received = state.received_files.write().await;
        received
            .entry(transfer_id.clone())
            .or_insert_with(HashSet::new)
            .insert(file_id);
    }

    // Atomically take the pending record so concurrent last-file uploads
    // cannot emit TransferComplete twice.
    let received_count = state
        .received_files
        .read()
        .await
        .get(&transfer_id)
        .map(|s| s.len())
        .unwrap_or(0);

    if received_count >= expected_count {
        let transfer_clone = {
            let mut pending = state.pending_transfers.write().await;
            pending.remove(&transfer_id)
        };

        if let Some(transfer_clone) = transfer_clone {
            tracing::info!(
                "Transfer {} complete: all {} files received",
                transfer_id,
                expected_count
            );

            let total_bytes = *state
                .transfer_bytes
                .read()
                .await
                .get(&transfer_id)
                .unwrap_or(&transfer_clone.total_size);

            state.emit_event(EngineEvent::TransferComplete {
                transfer_id: transfer_id.clone(),
            });

            state.record_receive_history(
                &transfer_clone,
                TransferStatus::Completed,
                total_bytes,
                None,
            );

            state.approved_tokens.write().await.remove(&transfer_id);
            state.received_files.write().await.remove(&transfer_id);
            state.transfer_bytes.write().await.remove(&transfer_id);
            state
                .transfer_start_times
                .write()
                .await
                .remove(&transfer_id);
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "file": stored_name,
            "bytes_received": bytes_received
        })),
    )
}

/// SSE endpoint for real-time transfer events
async fn events_handler(
    State(state): State<Arc<ServerState>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.internal_event_tx.subscribe();

    let stream = BroadcastStream::new(rx).map(|result| {
        match result {
            Ok(event) => {
                let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
                Ok(Event::default().data(data))
            }
            Err(_) => {
                // Lagged/closed: emit an SSE *comment* so JSON `onmessage`
                // handlers are not fed the literal string "heartbeat".
                Ok(Event::default().comment("lagged"))
            }
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Start the HTTP server and return a handle for controlling it
pub async fn start_server(state: Arc<ServerState>, port: u16) -> EngineResult<ServerHandle> {
    let app = create_router(state.clone());

    tracing::info!("Starting server on port {}", port);

    // Try binding to IPv6 wildcard first (dual-stack on most systems)
    // Fall back to IPv4 only if IPv6 binding fails
    let addr_v6 = SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, port));
    let addr_v4 = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = match tokio::net::TcpListener::bind(addr_v6).await {
        Ok(l) => {
            tracing::info!("Bound to IPv6 wildcard [::]:{}  (dual-stack)", port);
            l
        }
        Err(e) => {
            tracing::debug!("IPv6 bind failed ({}), falling back to IPv4", e);
            tokio::net::TcpListener::bind(addr_v4).await.map_err(|e| {
                EngineError::Network(format!("Failed to bind to port {}: {}", port, e))
            })?
        }
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Spawn the server in the background
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await
        .ok();
    });

    // Emit server started event
    state
        .event_handler
        .on_event(EngineEvent::ServerStarted { port });

    Ok(ServerHandle { shutdown_tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TransferFile;

    fn sample_file(name: &str, relative: Option<&str>) -> TransferFile {
        TransferFile {
            id: "file-id".to_string(),
            name: name.to_string(),
            size: 1,
            mime_type: None,
            relative_path: relative.map(|s| s.to_string()),
        }
    }

    #[test]
    fn receive_target_keeps_files_inside_download_dir() {
        let download = PathBuf::from("/tmp/downloads");
        let (parent, base) = receive_target(
            &download,
            &sample_file("secret.txt", Some("../../../secret.txt")),
        );
        assert!(parent.starts_with(&download), "parent={:?}", parent);
        assert_eq!(parent, download);
        assert_eq!(base, "secret.txt");
    }

    #[test]
    fn receive_target_empty_relative_does_not_use_download_dir_parent() {
        let download = PathBuf::from("/tmp/downloads");
        let (parent, base) = receive_target(&download, &sample_file("notes.txt", Some("..")));
        assert_eq!(parent, download);
        assert_eq!(base, "notes.txt");
        assert_ne!(parent, PathBuf::from("/tmp"));
    }

    #[test]
    fn receive_target_preserves_nested_directory() {
        let download = PathBuf::from("/tmp/downloads");
        let (parent, base) =
            receive_target(&download, &sample_file("a.txt", Some("sub/dir/a.txt")));
        assert_eq!(parent, download.join("sub").join("dir"));
        assert_eq!(base, "a.txt");
    }

    #[test]
    fn receive_target_normalizes_backslashes() {
        let download = PathBuf::from("/tmp/downloads");
        let (parent, base) =
            receive_target(&download, &sample_file("a.txt", Some("sub\\dir\\a.txt")));
        assert_eq!(parent, download.join("sub").join("dir"));
        assert_eq!(base, "a.txt");
    }

    #[test]
    fn sanitize_file_name_strips_control_characters() {
        let name = sanitize_file_name("ok\nfile\x07.txt", "fallback");
        assert!(!name.chars().any(|c| c.is_control()));
        assert!(name.contains("file"));
    }

    #[test]
    fn sse_progress_event_uses_transfer_progress_type() {
        let event = InternalEvent::TransferProgress {
            progress: TransferProgress {
                transfer_id: "t".to_string(),
                current_file: Some("a.txt".to_string()),
                bytes_transferred: 10,
                total_bytes: 100,
                speed_bps: 1,
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "transferProgress");
        assert_eq!(json["progress"]["bytesTransferred"], 10);
        assert_eq!(json["progress"]["totalBytes"], 100);
    }

    #[test]
    fn normalize_mapped_ipv6() {
        let mapped: std::net::IpAddr = "::ffff:192.168.1.10".parse().unwrap();
        assert_eq!(normalize_ip(mapped), "192.168.1.10");
        assert_eq!(
            normalize_trusted_host("::ffff:192.168.1.10"),
            "192.168.1.10"
        );
        assert_eq!(normalize_trusted_host("127.0.0.1"), "127.0.0.1");
    }
}
