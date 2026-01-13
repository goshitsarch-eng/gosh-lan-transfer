// SPDX-License-Identifier: MIT
// gosh-lan-transfer - HTTP server for receiving file transfers
//
// The server binds to 0.0.0.0 and :: to accept connections from any interface.
// This ensures it works reliably on LAN, Tailscale, and VPNs.

use axum::{
    body::Body,
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::{IntoResponse, Sse},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
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
    TransferRequest { transfer: PendingTransfer },
    Progress { progress: TransferProgress },
    TransferComplete {
        #[serde(rename = "transferId")]
        transfer_id: String,
    },
    TransferFailed {
        #[serde(rename = "transferId")]
        transfer_id: String,
        error: String,
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
            EngineEvent::TransferProgress(progress) => InternalEvent::Progress { progress },
            EngineEvent::TransferComplete { transfer_id } => {
                InternalEvent::TransferComplete { transfer_id }
            }
            EngineEvent::TransferFailed { transfer_id, error } => {
                InternalEvent::TransferFailed { transfer_id, error }
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
        self.rejected_transfers
            .write()
            .await
            .remove(transfer_id);

        Ok(token)
    }

    /// Reject a pending transfer
    pub async fn reject_transfer(&self, transfer_id: &str) -> EngineResult<()> {
        // Check if transfer exists
        let pending = self.pending_transfers.read().await;
        if !pending.contains_key(transfer_id) {
            return Err(EngineError::TransferNotFound(transfer_id.to_string()));
        }
        drop(pending);

        // Mark as rejected
        self.rejected_transfers
            .write()
            .await
            .insert(transfer_id.to_string(), "Rejected by user".to_string());
        self.approved_tokens.write().await.remove(transfer_id);

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
        let bytes_transferred = *self.transfer_bytes.read().await.get(transfer_id).unwrap_or(&0);

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

    /// Get all pending transfers
    pub async fn get_pending_transfers(&self) -> Vec<PendingTransfer> {
        self.pending_transfers
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    /// Update the configuration
    pub async fn update_config(&self, config: EngineConfig) {
        *self.config.write().await = config;
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
        .with_state(state)
}

fn sanitize_file_name(name: &str, fallback: &str) -> String {
    let trimmed = name.trim();
    let file_name = Path::new(trimmed)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.trim())
        .filter(|n| !n.is_empty() && *n != "." && *n != "..");

    file_name
        .map(|n| n.to_string())
        .unwrap_or_else(|| fallback.to_string())
}

fn split_file_name(name: &str) -> (&str, &str) {
    if let Some((stem, ext)) = name.rsplit_once('.') {
        if !stem.is_empty() {
            return (stem, ext);
        }
    }
    (name, "")
}

/// Sanitize a relative path to prevent directory traversal attacks
fn sanitize_relative_path(path: &str) -> PathBuf {
    let mut result = PathBuf::new();

    for component in Path::new(path).components() {
        // Only allow normal path components (no . or ..)
        // Skip root, parent (..), current (.), and prefix components
        if let std::path::Component::Normal(name) = component {
            if let Some(name_str) = name.to_str() {
                let safe_name = sanitize_file_name(name_str, "file");
                result.push(safe_name);
            }
        }
    }

    result
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

    let source_ip = addr.ip().to_string();

    // Create a pending transfer record
    let pending = PendingTransfer {
        id: request.transfer_id.clone(),
        source_ip: source_ip.clone(),
        sender_name: request.sender_name.clone(),
        files: request.files.clone(),
        total_size: computed_total,
        received_at: chrono::Utc::now(),
    };

    // Check if sender is in trusted hosts
    let config = state.config.read().await;
    let is_trusted = config.trusted_hosts.iter().any(|host| host == &source_ip);

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
        });
    }

    // Notify about the incoming request via event handler
    state.emit_event(EngineEvent::TransferRequest(pending));

    // Return pending status - UI will call accept/reject
    Json(TransferResponse {
        accepted: false,
        message: Some("Awaiting user approval".to_string()),
        token: None,
    })
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
            Json(serde_json::json!({"error": format!("Failed to create download directory: {}", e)})),
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

    // Determine the target path based on relative_path
    let (file_path, mut file) = if let Some(ref relative_path) = file_info.relative_path {
        // Sanitize the relative path to prevent directory traversal
        let sanitized_relative = sanitize_relative_path(relative_path);
        let target_path = download_dir.join(&sanitized_relative);

        // Create parent directories if needed
        if let Some(parent) = target_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::error!("Failed to create directory structure: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Failed to create directories: {}", e)})),
                );
            }
        }

        // Open or create with unique name within the subdirectory
        let base_name = target_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&file_info.id);
        let parent_dir = target_path.parent().unwrap_or(&download_dir);

        match open_unique_file(parent_dir, base_name).await {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Failed to create file: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Failed to create file: {}", e)})),
                );
            }
        }
    } else {
        // No relative path, save directly in download_dir
        let safe_name = sanitize_file_name(&file_info.name, &file_info.id);
        match open_unique_file(&download_dir, &safe_name).await {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Failed to create file: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Failed to create file: {}", e)})),
                );
            }
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
                    tracing::error!(
                        "Received more data than expected for {}",
                        file_info.name
                    );
                    drop(file);
                    let _ = tokio::fs::remove_file(&file_path).await;
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(serde_json::json!({"error": "Received more data than expected"})),
                    );
                }

                if let Err(e) = file.write_all(&data).await {
                    tracing::error!("Failed to write chunk: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("Failed to write: {}", e)})),
                    );
                }

                bytes_received = next_size;

                // Update cumulative transfer bytes
                {
                    let mut transfer_bytes = state.transfer_bytes.write().await;
                    let total = transfer_bytes.entry(params.transfer_id.clone()).or_insert(0);
                    *total += data.len() as u64;
                }

                // Throttle progress updates to every 32KB
                if bytes_received - last_progress_bytes >= 32768 || bytes_received == file_info.size {
                    last_progress_bytes = bytes_received;

                    // Calculate speed based on elapsed time
                    let speed_bps = {
                        let start_times = state.transfer_start_times.read().await;
                        let transfer_bytes = state.transfer_bytes.read().await;
                        if let (Some(start_time), Some(&total_bytes)) = (
                            start_times.get(&params.transfer_id),
                            transfer_bytes.get(&params.transfer_id),
                        ) {
                            let elapsed_secs = start_time.elapsed().as_secs_f64();
                            if elapsed_secs > 0.0 {
                                (total_bytes as f64 / elapsed_secs) as u64
                            } else {
                                0
                            }
                        } else {
                            0
                        }
                    };

                    // Send progress update
                    state.emit_event(EngineEvent::TransferProgress(TransferProgress {
                        transfer_id: params.transfer_id.clone(),
                        current_file: Some(stored_name.clone()),
                        bytes_transferred: bytes_received,
                        total_bytes: file_info.size,
                        speed_bps,
                    }));
                }
            }
            Err(e) => {
                tracing::error!("Error reading chunk: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Stream error: {}", e)})),
                );
            }
        }
    }

    // Ensure all data is flushed
    if let Err(e) = file.flush().await {
        tracing::error!("Failed to flush file: {}", e);
    }

    if bytes_received != file_info.size {
        tracing::warn!(
            "Size mismatch for {}: expected {}, received {}",
            file_info.name,
            file_info.size,
            bytes_received
        );
        let _ = tokio::fs::remove_file(&file_path).await;
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

    // Track received file and check if transfer is complete
    let transfer_id = params.transfer_id.clone();
    let file_id = params.file_id.clone();

    // Add file to received set
    {
        let mut received = state.received_files.write().await;
        received
            .entry(transfer_id.clone())
            .or_insert_with(HashSet::new)
            .insert(file_id);
    }

    // Check if all files have been received
    let pending = state.pending_transfers.read().await;
    if let Some(transfer) = pending.get(&transfer_id) {
        let expected_count = transfer.files.len();
        let received = state.received_files.read().await;
        let received_count = received.get(&transfer_id).map(|s| s.len()).unwrap_or(0);

        if received_count >= expected_count {
            tracing::info!(
                "Transfer {} complete: all {} files received",
                transfer_id,
                expected_count
            );

            // Record to history before cleanup (clone transfer info)
            let transfer_clone = transfer.clone();
            let total_bytes = *state.transfer_bytes.read().await.get(&transfer_id).unwrap_or(&transfer.total_size);

            // Emit completion event
            state.emit_event(EngineEvent::TransferComplete {
                transfer_id: transfer_id.clone(),
            });

            // Record to history
            state.record_receive_history(&transfer_clone, TransferStatus::Completed, total_bytes, None);

            // Clean up transfer state (drop the read lock first)
            drop(pending);
            drop(received);

            // Remove from tracking maps
            state.pending_transfers.write().await.remove(&transfer_id);
            state.approved_tokens.write().await.remove(&transfer_id);
            state.received_files.write().await.remove(&transfer_id);
            state.transfer_bytes.write().await.remove(&transfer_id);
            state.transfer_start_times.write().await.remove(&transfer_id);
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
) -> Sse<impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>>
{
    let rx = state.internal_event_tx.subscribe();

    let stream = BroadcastStream::new(rx).map(|result: Result<InternalEvent, _>| {
        let event: InternalEvent = match result {
            Ok(event) => event,
            Err(_) => {
                return Ok::<_, std::convert::Infallible>(
                    axum::response::sse::Event::default().data("heartbeat"),
                )
            }
        };

        let data = serde_json::to_string(&event).unwrap_or_default();
        Ok(axum::response::sse::Event::default().data(data))
    });

    Sse::new(stream)
}

/// Start the HTTP server and return a handle for controlling it
pub async fn start_server(
    state: Arc<ServerState>,
    port: u16,
) -> EngineResult<ServerHandle> {
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
            tokio::net::TcpListener::bind(addr_v4)
                .await
                .map_err(|e| EngineError::Network(format!("Failed to bind to port {}: {}", port, e)))?
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
    state.event_handler.on_event(EngineEvent::ServerStarted { port });

    Ok(ServerHandle { shutdown_tx })
}
