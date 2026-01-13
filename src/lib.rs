// SPDX-License-Identifier: MIT
//! # gosh-lan-transfer
//!
//! A Rust library for peer-to-peer file transfers over LAN, VPN, or Tailscale networks.
//!
//! This crate provides the core transfer engine without any GUI dependencies,
//! making it suitable for use in CLI tools, desktop applications, or as a library.
//!
//! ## Features
//!
//! - Send and receive files between peers
//! - Automatic peer discovery via hostname resolution
//! - Trust-based auto-acceptance for known hosts
//! - Progress tracking via events
//! - No cloud dependencies - all transfers are direct peer-to-peer
//!
//! ## Example
//!
//! ```ignore
//! use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, callback_handler, EngineEvent};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create configuration
//!     let config = EngineConfig::builder()
//!         .device_name("My Device")
//!         .download_dir("/tmp/downloads")
//!         .build();
//!
//!     // Create event handler
//!     let handler = callback_handler(|event| {
//!         if let EngineEvent::TransferProgress(p) = event {
//!             let percent = (p.bytes_transferred * 100) / p.total_bytes;
//!             println!("Progress: {}%", percent);
//!         }
//!     });
//!
//!     // Create engine
//!     let mut engine = GoshTransferEngine::new(config, handler);
//!
//!     // Start server to receive files
//!     let handle = engine.start_server().await?;
//!
//!     // Send files to a peer
//!     engine.send_files("192.168.1.100", 53317, vec!["/path/to/file.txt".into()]).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod events;
pub mod favorites;
pub mod history;
pub mod protocol;
pub mod server;
pub mod types;

// Protocol types (boundary-crossing messages)
pub use protocol::{
    EngineEvent, PeerInfo, PendingTransfer, TransferApprovalStatus, TransferDecision,
    TransferDirection, TransferFile, TransferProgress, TransferRequest, TransferResponse,
    TransferStatus,
};

// Event handling infrastructure
pub use events::{
    callback_handler, channel_handler, noop_handler, CallbackEventHandler, ChannelEventHandler,
    EventHandler, NoopEventHandler,
};

// Engine components
pub use client::{get_network_interfaces, TransferClient};
pub use config::{EngineConfig, EngineConfigBuilder};
pub use error::{EngineError, EngineResult};
pub use favorites::{FavoritesPersistence, InMemoryFavorites};
pub use history::{HistoryPersistence, InMemoryHistory};
pub use server::{ServerHandle, ServerState};

// Domain types
pub use types::{Favorite, NetworkInterface, ResolveResult, TransferRecord};

use std::path::PathBuf;
use std::sync::Arc;

/// The main engine that coordinates all file transfer operations
///
/// This is the primary interface for using the library. It manages:
/// - The HTTP server for receiving files
/// - The HTTP client for sending files
/// - Configuration and state
pub struct GoshTransferEngine {
    config: EngineConfig,
    client: TransferClient,
    server_state: Arc<ServerState>,
    server_handle: Option<ServerHandle>,
    event_handler: Arc<dyn EventHandler>,
    history: Option<Arc<dyn HistoryPersistence>>,
}

impl GoshTransferEngine {
    /// Create a new engine with the given configuration and event handler
    pub fn new(config: EngineConfig, event_handler: Arc<dyn EventHandler>) -> Self {
        let server_state = Arc::new(ServerState::new(config.clone(), event_handler.clone()));
        let client = TransferClient::new_with_config(event_handler.clone(), &config);

        Self {
            config,
            client,
            server_state,
            server_handle: None,
            event_handler,
            history: None,
        }
    }

    /// Create a new engine with the given configuration, event handler, and history persistence
    ///
    /// The history will automatically record completed and failed transfers.
    pub fn with_history(
        config: EngineConfig,
        event_handler: Arc<dyn EventHandler>,
        history: Arc<dyn HistoryPersistence>,
    ) -> Self {
        let server_state =
            Arc::new(ServerState::new_with_history(config.clone(), event_handler.clone(), history.clone()));
        let client = TransferClient::new_with_history_and_config(event_handler.clone(), history.clone(), &config);

        Self {
            config,
            client,
            server_state,
            server_handle: None,
            event_handler,
            history: Some(history),
        }
    }

    /// Create a new engine with a channel-based event handler
    ///
    /// This is a convenience constructor that returns both the engine
    /// and a receiver for events.
    pub fn with_channel_events(
        config: EngineConfig,
    ) -> (Self, tokio::sync::broadcast::Receiver<EngineEvent>) {
        let (handler, receiver) = channel_handler(100);
        (Self::new(config, handler), receiver)
    }

    /// Create a new engine with a channel-based event handler and history persistence
    ///
    /// This is a convenience constructor that returns both the engine
    /// and a receiver for events.
    pub fn with_channel_events_and_history(
        config: EngineConfig,
        history: Arc<dyn HistoryPersistence>,
    ) -> (Self, tokio::sync::broadcast::Receiver<EngineEvent>) {
        let (handler, receiver) = channel_handler(100);
        (Self::with_history(config, handler, history), receiver)
    }

    /// Get the history persistence (if configured)
    pub fn history(&self) -> Option<&Arc<dyn HistoryPersistence>> {
        self.history.as_ref()
    }

    // === Server Operations ===

    /// Start the HTTP server for receiving files
    ///
    /// The server binds to all interfaces (0.0.0.0) on the configured port.
    /// Returns a handle that can be used to stop the server.
    pub async fn start_server(&mut self) -> EngineResult<()> {
        if self.server_handle.is_some() {
            return Err(EngineError::ServerAlreadyRunning);
        }

        let handle = server::start_server(self.server_state.clone(), self.config.port).await?;
        self.server_handle = Some(handle);

        Ok(())
    }

    /// Stop the HTTP server
    pub async fn stop_server(&mut self) -> EngineResult<()> {
        if let Some(handle) = self.server_handle.take() {
            handle.shutdown();
            self.event_handler.on_event(EngineEvent::ServerStopped);
        }
        Ok(())
    }

    /// Check if the server is running
    pub fn is_server_running(&self) -> bool {
        self.server_handle.is_some()
    }

    /// Get the server state for advanced operations
    pub fn server_state(&self) -> &Arc<ServerState> {
        &self.server_state
    }

    // === Transfer Operations ===

    /// Send files to a peer
    ///
    /// This will:
    /// 1. Request permission from the peer
    /// 2. Wait for approval (or auto-accept if trusted)
    /// 3. Stream the files to the peer
    /// 4. Emit progress events during transfer
    pub async fn send_files(
        &self,
        address: &str,
        port: u16,
        file_paths: Vec<PathBuf>,
    ) -> EngineResult<()> {
        if self.config.receive_only {
            return Err(EngineError::InvalidConfig(
                "Sending is disabled in receive-only mode".to_string(),
            ));
        }

        self.client
            .send_files(
                address,
                port,
                file_paths,
                Some(self.config.device_name.clone()),
            )
            .await
    }

    /// Send a directory and all its contents to a peer
    ///
    /// The directory structure will be preserved on the receiving end.
    /// Files are sent with relative paths from the base directory.
    pub async fn send_directory(
        &self,
        address: &str,
        port: u16,
        dir_path: impl AsRef<std::path::Path>,
    ) -> EngineResult<()> {
        if self.config.receive_only {
            return Err(EngineError::InvalidConfig(
                "Sending is disabled in receive-only mode".to_string(),
            ));
        }

        self.client
            .send_directory(
                address,
                port,
                dir_path,
                Some(self.config.device_name.clone()),
            )
            .await
    }

    /// Accept a pending transfer
    ///
    /// Returns the token that the sender will use to upload files.
    /// Requires the server to be running.
    pub async fn accept_transfer(&self, transfer_id: &str) -> EngineResult<String> {
        if !self.is_server_running() {
            return Err(EngineError::ServerNotRunning);
        }
        self.server_state.accept_transfer(transfer_id).await
    }

    /// Reject a pending transfer
    ///
    /// Requires the server to be running.
    pub async fn reject_transfer(&self, transfer_id: &str) -> EngineResult<()> {
        if !self.is_server_running() {
            return Err(EngineError::ServerNotRunning);
        }
        self.server_state.reject_transfer(transfer_id).await
    }

    /// Get all pending transfers awaiting approval
    pub async fn get_pending_transfers(&self) -> Vec<PendingTransfer> {
        self.server_state.get_pending_transfers().await
    }

    /// Cancel an in-progress transfer
    ///
    /// This will stop the transfer and emit a TransferFailed event.
    /// Subsequent chunk uploads will be rejected.
    pub async fn cancel_transfer(&self, transfer_id: &str) -> EngineResult<()> {
        self.server_state.cancel_transfer(transfer_id).await
    }

    /// Accept all pending transfers
    ///
    /// Returns a list of (transfer_id, result) pairs.
    /// Each result contains either the token or the error.
    pub async fn accept_all_transfers(&self) -> Vec<(String, EngineResult<String>)> {
        if !self.is_server_running() {
            let pending = self.get_pending_transfers().await;
            return pending
                .into_iter()
                .map(|t| (t.id, Err(EngineError::ServerNotRunning)))
                .collect();
        }

        let pending = self.get_pending_transfers().await;
        let mut results = Vec::with_capacity(pending.len());

        for transfer in pending {
            let result = self.server_state.accept_transfer(&transfer.id).await;
            results.push((transfer.id, result));
        }

        results
    }

    /// Reject all pending transfers
    ///
    /// Returns a list of (transfer_id, result) pairs.
    pub async fn reject_all_transfers(&self) -> Vec<(String, EngineResult<()>)> {
        if !self.is_server_running() {
            let pending = self.get_pending_transfers().await;
            return pending
                .into_iter()
                .map(|t| (t.id, Err(EngineError::ServerNotRunning)))
                .collect();
        }

        let pending = self.get_pending_transfers().await;
        let mut results = Vec::with_capacity(pending.len());

        for transfer in pending {
            let result = self.server_state.reject_transfer(&transfer.id).await;
            results.push((transfer.id, result));
        }

        results
    }

    // === Network Utilities ===

    /// Resolve a hostname or IP to all available addresses
    pub fn resolve_address(address: &str) -> ResolveResult {
        TransferClient::resolve_address(address)
    }

    /// Resolve a hostname or IP, returning an error if resolution fails
    pub fn resolve_address_or_err(address: &str) -> EngineResult<Vec<String>> {
        TransferClient::resolve_address_or_err(address)
    }

    /// Get all network interfaces with their IP addresses
    pub fn get_network_interfaces() -> Vec<NetworkInterface> {
        get_network_interfaces()
    }

    /// Check if a peer is reachable
    pub async fn check_peer(&self, address: &str, port: u16) -> EngineResult<bool> {
        self.client.check_peer(address, port).await
    }

    /// Get peer device information
    pub async fn get_peer_info(&self, address: &str, port: u16) -> EngineResult<serde_json::Value> {
        self.client.get_peer_info(address, port).await
    }

    // === Configuration ===

    /// Update the engine configuration
    ///
    /// This updates the engine config, client config, and server state config.
    pub async fn update_config(&mut self, config: EngineConfig) {
        self.client.update_config(&config);
        self.config = config.clone();
        self.server_state.update_config(config).await;
    }

    /// Get the current configuration
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Add a trusted host for auto-accepting transfers
    pub async fn add_trusted_host(&mut self, host: String) {
        if !self.config.trusted_hosts.contains(&host) {
            self.config.trusted_hosts.push(host);
            self.server_state.update_config(self.config.clone()).await;
        }
    }

    /// Remove a trusted host
    pub async fn remove_trusted_host(&mut self, host: &str) {
        self.config.trusted_hosts.retain(|h| h != host);
        self.server_state.update_config(self.config.clone()).await;
    }

    /// Get the list of trusted hosts
    pub fn trusted_hosts(&self) -> &[String] {
        &self.config.trusted_hosts
    }
}

impl Drop for GoshTransferEngine {
    fn drop(&mut self) {
        // Shutdown server if running
        if let Some(handle) = self.server_handle.take() {
            handle.shutdown();
        }
    }
}
