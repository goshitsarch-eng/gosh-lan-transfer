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
//! - Automatic peer discovery via UDP multicast (plus hostname resolution)
//! - Trust-based auto-acceptance for known hosts
//! - Progress tracking via events
//! - Optional send-side bandwidth limiting
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
//!             let percent = if p.total_bytes > 0 {
//!                 (p.bytes_transferred * 100) / p.total_bytes
//!             } else {
//!                 100
//!             };
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
pub mod discovery;
pub mod error;
pub mod events;
pub mod favorites;
pub mod history;
pub mod protocol;
pub mod server;
mod throttle;
pub mod types;

// Protocol types (boundary-crossing messages)
pub use protocol::{
    DiscoveredPeer, DiscoveryAnnouncement, EngineEvent, PeerInfo, PendingTransfer,
    TransferApprovalStatus, TransferDecision, TransferDirection, TransferFile, TransferProgress,
    TransferRequest, TransferResponse, TransferStatus,
};

// Event handling infrastructure
pub use events::{
    callback_handler, channel_handler, noop_handler, CallbackEventHandler, ChannelEventHandler,
    EventHandler, NoopEventHandler,
};

// Engine components
pub use client::{get_network_interfaces, TransferClient};
pub use config::{EngineConfig, EngineConfigBuilder};
pub use discovery::{DiscoveryHandle, DiscoveryState};
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
    discovery_state: Arc<DiscoveryState>,
    discovery_handle: Option<DiscoveryHandle>,
    event_handler: Arc<dyn EventHandler>,
    history: Option<Arc<dyn HistoryPersistence>>,
}

impl GoshTransferEngine {
    /// Create a new engine with the given configuration and event handler
    pub fn new(config: EngineConfig, event_handler: Arc<dyn EventHandler>) -> Self {
        let server_state = Arc::new(ServerState::new(config.clone(), event_handler.clone()));
        let discovery_state = Arc::new(DiscoveryState::new(config.clone(), event_handler.clone()));
        let client = TransferClient::new_with_config(event_handler.clone(), &config);

        Self {
            config,
            client,
            server_state,
            server_handle: None,
            discovery_state,
            discovery_handle: None,
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
        let server_state = Arc::new(ServerState::new_with_history(
            config.clone(),
            event_handler.clone(),
            history.clone(),
        ));
        let discovery_state = Arc::new(DiscoveryState::new(config.clone(), event_handler.clone()));
        let client = TransferClient::new_with_history_and_config(
            event_handler.clone(),
            history.clone(),
            &config,
        );

        Self {
            config,
            client,
            server_state,
            server_handle: None,
            discovery_state,
            discovery_handle: None,
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

    /// Get the current server port
    pub fn port(&self) -> u16 {
        self.config.port
    }

    /// Change the server port at runtime
    ///
    /// This will gracefully stop the current server, bind to the new port,
    /// and emit appropriate events. If binding to the new port fails,
    /// it will attempt to restore the previous port.
    ///
    /// # Arguments
    /// * `new_port` - The new port to bind to
    ///
    /// # Errors
    /// * `EngineError::InvalidConfig` if the port is invalid (e.g., port 0)
    /// * `EngineError::Network` if binding to the new port fails
    pub async fn change_port(&mut self, new_port: u16) -> EngineResult<()> {
        self.change_port_with_options(new_port, true).await
    }

    /// Change the server port with configurable rollback behavior
    ///
    /// # Arguments
    /// * `new_port` - The new port to bind to
    /// * `rollback_on_failure` - If true, attempt to restore the old port if new binding fails
    ///
    /// # Behavior
    /// 1. Validates the new port
    /// 2. If server is running, stops it gracefully
    /// 3. Attempts to bind to the new port
    /// 4. On success: updates config, emits `PortChanged` and `ServerStarted` events
    /// 5. On failure with rollback: attempts to restore old port
    /// 6. On failure without rollback: leaves server stopped
    pub async fn change_port_with_options(
        &mut self,
        new_port: u16,
        rollback_on_failure: bool,
    ) -> EngineResult<()> {
        // Validate the new port
        Self::validate_port(new_port)?;

        let old_port = self.config.port;

        // No-op if port hasn't changed
        if old_port == new_port {
            tracing::debug!("Port unchanged ({}), skipping restart", new_port);
            return Ok(());
        }

        let was_running = self.is_server_running();

        // Stop the current server if running
        if was_running {
            self.stop_server().await?;
        }

        // Update config with new port
        self.config.port = new_port;
        self.server_state.update_config(self.config.clone()).await;

        // Attempt to start on new port
        if was_running {
            match self.start_server().await {
                Ok(()) => {
                    // Success - emit port changed event
                    self.event_handler
                        .on_event(EngineEvent::PortChanged { old_port, new_port });
                    tracing::info!("Server port changed from {} to {}", old_port, new_port);
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Failed to bind to new port {}: {}", new_port, e);

                    if rollback_on_failure {
                        tracing::info!("Attempting to restore previous port {}", old_port);

                        // Restore old port in config
                        self.config.port = old_port;
                        self.server_state.update_config(self.config.clone()).await;

                        // Try to restart on old port
                        if let Err(restore_err) = self.start_server().await {
                            tracing::error!(
                                "Failed to restore old port {}: {}",
                                old_port,
                                restore_err
                            );
                            // Return the original error, not the restore error
                            return Err(EngineError::Network(format!(
                                "Port change failed and rollback failed: original error: {}, rollback error: {}",
                                e, restore_err
                            )));
                        }

                        // Rollback succeeded, return the original error
                        Err(EngineError::Network(format!(
                            "Failed to bind to port {}: {} (restored to port {})",
                            new_port, e, old_port
                        )))
                    } else {
                        // No rollback - config already updated, server stopped
                        Err(e)
                    }
                }
            }
        } else {
            // Server wasn't running, just update config
            self.event_handler
                .on_event(EngineEvent::PortChanged { old_port, new_port });
            Ok(())
        }
    }

    /// Validate a port number for runtime changes
    fn validate_port(port: u16) -> EngineResult<()> {
        if port == 0 {
            return Err(EngineError::InvalidConfig(
                "Port 0 (auto-assign) is not supported; specify an explicit port".to_string(),
            ));
        }

        if port < 1024 {
            tracing::warn!(
                "Port {} is privileged and may require elevated permissions",
                port
            );
        }

        Ok(())
    }

    // === Peer Discovery ===

    /// Start UDP multicast peer discovery
    ///
    /// Announces this device on the discovery multicast group and listens
    /// for announcements from other devices. Discovered peers are reported
    /// via `EngineEvent::PeerDiscovered` / `EngineEvent::PeerLost` and can
    /// be listed with [`discovered_peers`](Self::discovered_peers).
    pub async fn start_discovery(&mut self) -> EngineResult<()> {
        if self.discovery_handle.is_some() {
            return Err(EngineError::DiscoveryAlreadyRunning);
        }

        let handle = discovery::start_discovery(self.discovery_state.clone()).await?;
        self.discovery_handle = Some(handle);
        Ok(())
    }

    /// Stop peer discovery and clear the peer table
    pub async fn stop_discovery(&mut self) -> EngineResult<()> {
        match self.discovery_handle.take() {
            Some(handle) => {
                handle.shutdown();
                self.discovery_state.clear_peers().await;
                Ok(())
            }
            None => Err(EngineError::DiscoveryNotRunning),
        }
    }

    /// Check if peer discovery is running
    pub fn is_discovery_running(&self) -> bool {
        self.discovery_handle.is_some()
    }

    /// Get a snapshot of currently discovered peers
    pub async fn discovered_peers(&self) -> Vec<DiscoveredPeer> {
        self.discovery_state.peers().await
    }

    /// Discover peers for a fixed duration and return what was found
    ///
    /// If discovery is not already running it is started for the duration
    /// of the call and stopped afterwards. If discovery is already running,
    /// this simply waits and returns a snapshot without stopping it.
    pub async fn discover_peers(
        &mut self,
        timeout: std::time::Duration,
    ) -> EngineResult<Vec<DiscoveredPeer>> {
        let started_here = if self.discovery_handle.is_none() {
            self.start_discovery().await?;
            true
        } else {
            false
        };

        tokio::time::sleep(timeout).await;
        let peers = self.discovered_peers().await;

        if started_here {
            self.stop_discovery().await?;
        }

        Ok(peers)
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
    /// This updates the engine config, client config, server state config,
    /// and discovery state config. Changes to discovery settings (multicast
    /// group, port, intervals) take effect the next time discovery is started.
    pub async fn update_config(&mut self, config: EngineConfig) {
        self.client.update_config(&config);
        self.config = config.clone();
        self.server_state.update_config(config.clone()).await;
        self.discovery_state.update_config(config).await;
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
        // Shutdown discovery if running
        if let Some(handle) = self.discovery_handle.take() {
            handle.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to find an available port
    async fn find_available_port() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn test_change_port_not_running() {
        let config = EngineConfig::builder()
            .port(53317)
            .device_name("Test")
            .download_dir("/tmp")
            .build();

        let (mut engine, mut rx) = GoshTransferEngine::with_channel_events(config);

        // Change port when not running
        engine.change_port(53318).await.unwrap();

        assert_eq!(engine.port(), 53318);

        // Should receive PortChanged event
        let event = rx.try_recv().unwrap();
        match event {
            EngineEvent::PortChanged { old_port, new_port } => {
                assert_eq!(old_port, 53317);
                assert_eq!(new_port, 53318);
            }
            _ => panic!("Expected PortChanged event, got {:?}", event),
        }
    }

    #[tokio::test]
    async fn test_change_port_while_running() {
        let port1 = find_available_port().await;
        let port2 = find_available_port().await;

        let config = EngineConfig::builder()
            .port(port1)
            .device_name("Test")
            .download_dir("/tmp")
            .build();

        let (mut engine, _rx) = GoshTransferEngine::with_channel_events(config);

        engine.start_server().await.unwrap();
        assert!(engine.is_server_running());

        engine.change_port(port2).await.unwrap();

        assert_eq!(engine.port(), port2);
        assert!(engine.is_server_running());

        engine.stop_server().await.unwrap();
    }

    #[tokio::test]
    async fn test_change_port_same_port_noop() {
        let config = EngineConfig::builder()
            .port(53317)
            .device_name("Test")
            .download_dir("/tmp")
            .build();

        let (mut engine, mut rx) = GoshTransferEngine::with_channel_events(config);

        // Should be a no-op
        engine.change_port(53317).await.unwrap();
        assert_eq!(engine.port(), 53317);

        // No event should be emitted
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_change_port_invalid_zero() {
        let config = EngineConfig::builder()
            .port(53317)
            .device_name("Test")
            .download_dir("/tmp")
            .build();

        let (mut engine, _rx) = GoshTransferEngine::with_channel_events(config);

        let result = engine.change_port(0).await;
        assert!(matches!(result, Err(EngineError::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn test_change_port_rollback_on_failure() {
        let port1 = find_available_port().await;

        let config = EngineConfig::builder()
            .port(port1)
            .device_name("Test")
            .download_dir("/tmp")
            .build();

        let (mut engine, _rx) = GoshTransferEngine::with_channel_events(config);

        engine.start_server().await.unwrap();

        // Bind to both IPv4 and IPv6 to block the port completely
        let blocked_port = find_available_port().await;
        let _blocker_v4 = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", blocked_port))
            .await
            .unwrap();
        let _blocker_v6 = tokio::net::TcpListener::bind(format!("[::]:{}", blocked_port)).await;

        // Attempt to change to the blocked port
        let result = engine.change_port(blocked_port).await;
        assert!(result.is_err());

        // Port value should be restored (rollback)
        assert_eq!(engine.port(), port1);

        engine.stop_server().await.ok();
    }

    #[tokio::test]
    async fn test_port_getter() {
        let config = EngineConfig::builder()
            .port(12345)
            .device_name("Test")
            .download_dir("/tmp")
            .build();

        let (engine, _rx) = GoshTransferEngine::with_channel_events(config);
        assert_eq!(engine.port(), 12345);
    }
}
