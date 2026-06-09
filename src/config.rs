// SPDX-License-Identifier: MIT
// gosh-lan-transfer - Engine configuration

use std::net::Ipv4Addr;
use std::path::PathBuf;

/// Engine configuration
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Port for the HTTP server (default: 53317)
    pub port: u16,
    /// Device name shown to peers
    pub device_name: String,
    /// Default download directory
    pub download_dir: PathBuf,
    /// Trusted hosts that auto-accept transfers
    pub trusted_hosts: Vec<String>,
    /// Receive-only mode (disable sending)
    pub receive_only: bool,
    /// Maximum number of retry attempts for transient failures (default: 3)
    pub max_retries: u32,
    /// Base delay between retries in milliseconds (default: 1000)
    /// Actual delay uses exponential backoff: delay * 2^attempt
    pub retry_delay_ms: u64,
    /// Optional bandwidth limit in bytes per second (None = unlimited)
    pub bandwidth_limit_bps: Option<u64>,
    /// Multicast group used for peer discovery (default: 224.0.0.167)
    pub discovery_multicast_addr: Ipv4Addr,
    /// UDP port used for peer discovery (default: 53318)
    pub discovery_port: u16,
    /// Seconds between discovery announcements (default: 5)
    pub discovery_announce_interval_secs: u64,
    /// Seconds without an announcement before a peer is considered lost (default: 15)
    pub discovery_peer_timeout_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            port: 53317,
            device_name: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "Gosh Device".to_string()),
            download_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            trusted_hosts: Vec::new(),
            receive_only: false,
            max_retries: 3,
            retry_delay_ms: 1000,
            bandwidth_limit_bps: None,
            discovery_multicast_addr: Ipv4Addr::new(224, 0, 0, 167),
            discovery_port: 53318,
            discovery_announce_interval_secs: 5,
            discovery_peer_timeout_secs: 15,
        }
    }
}

impl EngineConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a builder for more ergonomic configuration
    pub fn builder() -> EngineConfigBuilder {
        EngineConfigBuilder::default()
    }
}

/// Builder for EngineConfig
#[derive(Default)]
pub struct EngineConfigBuilder {
    port: Option<u16>,
    device_name: Option<String>,
    download_dir: Option<PathBuf>,
    trusted_hosts: Option<Vec<String>>,
    receive_only: Option<bool>,
    max_retries: Option<u32>,
    retry_delay_ms: Option<u64>,
    bandwidth_limit_bps: Option<Option<u64>>,
    discovery_multicast_addr: Option<Ipv4Addr>,
    discovery_port: Option<u16>,
    discovery_announce_interval_secs: Option<u64>,
    discovery_peer_timeout_secs: Option<u64>,
}

impl EngineConfigBuilder {
    /// Set the port for the HTTP server
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the device name shown to peers
    pub fn device_name(mut self, name: impl Into<String>) -> Self {
        self.device_name = Some(name.into());
        self
    }

    /// Set the download directory for received files
    pub fn download_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.download_dir = Some(dir.into());
        self
    }

    /// Set the list of trusted hosts (auto-accept transfers)
    pub fn trusted_hosts(mut self, hosts: Vec<String>) -> Self {
        self.trusted_hosts = Some(hosts);
        self
    }

    /// Add a single trusted host
    pub fn add_trusted_host(mut self, host: impl Into<String>) -> Self {
        self.trusted_hosts
            .get_or_insert_with(Vec::new)
            .push(host.into());
        self
    }

    /// Set receive-only mode
    pub fn receive_only(mut self, enabled: bool) -> Self {
        self.receive_only = Some(enabled);
        self
    }

    /// Set maximum retry attempts for transient failures
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Set base delay between retries in milliseconds
    pub fn retry_delay_ms(mut self, delay_ms: u64) -> Self {
        self.retry_delay_ms = Some(delay_ms);
        self
    }

    /// Set bandwidth limit in bytes per second (None = unlimited)
    pub fn bandwidth_limit_bps(mut self, limit: Option<u64>) -> Self {
        self.bandwidth_limit_bps = Some(limit);
        self
    }

    /// Set the multicast group used for peer discovery
    pub fn discovery_multicast_addr(mut self, addr: Ipv4Addr) -> Self {
        self.discovery_multicast_addr = Some(addr);
        self
    }

    /// Set the UDP port used for peer discovery
    pub fn discovery_port(mut self, port: u16) -> Self {
        self.discovery_port = Some(port);
        self
    }

    /// Set the seconds between discovery announcements
    pub fn discovery_announce_interval_secs(mut self, secs: u64) -> Self {
        self.discovery_announce_interval_secs = Some(secs);
        self
    }

    /// Set the seconds without an announcement before a peer is considered lost
    pub fn discovery_peer_timeout_secs(mut self, secs: u64) -> Self {
        self.discovery_peer_timeout_secs = Some(secs);
        self
    }

    /// Build the configuration
    pub fn build(self) -> EngineConfig {
        let default = EngineConfig::default();
        EngineConfig {
            port: self.port.unwrap_or(default.port),
            device_name: self.device_name.unwrap_or(default.device_name),
            download_dir: self.download_dir.unwrap_or(default.download_dir),
            trusted_hosts: self.trusted_hosts.unwrap_or(default.trusted_hosts),
            receive_only: self.receive_only.unwrap_or(default.receive_only),
            max_retries: self.max_retries.unwrap_or(default.max_retries),
            retry_delay_ms: self.retry_delay_ms.unwrap_or(default.retry_delay_ms),
            bandwidth_limit_bps: self
                .bandwidth_limit_bps
                .unwrap_or(default.bandwidth_limit_bps),
            discovery_multicast_addr: self
                .discovery_multicast_addr
                .unwrap_or(default.discovery_multicast_addr),
            discovery_port: self.discovery_port.unwrap_or(default.discovery_port),
            discovery_announce_interval_secs: self
                .discovery_announce_interval_secs
                .unwrap_or(default.discovery_announce_interval_secs),
            discovery_peer_timeout_secs: self
                .discovery_peer_timeout_secs
                .unwrap_or(default.discovery_peer_timeout_secs),
        }
    }
}
