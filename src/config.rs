// SPDX-License-Identifier: MIT
// gosh-lan-transfer - Engine configuration

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

    /// Build the configuration
    pub fn build(self) -> EngineConfig {
        let default = EngineConfig::default();
        EngineConfig {
            port: self.port.unwrap_or(default.port),
            device_name: self.device_name.unwrap_or(default.device_name),
            download_dir: self.download_dir.unwrap_or(default.download_dir),
            trusted_hosts: self.trusted_hosts.unwrap_or(default.trusted_hosts),
            receive_only: self.receive_only.unwrap_or(default.receive_only),
        }
    }
}
