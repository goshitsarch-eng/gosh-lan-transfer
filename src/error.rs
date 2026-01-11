// SPDX-License-Identifier: MIT
// gosh-lan-transfer - Error types for the engine

use thiserror::Error;

/// Error types for the transfer engine
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("DNS resolution failed: {0}")]
    DnsResolution(String),

    #[error("Connection refused: {0}")]
    ConnectionRefused(String),

    #[error("Transfer rejected by peer")]
    TransferRejected,

    #[error("File I/O error: {0}")]
    FileIo(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Server not running")]
    ServerNotRunning,

    #[error("Server already running")]
    ServerAlreadyRunning,

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Transfer not found: {0}")]
    TransferNotFound(String),

    #[error("Transfer timed out")]
    TransferTimeout,

    #[error("Invalid token")]
    InvalidToken,
}

/// Result alias for engine operations
pub type EngineResult<T> = Result<T, EngineError>;

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        EngineError::FileIo(err.to_string())
    }
}

impl From<serde_json::Error> for EngineError {
    fn from(err: serde_json::Error) -> Self {
        EngineError::Serialization(err.to_string())
    }
}

impl From<reqwest::Error> for EngineError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() {
            EngineError::ConnectionRefused(err.to_string())
        } else {
            EngineError::Network(err.to_string())
        }
    }
}
