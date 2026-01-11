// SPDX-License-Identifier: MIT
//! Domain types for gosh-lan-transfer
//!
//! This module contains domain entities and utility types that are
//! internal to the engine or used for persistence/history.
//!
//! For types that cross the engine boundary (wire protocol, events),
//! see the `protocol` module.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Re-export protocol types that domain types depend on
pub use crate::protocol::{TransferDirection, TransferFile, TransferStatus};

// =============================================================================
// Domain Entities - Persistence and history
// =============================================================================

/// A saved peer/favorite for quick access
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Favorite {
    /// Unique identifier
    pub id: String,
    /// User-friendly name (e.g., "Living Room PC")
    pub name: String,
    /// Hostname or IP address
    pub address: String,
    /// Last successfully resolved IP (if available)
    pub last_resolved_ip: Option<String>,
    /// When this favorite was last used
    pub last_used: Option<DateTime<Utc>>,
}

impl Favorite {
    pub fn new(name: String, address: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            address,
            last_resolved_ip: None,
            last_used: None,
        }
    }
}

/// A completed or failed transfer record (for history)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRecord {
    /// Unique identifier
    pub id: String,
    /// Direction of transfer
    pub direction: TransferDirection,
    /// Status of the transfer
    pub status: TransferStatus,
    /// Peer address (IP or hostname)
    pub peer_address: String,
    /// Files transferred
    pub files: Vec<TransferFile>,
    /// Total size transferred
    pub total_size: u64,
    /// Bytes actually transferred (for progress/partial)
    pub bytes_transferred: u64,
    /// When the transfer started
    pub started_at: DateTime<Utc>,
    /// When the transfer completed (or failed)
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message if failed
    pub error: Option<String>,
}

// =============================================================================
// Utility Types - Network and resolution
// =============================================================================

/// Network interface information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInterface {
    /// Interface name
    pub name: String,
    /// IP address
    pub ip: String,
    /// Whether this is a loopback interface
    pub is_loopback: bool,
}

/// DNS resolution result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveResult {
    /// Original hostname/address
    pub hostname: String,
    /// Resolved IP addresses
    pub ips: Vec<String>,
    /// Whether resolution was successful
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}
