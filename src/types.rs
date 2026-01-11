// SPDX-License-Identifier: AGPL-3.0
// gosh-lan-transfer - Shared types for the engine

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// Direction of a transfer
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferDirection {
    Sent,
    Received,
}

/// Status of a transfer
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Rejected,
}

/// A single file in a transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferFile {
    /// File name (not full path for security)
    pub name: String,
    /// File size in bytes
    pub size: u64,
    /// MIME type (if detected)
    pub mime_type: Option<String>,
    /// Unique identifier for this file in the transfer
    pub id: String,
}

/// Metadata for a transfer request (sent before actual data)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRequest {
    /// Unique transfer session ID
    pub transfer_id: String,
    /// Optional friendly name of the sender
    pub sender_name: Option<String>,
    /// List of files to be transferred
    pub files: Vec<TransferFile>,
    /// Total size of all files
    pub total_size: u64,
}

/// Response to a transfer request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferResponse {
    /// Whether the transfer was accepted
    pub accepted: bool,
    /// Optional message (e.g., rejection reason)
    pub message: Option<String>,
    /// Token for subsequent chunk uploads (if accepted)
    pub token: Option<String>,
}

/// Transfer approval status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferDecision {
    Pending,
    Accepted,
    Rejected,
    NotFound,
}

/// Status response for a transfer awaiting approval
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferApprovalStatus {
    /// Current approval status
    pub status: TransferDecision,
    /// Token for subsequent chunk uploads (if accepted)
    pub token: Option<String>,
    /// Optional message
    pub message: Option<String>,
}

/// A completed or failed transfer record
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

/// Progress update for an ongoing transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferProgress {
    /// Transfer ID
    pub transfer_id: String,
    /// Current file being transferred
    pub current_file: Option<String>,
    /// Bytes transferred so far
    pub bytes_transferred: u64,
    /// Total bytes to transfer
    pub total_bytes: u64,
    /// Transfer speed in bytes/sec
    pub speed_bps: u64,
}

/// An incoming transfer pending user approval
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingTransfer {
    /// Transfer ID
    pub id: String,
    /// Source IP address
    pub source_ip: String,
    /// Optional sender name
    pub sender_name: Option<String>,
    /// Files to be received
    pub files: Vec<TransferFile>,
    /// Total size
    pub total_size: u64,
    /// When the request was received
    pub received_at: DateTime<Utc>,
}

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

/// Peer device information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    /// Device name
    pub device_name: String,
    /// Protocol version
    pub version: String,
}
