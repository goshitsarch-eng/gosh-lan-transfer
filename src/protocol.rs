// SPDX-License-Identifier: MIT
//! Protocol types for gosh-lan-transfer
//!
//! This module contains all types that cross the engine boundary:
//! - Wire protocol types (sent between peers over HTTP)
//! - Event types (emitted from engine to consumers)
//! - Shared status enums
//!
//! Rule: If it crosses the engine boundary, it belongs here.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// =============================================================================
// Status Enums - Shared vocabulary for transfer state
// =============================================================================

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

/// Transfer approval decision
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferDecision {
    Pending,
    Accepted,
    Rejected,
    NotFound,
}

// =============================================================================
// Wire Protocol Types - Sent between peers over HTTP
// =============================================================================

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
    /// Relative path within a directory transfer (e.g., "subdir/file.txt")
    /// When present, the receiver will recreate the directory structure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,
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

/// Peer device information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    /// Device name
    pub device_name: String,
    /// Protocol version
    pub version: String,
}

// =============================================================================
// Event Payload Types - Data carried in engine events
// =============================================================================

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

// =============================================================================
// Engine Events - Emitted from engine to consumers
// =============================================================================

/// Events emitted by the engine
///
/// These events cross the engine boundary and are delivered to consumers
/// via the `EventHandler` trait.
#[derive(Debug, Clone)]
pub enum EngineEvent {
    /// New transfer request awaiting approval
    TransferRequest(PendingTransfer),

    /// Progress update for an active transfer
    TransferProgress(TransferProgress),

    /// Transfer completed successfully
    TransferComplete {
        transfer_id: String,
    },

    /// Transfer failed
    TransferFailed {
        transfer_id: String,
        error: String,
    },

    /// Retrying a failed operation
    TransferRetry {
        transfer_id: String,
        /// Current attempt number (1-based)
        attempt: u32,
        /// Maximum attempts allowed
        max_attempts: u32,
        /// Error that triggered the retry
        error: String,
    },

    /// Server started successfully
    ServerStarted {
        port: u16,
    },

    /// Server stopped
    ServerStopped,
}
