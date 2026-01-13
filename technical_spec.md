# Technical Specification

## Overview

gosh-lan-transfer is a Rust library providing peer-to-peer file transfer capabilities over LAN, VPN, and Tailscale networks. It operates without cloud dependencies, performing all transfers directly between peers using HTTP.

## System Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    GoshTransferEngine                        │
│  - Coordinates all operations                                │
│  - Manages server lifecycle                                  │
│  - Provides high-level API                                   │
│  - Automatic history recording                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────┐          ┌─────────────────┐           │
│  │  TransferClient │          │  TransferServer │           │
│  │  (Sending)      │          │  (Receiving)    │           │
│  │                 │          │                 │           │
│  │  - DNS resolve  │          │  - HTTP server  │           │
│  │  - File upload  │          │  - File receive │           │
│  │  - Progress     │          │  - Approval     │           │
│  │  - Retry logic  │          │  - Dir support  │           │
│  └─────────────────┘          └─────────────────┘           │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│  protocol (boundary types)     │  events (infrastructure)   │
│  - EngineEvent                 │  - EventHandler trait      │
│  - TransferRequest/Response    │  - ChannelEventHandler     │
│  - TransferProgress            │  - CallbackEventHandler    │
│  - PendingTransfer             │  - NoopEventHandler        │
├─────────────────────────────────────────────────────────────┤
│  history (persistence)         │  config (settings)         │
│  - HistoryPersistence trait    │  - EngineConfig            │
│  - InMemoryHistory             │  - EngineConfigBuilder     │
│  - TransferRecord              │  - Retry/bandwidth options │
└─────────────────────────────────────────────────────────────┘
```

### Design Decisions

1. **HTTP-based protocol**: Uses HTTP/1.1 for all transfers to ensure compatibility across firewalls and NAT. No custom binary protocol.

2. **Approval workflow**: Transfers require explicit approval unless the sender is in the trusted hosts list. This prevents unwanted file pushes.

3. **Token-based uploads**: Each approved transfer receives a UUID token that must be provided with chunk uploads, preventing unauthorized data injection.

4. **Event-driven architecture**: All state changes emit events through the `EventHandler` trait, allowing flexible integration with different UI frameworks.

5. **Type boundary separation**: Wire protocol types and event payloads live in `protocol.rs`; internal domain types live in `types.rs`. This keeps the API surface clean.

## Technology Stack

### Runtime & Async

- **tokio** (1.x) - Async runtime with full features (rt-multi-thread, io, net, sync, time, fs)
- **futures** / **futures-util** (0.3) - Stream utilities and combinators
- **tokio-util** (0.7) - I/O utilities including `ReaderStream`
- **tokio-stream** (0.1) - Stream wrappers for broadcast channels

### HTTP

- **axum** (0.8) - HTTP server framework with WebSocket support
- **hyper** (1.x) - Low-level HTTP implementation
- **hyper-util** (0.1) - Hyper utilities
- **tower** (0.5) - Service middleware
- **tower-http** (0.6) - HTTP-specific middleware (CORS, static files)
- **reqwest** (0.12) - HTTP client with streaming, multipart, and JSON support

### Serialization

- **serde** (1.x) - Serialization framework with derive macros
- **serde_json** (1.x) - JSON serialization

### Utilities

- **uuid** (1.x) - UUID generation (v4)
- **chrono** (0.4) - Date/time handling with serde support
- **thiserror** (2.x) - Error derive macros
- **tracing** (0.1) - Structured logging
- **mime_guess** (2.x) - MIME type detection from file extensions
- **local-ip-address** (0.6) - Network interface enumeration
- **hostname** (0.4) - System hostname retrieval
- **bytes** (1.x) - Byte buffer utilities

## Data Models

### Wire Protocol Types

```rust
// Transfer request sent from sender to receiver
struct TransferRequest {
    transfer_id: String,      // UUID
    sender_name: Option<String>,
    files: Vec<TransferFile>,
    total_size: u64,
}

// Single file in a transfer
struct TransferFile {
    id: String,               // UUID
    name: String,             // Filename only (no path)
    size: u64,
    mime_type: Option<String>,
    relative_path: Option<String>, // For directory transfers
}

// Response to transfer request
struct TransferResponse {
    accepted: bool,
    message: Option<String>,
    token: Option<String>,    // Upload token if accepted
}

// Approval status (polling response)
struct TransferApprovalStatus {
    status: TransferDecision, // Pending | Accepted | Rejected | NotFound
    token: Option<String>,
    message: Option<String>,
}
```

### Event Payloads

```rust
enum EngineEvent {
    TransferRequest(PendingTransfer),
    TransferProgress(TransferProgress),
    TransferComplete { transfer_id: String },
    TransferFailed { transfer_id: String, error: String },
    TransferRetry { transfer_id: String, attempt: u32, max_attempts: u32, error: String },
    ServerStarted { port: u16 },
    ServerStopped,
}

struct TransferProgress {
    transfer_id: String,
    current_file: Option<String>,
    bytes_transferred: u64,
    total_bytes: u64,
    speed_bps: u64,
}
```

### Domain Types

```rust
// Saved peer for quick access
struct Favorite {
    id: String,
    name: String,
    address: String,
    last_resolved_ip: Option<String>,
    last_used: Option<DateTime<Utc>>,
}

// Transfer history record
struct TransferRecord {
    id: String,
    direction: TransferDirection,
    status: TransferStatus,
    peer_address: String,
    files: Vec<TransferFile>,
    total_size: u64,
    bytes_transferred: u64,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    error: Option<String>,
}
```

### Configuration

```rust
struct EngineConfig {
    port: u16,                           // Default: 53317
    device_name: String,                 // Default: system hostname
    download_dir: PathBuf,               // Default: current directory
    trusted_hosts: Vec<String>,          // Default: empty
    receive_only: bool,                  // Default: false
    max_retries: u32,                    // Default: 3
    retry_delay_ms: u64,                 // Default: 1000
    bandwidth_limit_bps: Option<u64>,    // Default: None (unlimited)
}
```

### Persistence Traits

```rust
// Transfer history persistence
trait HistoryPersistence: Send + Sync {
    fn list(&self) -> EngineResult<Vec<TransferRecord>>;
    fn list_paginated(&self, offset: usize, limit: usize) -> EngineResult<Vec<TransferRecord>>;
    fn get(&self, transfer_id: &str) -> EngineResult<Option<TransferRecord>>;
    fn add(&self, record: TransferRecord) -> EngineResult<()>;
    fn delete(&self, transfer_id: &str) -> EngineResult<()>;
    fn clear(&self) -> EngineResult<()>;
    fn count(&self) -> EngineResult<usize>;
}

// Favorites persistence (peers)
trait FavoritesPersistence: Send + Sync {
    fn list(&self) -> EngineResult<Vec<Favorite>>;
    fn add(&self, favorite: Favorite) -> EngineResult<()>;
    fn update(&self, favorite: Favorite) -> EngineResult<()>;
    fn delete(&self, id: &str) -> EngineResult<()>;
    fn get(&self, id: &str) -> EngineResult<Option<Favorite>>;
    fn touch(&self, id: &str) -> EngineResult<()>;
}
```

## API Specification

### HTTP Endpoints

| Endpoint | Method | Description | Request Body | Response |
|----------|--------|-------------|--------------|----------|
| `/health` | GET | Health check | - | `{"status": "ok", "app": "gosh-lan-transfer", "version": "..."}` |
| `/info` | GET | Device info | - | `{"name": "...", "version": "...", "app": "gosh-lan-transfer"}` |
| `/transfer` | POST | Initiate transfer | `TransferRequest` | `TransferResponse` |
| `/transfer/status` | GET | Check approval | `?transfer_id=...` | `TransferApprovalStatus` |
| `/chunk` | POST | Upload file data | Binary stream | `{"status": "ok", "file": "...", "bytes_received": ...}` |
| `/events` | GET | SSE event stream | - | Server-Sent Events |

### Chunk Upload Query Parameters

- `transfer_id` - Transfer session ID
- `file_id` - File ID within the transfer
- `token` - Authorization token from approval

## Performance Considerations

1. **Progress throttling**: Progress events are throttled to every 32KB to avoid flooding event handlers during high-speed transfers.

2. **Streaming uploads**: Files are streamed directly from disk using `ReaderStream`, avoiding full file buffering in memory.

3. **No global timeout**: The HTTP client has no global timeout since large transfers can take hours. Read timeout (60s) and connect timeout (30s) detect stalled or unreachable connections.

4. **Unique filename handling**: The receiver generates unique filenames using `(n)` suffix pattern when conflicts occur, testing up to 1000 variations.

5. **Speed calculation**: Transfer speed is calculated as total bytes transferred divided by elapsed time since transfer start.

6. **Retry logic**: Transient failures (network errors, connection refused) are automatically retried with exponential backoff: `delay * 2^attempt`. Only the initial transfer request is retried, not individual chunk uploads.

7. **Directory transfers**: Files are collected recursively and sent with relative paths. The receiver creates subdirectories as needed under the download directory.

## Security Considerations

1. **Token-based authorization**: Upload tokens are UUID v4, providing 122 bits of randomness.

2. **Filename sanitization**: Received filenames are sanitized to prevent path traversal attacks. Only the filename component is used.

3. **Relative path sanitization**: Directory transfer paths are sanitized to prevent traversal attacks. Parent directory components (`..`) and absolute paths are stripped.

4. **Size validation**: Files exceeding their declared size are rejected and deleted.

5. **No authentication**: The library is designed for trusted networks (LAN, VPN, Tailscale). It does not implement user authentication.

6. **Trust model**: Auto-acceptance only applies to explicitly configured trusted host IPs.

## Infrastructure Requirements

### Network

- TCP port 53317 (default, configurable)
- Server binds to [::] (IPv6 dual-stack) with fallback to 0.0.0.0 (IPv4)
- Works across LAN, Tailscale, and VPN networks
- Supports both IPv4 and IPv6 clients

### Filesystem

- Read access to files being sent
- Write access to download directory
- Download directory is created if it doesn't exist

### Runtime

- Tokio async runtime with multi-thread scheduler
- No specific OS requirements beyond Rust's supported platforms (Linux, macOS, Windows)
