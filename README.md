# gosh-lan-transfer

A Rust library for peer-to-peer file transfers over LAN, VPN, or Tailscale networks.

This crate provides the core transfer engine without any GUI dependencies, making it suitable for use in CLI tools, desktop applications, or as a library dependency.

## Features

- **Direct peer-to-peer transfers** - No cloud, no intermediary servers
- **Cross-platform** - Works on Linux, macOS, and Windows
- **Async/await** - Built on Tokio for efficient async I/O
- **Event-driven** - Flexible event system via traits or channels
- **Trust-based approval** - Auto-accept transfers from trusted hosts
- **Progress tracking** - Real-time progress updates during transfers
- **Favorites management** - Save and manage frequently used peers
- **Zero GUI dependencies** - Use from CLI, GUI, or headless applications

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
gosh-lan-transfer = { git = "https://github.com/your-org/gosh-lan-transfer" }
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, EngineEvent};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = EngineConfig::builder()
        .device_name("My Device")
        .download_dir("/tmp/downloads")
        .port(53317)
        .build();

    // Create engine with channel-based events
    let (mut engine, mut events) = GoshTransferEngine::with_channel_events(config);

    // Start the server to receive files
    engine.start_server().await?;
    println!("Server running on port 53317");

    // Handle events in background
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                EngineEvent::TransferRequest(transfer) => {
                    println!("Incoming transfer from {}: {} files",
                        transfer.source_ip, transfer.files.len());
                }
                EngineEvent::TransferProgress(progress) => {
                    let percent = (progress.bytes_transferred * 100) / progress.total_bytes;
                    println!("Progress: {}%", percent);
                }
                EngineEvent::TransferComplete { transfer_id } => {
                    println!("Transfer {} complete!", transfer_id);
                }
                _ => {}
            }
        }
    });

    // Send files to a peer
    engine.send_files(
        "192.168.1.100",
        53317,
        vec![PathBuf::from("/path/to/file.txt")],
    ).await?;

    Ok(())
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GoshTransferEngine                        │
│  - Coordinates all operations                                │
│  - Manages server lifecycle                                  │
│  - Provides high-level API                                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────┐          ┌─────────────────┐           │
│  │  TransferClient │          │  TransferServer │           │
│  │  (Sending)      │          │  (Receiving)    │           │
│  │                 │          │                 │           │
│  │  - DNS resolve  │          │  - HTTP server  │           │
│  │  - File upload  │          │  - File receive │           │
│  │  - Progress     │          │  - Approval     │           │
│  └─────────────────┘          └─────────────────┘           │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│  protocol (boundary types)     │  events (infrastructure)   │
│  - EngineEvent                 │  - EventHandler trait      │
│  - TransferRequest/Response    │  - ChannelEventHandler     │
│  - TransferProgress            │  - CallbackEventHandler    │
│  - PendingTransfer             │  - NoopEventHandler        │
└─────────────────────────────────────────────────────────────┘
```

## Module Organization

The crate is organized with a clear separation between protocol types and internal implementation:

```
src/
├── lib.rs          # Public API and GoshTransferEngine
├── protocol.rs     # Boundary-crossing types (wire + events)
├── types.rs        # Domain entities (favorites, history)
├── events.rs       # EventHandler trait and implementations
├── config.rs       # EngineConfig
├── error.rs        # EngineError
├── client.rs       # TransferClient (internal)
├── server.rs       # HTTP server (internal)
└── favorites.rs    # FavoritesPersistence trait
```

### Protocol Module

The `protocol` module contains all types that cross the engine boundary:

| Category | Types |
|----------|-------|
| **Wire Protocol** | `TransferRequest`, `TransferResponse`, `TransferApprovalStatus`, `TransferFile`, `PeerInfo` |
| **Events** | `EngineEvent`, `TransferProgress`, `PendingTransfer` |
| **Status Enums** | `TransferStatus`, `TransferDirection`, `TransferDecision` |

**Rule**: If it crosses the engine boundary (sent over HTTP or emitted as an event), it belongs in `protocol`.

### Types Module

The `types` module contains domain entities that don't cross boundaries:

| Type | Purpose |
|------|---------|
| `Favorite` | Saved peer for quick access (persistence) |
| `TransferRecord` | Completed transfer history |
| `NetworkInterface` | Local network interface info |
| `ResolveResult` | DNS resolution result |

## API Reference

### GoshTransferEngine

The main entry point for all operations.

#### Creating an Engine

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, callback_handler, EngineEvent};
use std::sync::Arc;

// Option 1: With channel-based events (recommended for async apps)
let config = EngineConfig::default();
let (mut engine, mut events_rx) = GoshTransferEngine::with_channel_events(config);

// Option 2: With callback-based events
let handler = callback_handler(|event| {
    println!("Event: {:?}", event);
});
let mut engine = GoshTransferEngine::new(EngineConfig::default(), handler);

// Option 3: With no-op handler (discard events)
use gosh_lan_transfer::noop_handler;
let mut engine = GoshTransferEngine::new(EngineConfig::default(), noop_handler());
```

#### Server Operations

```rust
// Start the HTTP server
engine.start_server().await?;

// Check if server is running
if engine.is_server_running() {
    println!("Server is running");
}

// Stop the server
engine.stop_server().await?;
```

#### Sending Files

```rust
use std::path::PathBuf;

// Send files to a peer
let files = vec![
    PathBuf::from("/path/to/document.pdf"),
    PathBuf::from("/path/to/image.png"),
];

engine.send_files("192.168.1.100", 53317, files).await?;
```

The send operation:
1. Sends transfer metadata to the peer
2. Waits for approval (up to 2 minutes)
3. Streams each file with progress updates
4. Emits `TransferComplete` or `TransferFailed` event

#### Receiving Files

When a transfer request comes in, you'll receive a `TransferRequest` event:

```rust
match event {
    EngineEvent::TransferRequest(transfer) => {
        println!("Transfer from: {}", transfer.source_ip);
        println!("Sender: {:?}", transfer.sender_name);
        println!("Files: {:?}", transfer.files);
        println!("Total size: {} bytes", transfer.total_size);

        // Accept or reject the transfer
        if should_accept(&transfer) {
            engine.accept_transfer(&transfer.id).await?;
        } else {
            engine.reject_transfer(&transfer.id).await?;
        }
    }
    _ => {}
}
```

#### Pending Transfers

```rust
// Get all pending transfers awaiting approval
let pending = engine.get_pending_transfers().await;
for transfer in pending {
    println!("{}: {} files from {}",
        transfer.id, transfer.files.len(), transfer.source_ip);
}
```

#### Cancelling Transfers

```rust
// Cancel an in-progress transfer
engine.cancel_transfer(&transfer.id).await?;
// This emits a TransferFailed event and rejects further uploads
```

#### Network Utilities

```rust
// Resolve hostname to IPs (returns ResolveResult)
let result = GoshTransferEngine::resolve_address("mypc.local");
if result.success {
    println!("Resolved to: {:?}", result.ips);
}

// Or use the error-returning variant
let ips = GoshTransferEngine::resolve_address_or_err("mypc.local")?;

// Get all network interfaces
let interfaces = GoshTransferEngine::get_network_interfaces();
for iface in interfaces {
    if !iface.is_loopback {
        println!("{}: {}", iface.name, iface.ip);
    }
}

// Check if a peer is reachable (returns EngineResult<bool>)
match engine.check_peer("192.168.1.100", 53317).await {
    Ok(true) => println!("Peer is online"),
    Ok(false) => println!("Peer returned error status"),
    Err(e) => println!("Could not reach peer: {}", e),
}

// Get peer device info
let info = engine.get_peer_info("192.168.1.100", 53317).await?;
println!("Peer name: {}", info["name"]);
```

#### Configuration Management

```rust
// Get current config
let config = engine.config();
println!("Port: {}", config.port);

// Update config
let new_config = EngineConfig::builder()
    .port(53318)
    .device_name("New Name")
    .build();
engine.update_config(new_config).await;

// Manage trusted hosts
engine.add_trusted_host("192.168.1.50".to_string()).await;
engine.remove_trusted_host("192.168.1.50").await;

let trusted = engine.trusted_hosts();
println!("Trusted hosts: {:?}", trusted);
```

### EngineConfig

Configuration for the engine.

```rust
use gosh_lan_transfer::EngineConfig;
use std::path::PathBuf;

// Using builder pattern
let config = EngineConfig::builder()
    .port(53317)                              // HTTP server port
    .device_name("My Device")                 // Name shown to peers
    .download_dir("/home/user/Downloads")     // Where to save files
    .trusted_hosts(vec!["192.168.1.10".into()]) // Auto-accept from these
    .receive_only(false)                      // Allow sending
    .build();

// Using defaults
let config = EngineConfig::default();
// port: 53317
// device_name: system hostname
// download_dir: current directory
// trusted_hosts: empty
// receive_only: false
```

### Events

The engine emits events for all significant operations.

```rust
use gosh_lan_transfer::EngineEvent;

match event {
    // New transfer request received
    EngineEvent::TransferRequest(transfer) => {
        // transfer.id: String
        // transfer.source_ip: String
        // transfer.sender_name: Option<String>
        // transfer.files: Vec<TransferFile>
        // transfer.total_size: u64
        // transfer.received_at: DateTime<Utc>
    }

    // Progress update during transfer
    EngineEvent::TransferProgress(progress) => {
        // progress.transfer_id: String
        // progress.current_file: Option<String>
        // progress.bytes_transferred: u64
        // progress.total_bytes: u64
        // progress.speed_bps: u64
    }

    // Transfer completed successfully
    EngineEvent::TransferComplete { transfer_id } => {
        println!("Transfer {} done!", transfer_id);
    }

    // Transfer failed
    EngineEvent::TransferFailed { transfer_id, error } => {
        eprintln!("Transfer {} failed: {}", transfer_id, error);
    }

    // Server started
    EngineEvent::ServerStarted { port } => {
        println!("Server listening on port {}", port);
    }

    // Server stopped
    EngineEvent::ServerStopped => {
        println!("Server stopped");
    }
}
```

### Event Handlers

Three built-in event handler implementations:

#### ChannelEventHandler

Best for async applications. Uses Tokio broadcast channels.

```rust
use gosh_lan_transfer::channel_handler;

let (handler, mut receiver) = channel_handler(100); // buffer size

// In async task
tokio::spawn(async move {
    while let Ok(event) = receiver.recv().await {
        // Handle event
    }
});

// Multiple subscribers supported
let mut receiver2 = handler.subscribe();
```

#### CallbackEventHandler

Best for simple use cases or FFI.

```rust
use gosh_lan_transfer::callback_handler;

let handler = callback_handler(|event| {
    match event {
        EngineEvent::TransferProgress(p) => {
            let pct = (p.bytes_transferred * 100) / p.total_bytes;
            print!("\rProgress: {}%", pct);
        }
        _ => {}
    }
});
```

#### NoopEventHandler

Discards all events. Useful for testing or batch operations.

```rust
use gosh_lan_transfer::noop_handler;

let engine = GoshTransferEngine::new(config, noop_handler());
```

#### Custom EventHandler

Implement the trait for custom handling:

```rust
use gosh_lan_transfer::{EventHandler, EngineEvent};

struct MyHandler {
    // your state
}

impl EventHandler for MyHandler {
    fn on_event(&self, event: EngineEvent) {
        // your handling logic
    }
}
```

### Favorites Persistence

The engine provides a `FavoritesPersistence` trait for storing peer favorites.

```rust
use gosh_lan_transfer::{FavoritesPersistence, InMemoryFavorites, Favorite};

// In-memory storage (included)
let store = InMemoryFavorites::new();

// Add a favorite
let fav = store.add("Living Room PC".into(), "192.168.1.100".into())?;
println!("Created favorite: {}", fav.id);

// List all favorites
for fav in store.list()? {
    println!("{}: {} ({})", fav.id, fav.name, fav.address);
}

// Update a favorite
store.update(&fav.id, Some("New Name".into()), None)?;

// Delete a favorite
store.delete(&fav.id)?;
```

#### Custom Persistence

Implement the trait for file-based, database, or cloud storage:

```rust
use gosh_lan_transfer::{FavoritesPersistence, Favorite, EngineResult};

struct FileFavorites {
    path: PathBuf,
}

impl FavoritesPersistence for FileFavorites {
    fn list(&self) -> EngineResult<Vec<Favorite>> {
        // Load from file
    }

    fn add(&self, name: String, address: String) -> EngineResult<Favorite> {
        // Add and save to file
    }

    fn update(&self, id: &str, name: Option<String>, address: Option<String>)
        -> EngineResult<Favorite> {
        // Update and save
    }

    fn delete(&self, id: &str) -> EngineResult<()> {
        // Delete and save
    }

    fn get(&self, id: &str) -> EngineResult<Option<Favorite>> {
        // Find by ID
    }
}
```

### Error Handling

All operations return `EngineResult<T>` which is `Result<T, EngineError>`.

```rust
use gosh_lan_transfer::{EngineError, EngineResult};

match engine.send_files(addr, port, files).await {
    Ok(()) => println!("Success!"),
    Err(EngineError::ConnectionRefused(msg)) => {
        eprintln!("Could not connect: {}", msg);
    }
    Err(EngineError::TransferRejected) => {
        eprintln!("Peer rejected the transfer");
    }
    Err(EngineError::TransferTimeout) => {
        eprintln!("Peer did not respond in time");
    }
    Err(EngineError::FileIo(msg)) => {
        eprintln!("File error: {}", msg);
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

Error variants:
- `Network(String)` - General network error
- `DnsResolution(String)` - DNS lookup failed
- `ConnectionRefused(String)` - Could not connect to peer
- `TransferRejected` - Peer rejected the transfer
- `TransferTimeout` - Approval timeout (2 minutes)
- `TransferNotFound(String)` - Transfer ID not found
- `TransferCancelled` - Transfer was cancelled
- `FileIo(String)` - File read/write error
- `Serialization(String)` - JSON serialization error
- `ServerNotRunning` - Server not started
- `ServerAlreadyRunning` - Server already running
- `InvalidConfig(String)` - Configuration error

## Transfer Protocol

The engine uses HTTP for all transfers. This ensures compatibility across firewalls and NAT.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check, returns `{"status": "ok", "app": "gosh-lan-transfer", "version": "..."}` |
| `/info` | GET | Device info: name, version |
| `/transfer` | POST | Initiate transfer request |
| `/transfer/status` | GET | Check approval status |
| `/chunk` | POST | Upload file data |
| `/events` | GET | SSE stream for real-time events |

### Transfer Flow

```
SENDER                                    RECEIVER
  │                                          │
  │── POST /transfer ──────────────────────▶│
  │   {transfer_id, files[], total_size}     │
  │                                          │
  │◀── 200 {accepted: false} ───────────────│
  │    (or accepted: true if trusted)        │
  │                                          │
  │                                          │ User approves
  │                                          │
  │── GET /transfer/status ────────────────▶│
  │◀── {status: "accepted", token: "..."} ──│
  │                                          │
  │── POST /chunk?token=...&file_id=... ───▶│
  │   [binary file data]                     │
  │◀── 200 OK ──────────────────────────────│
  │                                          │
  │   (repeat for each file)                 │
  │                                          │
```

### Security

- **Token-based uploads**: Each approved transfer gets a unique UUID token
- **Filename sanitization**: Path traversal attacks are prevented
- **Size validation**: Files exceeding declared size are rejected
- **No authentication**: Designed for trusted networks (LAN, VPN)

## Examples

### CLI File Sender

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, EngineEvent, callback_handler};
use std::{env, path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <address> <file1> [file2...]", args[0]);
        std::process::exit(1);
    }

    let address = &args[1];
    let files: Vec<PathBuf> = args[2..].iter().map(PathBuf::from).collect();

    // Progress callback
    let handler = callback_handler(|event| {
        if let EngineEvent::TransferProgress(p) = event {
            let pct = (p.bytes_transferred * 100) / p.total_bytes;
            eprint!("\rSending: {}%  ", pct);
        }
    });

    let config = EngineConfig::builder()
        .device_name("CLI Sender")
        .build();

    let engine = GoshTransferEngine::new(config, handler);

    println!("Sending {} file(s) to {}...", files.len(), address);
    engine.send_files(address, 53317, files).await?;
    println!("\nDone!");

    Ok(())
}
```

### CLI File Receiver

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, EngineEvent};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::builder()
        .device_name("CLI Receiver")
        .download_dir(PathBuf::from("./downloads"))
        .build();

    let (mut engine, mut events) = GoshTransferEngine::with_channel_events(config);

    engine.start_server().await?;
    println!("Listening on port 53317...");
    println!("Press Ctrl+C to stop");

    while let Ok(event) = events.recv().await {
        match event {
            EngineEvent::TransferRequest(transfer) => {
                println!("\nIncoming transfer from {}:", transfer.source_ip);
                for file in &transfer.files {
                    println!("  - {} ({} bytes)", file.name, file.size);
                }

                // Auto-accept all transfers
                engine.accept_transfer(&transfer.id).await?;
                println!("Accepted!");
            }
            EngineEvent::TransferProgress(p) => {
                let pct = (p.bytes_transferred * 100) / p.total_bytes;
                eprint!("\rReceiving: {}%  ", pct);
            }
            EngineEvent::TransferComplete { .. } => {
                println!("\nTransfer complete!");
            }
            EngineEvent::TransferFailed { error, .. } => {
                eprintln!("\nTransfer failed: {}", error);
            }
            _ => {}
        }
    }

    Ok(())
}
```

### Integration with GUI Framework

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, EngineEvent};
use std::sync::Arc;
use tokio::sync::Mutex;

struct MyApp {
    engine: Arc<Mutex<GoshTransferEngine>>,
}

impl MyApp {
    pub async fn new() -> Self {
        let config = EngineConfig::default();
        let (engine, events) = GoshTransferEngine::with_channel_events(config);
        let engine = Arc::new(Mutex::new(engine));

        // Start event handling
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            let mut events = events;
            while let Ok(event) = events.recv().await {
                // Update UI based on event
                Self::handle_event(event).await;
            }
        });

        // Start server
        engine.lock().await.start_server().await.unwrap();

        Self { engine }
    }

    async fn handle_event(event: EngineEvent) {
        // Send to UI thread
        match event {
            EngineEvent::TransferRequest(t) => {
                // Show approval dialog
            }
            EngineEvent::TransferProgress(p) => {
                // Update progress bar
            }
            _ => {}
        }
    }

    pub async fn send_files(&self, address: &str, files: Vec<PathBuf>) {
        let engine = self.engine.lock().await;
        if let Err(e) = engine.send_files(address, 53317, files).await {
            // Show error dialog
        }
    }
}
```

## Types Reference

### Protocol Types (`gosh_lan_transfer::protocol`)

These types cross the engine boundary (wire protocol or events).

#### TransferFile

```rust
pub struct TransferFile {
    pub id: String,                // UUID for this file
    pub name: String,              // Filename (no path)
    pub size: u64,                 // Size in bytes
    pub mime_type: Option<String>, // MIME type if detected
}
```

#### TransferRequest (Wire)

```rust
pub struct TransferRequest {
    pub transfer_id: String,           // Unique transfer ID
    pub sender_name: Option<String>,   // Sender's device name
    pub files: Vec<TransferFile>,      // Files to transfer
    pub total_size: u64,               // Total bytes
}
```

#### TransferResponse (Wire)

```rust
pub struct TransferResponse {
    pub accepted: bool,            // Whether accepted
    pub message: Option<String>,   // Message (e.g., rejection reason)
    pub token: Option<String>,     // Upload token if accepted
}
```

#### PendingTransfer (Event)

```rust
pub struct PendingTransfer {
    pub id: String,                      // Transfer ID
    pub source_ip: String,               // Sender's IP
    pub sender_name: Option<String>,     // Sender's device name
    pub files: Vec<TransferFile>,        // Files to receive
    pub total_size: u64,                 // Total bytes
    pub received_at: DateTime<Utc>,      // When request arrived
}
```

#### TransferProgress (Event)

```rust
pub struct TransferProgress {
    pub transfer_id: String,           // Transfer ID
    pub current_file: Option<String>,  // Current filename
    pub bytes_transferred: u64,        // Bytes sent/received
    pub total_bytes: u64,              // Total bytes
    pub speed_bps: u64,                // Speed in bytes/sec
}
```

#### Status Enums

```rust
pub enum TransferDirection { Sent, Received }
pub enum TransferStatus { Pending, InProgress, Completed, Failed, Rejected }
pub enum TransferDecision { Pending, Accepted, Rejected, NotFound }
```

### Domain Types (`gosh_lan_transfer::types`)

These types are internal domain entities.

#### Favorite

```rust
pub struct Favorite {
    pub id: String,                          // UUID
    pub name: String,                        // Display name
    pub address: String,                     // Hostname or IP
    pub last_resolved_ip: Option<String>,    // Cached IP
    pub last_used: Option<DateTime<Utc>>,    // Last used time
}
```

#### TransferRecord

```rust
pub struct TransferRecord {
    pub id: String,
    pub direction: TransferDirection,
    pub status: TransferStatus,
    pub peer_address: String,
    pub files: Vec<TransferFile>,
    pub total_size: u64,
    pub bytes_transferred: u64,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}
```

#### NetworkInterface

```rust
pub struct NetworkInterface {
    pub name: String,      // Interface name (eth0, wlan0, etc.)
    pub ip: String,        // IP address
    pub is_loopback: bool, // Is loopback interface
}
```

#### ResolveResult

```rust
pub struct ResolveResult {
    pub hostname: String,        // Original hostname
    pub ips: Vec<String>,        // Resolved IPs
    pub success: bool,           // Resolution succeeded
    pub error: Option<String>,   // Error message if failed
}
```

## License

MIT - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.
