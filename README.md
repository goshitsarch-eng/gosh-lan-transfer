# gosh-lan-transfer

[![Crates.io](https://img.shields.io/crates/v/gosh-lan-transfer.svg)](https://crates.io/crates/gosh-lan-transfer)
[![Documentation](https://docs.rs/gosh-lan-transfer/badge.svg)](https://docs.rs/gosh-lan-transfer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Rust library for peer-to-peer file transfers over LAN, VPN, or Tailscale networks.

This crate provides the core transfer engine without any GUI dependencies, making it suitable for integration into CLI tools, desktop applications, mobile apps, or headless services. Files transfer directly between devices with no cloud intermediary, keeping your data private and your transfers fast.

## Why gosh-lan-transfer?

Sharing files between devices on the same network shouldn't require uploading to the cloud, installing platform-specific software, or configuring SSH keys. Yet existing solutions each come with significant limitations.

**AirDrop and Nearby Share** work seamlessly within their ecosystems, but they lock you into Apple or Google platforms respectively. If you have a mix of devices a MacBook, a Windows desktop, a Linux server, and an Android phone these solutions leave you stranded.

**Cloud services** like Dropbox, Google Drive, or WeTransfer solve the cross platform problem, but they route your files through external servers. For a 10GB video file sitting on your laptop that you want on your desktop three feet away, uploading to the cloud and downloading again wastes time and bandwidth. It also means your files pass through third-party infrastructure, which may be unacceptable for sensitive data.

**SCP and rsync** are powerful and cross platform, but they require SSH access, key management, and command line knowledge. They're tools for sysadmins, not for quickly sending a folder to a colleague.

**LocalSend** offers a good user experience for casual file sharing, but it's an end-user application, not a library. If you're building your own file sharing feature into an application, you can't easily integrate it.

gosh-lan-transfer takes a different approach. It's a library first, designed to be embedded into whatever application you're building. The HTTP-based protocol works across any platform that supports TCP. Discovery is optional: you can always specify the target device's IP or hostname directly, which works equally well on traditional LANs, corporate VPNs, and Tailscale networks where mDNS discovery often fails. On networks that allow multicast, the built-in UDP multicast discovery finds nearby peers automatically.

The library handles the complexity of file transfer progress tracking, approval workflows, retry logic, directory transfers, while staying out of your way on everything else. You provide the UI, the storage backend, and the user experience. gosh-lan-transfer provides reliable, fast, direct transfers.

## Core Capabilities

The engine supports sending individual files or entire directory trees with their structure preserved. When receiving, transfers require explicit approval unless the sender is in your trusted hosts list, preventing unwanted file pushes. Progress updates stream in real-time with transfer speeds calculated on the fly. If a network hiccup interrupts the connection, automatic retry with exponential backoff handles transient failures gracefully.

The event-driven architecture means your application stays responsive. Whether you're building a GUI that needs to update a progress bar, a CLI that prints status to the terminal, or a headless service that logs to a file, the same event stream powers all of them. Three built-in event handlers cover common cases, and implementing your own takes just a few lines.

For applications that need to remember transfer history or save frequently used peers, the library defines persistence traits that you implement with whatever storage backend fits your needs SQLite, JSON files, or a full database. In memory implementations ship with the library for testing and simple use cases.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
gosh-lan-transfer = "0.3"
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
                    let percent = if progress.total_bytes > 0 {
                        (progress.bytes_transferred * 100) / progress.total_bytes
                    } else {
                        100
                    };
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

The library centers on `GoshTransferEngine`, which coordinates all operations. Internally, it manages an HTTP server for receiving files and an HTTP client for sending them. The engine exposes a high-level API while handling connection management, progress tracking, and error recovery behind the scenes.

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

Types are organized with a clear separation between what crosses the engine boundary and what stays internal. The `protocol` module contains wire protocol types and event payloads anything sent over HTTP or emitted as an event lives here. The `types` module holds domain entities like favorites and transfer records that don't leave the local process.

## Module Organization

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
├── discovery.rs    # UDP multicast peer discovery
├── throttle.rs     # Send-side bandwidth pacing (internal)
├── favorites.rs    # FavoritesPersistence trait
└── history.rs      # HistoryPersistence trait
```

## API Reference

### GoshTransferEngine

The main entry point for all operations. You create an engine with a configuration and an event handler, then use it to send files, receive files, and manage transfers.

#### Creating an Engine

The library offers several ways to create an engine depending on how you want to handle events. Channel based events work best for async applications where you want to process events in a separate task. Callback based events suit simpler use cases or FFI scenarios. The no-op handler discards all events, useful for batch operations or testing.

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

The HTTP server listens for incoming transfer requests. Starting and stopping the server is straightforward, and you can change the port at runtime if needed. When you change the port, the server gracefully shuts down and restarts on the new port. If binding to the new port fails, it automatically rolls back to the previous port.

```rust
// Start the HTTP server
engine.start_server().await?;

// Check if server is running
if engine.is_server_running() {
    println!("Server is running");
}

// Stop the server
engine.stop_server().await?;

// Get current port
let current_port = engine.port();

// Change port at runtime (gracefully restarts server)
engine.change_port(8080).await?;

// Change port without rollback on failure
engine.change_port_with_options(8080, false).await?;
```

#### Sending Files

Sending files involves specifying the target peer's address and port, along with the files to send. The engine handles DNS resolution, negotiates the transfer with the peer, waits for approval, and streams the files with progress updates.

```rust
use std::path::PathBuf;

let files = vec![
    PathBuf::from("/path/to/document.pdf"),
    PathBuf::from("/path/to/image.png"),
];

engine.send_files("192.168.1.100", 53317, files).await?;
```

The send operation first transmits file metadata to the peer, then waits up to two minutes for approval. Once approved, it streams each file while emitting progress events. If the network connection drops, it automatically retries with exponential backoff. When all files are sent, a `TransferComplete` event fires; if something goes wrong, you get a `TransferFailed` event with the error details.

#### Sending Directories

For directory transfers, the library recursively enumerates all files and sends them with their relative paths preserved. The receiver automatically recreates the directory structure under its download directory.

```rust
engine.send_directory("192.168.1.100", 53317, "/path/to/folder").await?;
```

#### Receiving Files

When a transfer request arrives, your application receives a `TransferRequest` event containing the sender's IP, device name, file list, and total size. You then decide whether to accept or reject the transfer. Accepted transfers receive a unique token that authorizes the sender to upload files.

```rust
match event {
    EngineEvent::TransferRequest(transfer) => {
        println!("Transfer from: {}", transfer.source_ip);
        println!("Sender: {:?}", transfer.sender_name);
        println!("Files: {:?}", transfer.files);
        println!("Total size: {} bytes", transfer.total_size);

        if should_accept(&transfer) {
            engine.accept_transfer(&transfer.id).await?;
        } else {
            engine.reject_transfer(&transfer.id).await?;
        }
    }
    _ => {}
}
```

#### Batch Operations

When multiple transfers are pending, you can accept or reject them all at once. The batch methods return results for each transfer, so you can handle individual failures.

```rust
// Accept all pending transfers
let results = engine.accept_all_transfers().await;
for (transfer_id, result) in results {
    match result {
        Ok(token) => println!("Accepted {}", transfer_id),
        Err(e) => eprintln!("Failed to accept {}: {}", transfer_id, e),
    }
}

// Reject all pending transfers
let results = engine.reject_all_transfers().await;
```

#### Cancelling Transfers

You can cancel an in-progress transfer at any time. Cancellation emits a `TransferFailed` event and causes subsequent upload attempts to be rejected.

```rust
engine.cancel_transfer(&transfer.id).await?;
```

#### Network Utilities

The library includes utilities for DNS resolution, network interface enumeration, and peer health checks. These help you build features like peer discovery or connection status indicators.

```rust
// Resolve hostname to IPs
let result = GoshTransferEngine::resolve_address("mypc.local");
if result.success {
    println!("Resolved to: {:?}", result.ips);
}

// Get all network interfaces
let interfaces = GoshTransferEngine::get_network_interfaces();
for iface in interfaces {
    if !iface.is_loopback {
        println!("{}: {}", iface.name, iface.ip);
    }
}

// Check if a peer is reachable
match engine.check_peer("192.168.1.100", 53317).await {
    Ok(true) => println!("Peer is online"),
    Err(e) => println!("Could not reach peer: {}", e),
}

// Get peer device info
let info = engine.get_peer_info("192.168.1.100", 53317).await?;
println!("Peer name: {}", info["name"]);
```

#### Peer Discovery

Devices running gosh-lan-transfer can find each other automatically via UDP multicast (group `224.0.0.167`, UDP port `53318` by default). Each engine periodically announces its device name and transfer port; listeners maintain a peer table, reply so the announcer learns about them too, and expire peers that stop announcing.

```rust
use std::time::Duration;

// One-shot: scan for 3 seconds and return what was found
let peers = engine.discover_peers(Duration::from_secs(3)).await?;
for peer in peers {
    println!("{} at {}:{}", peer.device_name, peer.address, peer.port);
}

// Or run discovery continuously and react to events
engine.start_discovery().await?;
// ... EngineEvent::PeerDiscovered / EngineEvent::PeerLost arrive as peers come and go
let peers = engine.discovered_peers().await; // snapshot at any time
engine.stop_discovery().await?;
```

Discovery is advisory only: announcements are unauthenticated, so the peer list is a convenience for the UI, never an authorization. Incoming transfers still require explicit approval or a trusted-host entry.

Notes for multicast environments:
- UDP port 53318 must be open in the firewall for discovery to work; file transfers themselves only need the TCP transfer port.
- On macOS 14+, GUI applications embedding this library may trigger the Local Network privacy prompt before multicast works.
- Multicast is often filtered on VPNs and Tailscale; direct IP/hostname addressing always remains available.

#### Configuration Management

Configuration can be updated at runtime. Trusted hosts determine which peers can send files without requiring manual approval.

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
```

### EngineConfig

The configuration uses a builder pattern with sensible defaults. You can create a working configuration with just `EngineConfig::default()`, or customize specific fields as needed.

```rust
use gosh_lan_transfer::EngineConfig;
use std::path::PathBuf;

let config = EngineConfig::builder()
    .port(53317)                              // HTTP server port
    .device_name("My Device")                 // Name shown to peers
    .download_dir("/home/user/Downloads")     // Where to save files
    .trusted_hosts(vec!["192.168.1.10".into()]) // Auto-accept from these
    .receive_only(false)                      // Allow sending
    .max_retries(3)                           // Retry failed transfers
    .retry_delay_ms(1000)                     // Delay between retries
    .bandwidth_limit_bps(Some(5_000_000))     // Cap outgoing transfers at ~5 MB/s
    .discovery_port(53318)                    // UDP port for peer discovery
    .discovery_announce_interval_secs(5)      // How often to announce
    .discovery_peer_timeout_secs(15)          // When silent peers are considered lost
    .build();
```

The defaults use port 53317, the system hostname as the device name, the current directory for downloads, no trusted hosts, three retry attempts with a one-second base delay, and no bandwidth limit. `bandwidth_limit_bps` throttles the send side only; receiving is never rate-limited.

### Events

The engine emits events for all significant state changes. Your application subscribes to these events to update UI, log activity, or trigger other actions.

```rust
use gosh_lan_transfer::EngineEvent;

match event {
    EngineEvent::TransferRequest(transfer) => {
        // New transfer request received, awaiting approval
    }

    EngineEvent::TransferProgress(progress) => {
        // Progress update with bytes transferred, total bytes, and speed
    }

    EngineEvent::TransferComplete { transfer_id } => {
        // Transfer finished successfully
    }

    EngineEvent::TransferFailed { transfer_id, error } => {
        // Transfer failed with error message
    }

    EngineEvent::TransferRetry { transfer_id, attempt, max_attempts, error } => {
        // Retrying after transient failure
    }

    EngineEvent::ServerStarted { port } => {
        // Server is now listening
    }

    EngineEvent::ServerStopped => {
        // Server has shut down
    }

    EngineEvent::PortChanged { old_port, new_port } => {
        // Server port changed at runtime
    }

    EngineEvent::PeerDiscovered(peer) => {
        // A peer was found via multicast discovery
    }

    EngineEvent::PeerLost { fingerprint, device_name } => {
        // A discovered peer stopped announcing and timed out
    }

    _ => {
        // EngineEvent is #[non_exhaustive]; future versions may add variants
    }
}
```

`TransferProgress` events report transfer-wide totals on both the sending and receiving side: `bytes_transferred` accumulates across all files in the transfer and `total_bytes` is the size of the whole transfer, with `current_file` naming the file currently in flight.

### Event Handlers

Three built-in handlers cover common scenarios. The channel handler uses Tokio broadcast channels and supports multiple subscribers, making it ideal for async applications. The callback handler wraps a closure, suitable for simple cases or FFI. The no-op handler discards events silently.

```rust
use gosh_lan_transfer::channel_handler;

// Channel-based: multiple subscribers, async-friendly
let (handler, mut receiver) = channel_handler(100);

tokio::spawn(async move {
    while let Ok(event) = receiver.recv().await {
        // Handle event
    }
});

// Additional subscribers can be created
let mut receiver2 = handler.subscribe();
```

For custom handling, implement the `EventHandler` trait:

```rust
use gosh_lan_transfer::{EventHandler, EngineEvent};

struct MyHandler { /* your state */ }

impl EventHandler for MyHandler {
    fn on_event(&self, event: EngineEvent) {
        // your handling logic
    }
}
```

### Persistence

The library doesn't impose a storage backend. Instead, it defines traits for favorites and history persistence that you implement with whatever storage fits your application.

#### Favorites

Favorites let users save frequently-used peers for quick access. The `FavoritesPersistence` trait defines CRUD operations, and `InMemoryFavorites` provides a simple in-memory implementation.

```rust
use gosh_lan_transfer::{FavoritesPersistence, InMemoryFavorites};

let store = InMemoryFavorites::new();

let fav = store.add("Living Room PC".into(), "192.168.1.100".into())?;
store.update(&fav.id, Some("New Name".into()), None)?;
store.delete(&fav.id)?;
```

#### History

Transfer history records completed and failed transfers automatically when you provide a `HistoryPersistence` implementation. The `InMemoryHistory` implementation optionally limits how many records it keeps.

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, InMemoryHistory};
use std::sync::Arc;

let history = Arc::new(InMemoryHistory::with_limit(1000));
let config = EngineConfig::default();

let (mut engine, events) = GoshTransferEngine::with_channel_events_and_history(
    config,
    history.clone(),
);

// Later: query history
let records = history.list()?;
let page = history.list_paginated(0, 10)?;
```

### Error Handling

All operations return `EngineResult<T>`, which is `Result<T, EngineError>`. The error type covers network issues, file I/O problems, protocol errors, and configuration mistakes.

```rust
use gosh_lan_transfer::EngineError;

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
    Err(e) => eprintln!("Error: {}", e),
}
```

## Transfer Protocol

The engine uses HTTP for all transfers, ensuring compatibility across firewalls and NAT. No custom binary protocol means standard tools can inspect traffic for debugging.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/info` | GET | Device name and version |
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

Each approved transfer receives a unique UUID token that must accompany all file uploads, preventing unauthorized data injection. Received filenames are sanitized to prevent path traversal attacks only the filename component is used, and parent directory references are stripped. Files exceeding their declared size are rejected and deleted.

Discovery announcements are unauthenticated UDP datagrams and can be spoofed by anyone on the local network. They only populate the peer list: device names are sanitized (control characters stripped, length capped), and discovering a peer never grants it transfer permissions — every incoming transfer still goes through the approval or trusted-host flow.

The library is designed for trusted networks and does not implement user authentication. If you need transfers over untrusted networks, layer TLS on top or use a VPN.

## Examples

### CLI File Sender

```rust
use gosh_lan_transfer::{GoshTransferEngine, EngineConfig, EngineEvent, callback_handler};
use std::{env, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <address> <file1> [file2...]", args[0]);
        std::process::exit(1);
    }

    let address = &args[1];
    let files: Vec<PathBuf> = args[2..].iter().map(PathBuf::from).collect();

    let handler = callback_handler(|event| {
        if let EngineEvent::TransferProgress(p) = event {
            let pct = if p.total_bytes > 0 {
                (p.bytes_transferred * 100) / p.total_bytes
            } else {
                100
            };
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

    while let Ok(event) = events.recv().await {
        match event {
            EngineEvent::TransferRequest(transfer) => {
                println!("\nIncoming transfer from {}:", transfer.source_ip);
                for file in &transfer.files {
                    println!("  - {} ({} bytes)", file.name, file.size);
                }
                engine.accept_transfer(&transfer.id).await?;
            }
            EngineEvent::TransferProgress(p) => {
                let pct = if p.total_bytes > 0 {
                    (p.bytes_transferred * 100) / p.total_bytes
                } else {
                    100
                };
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

## Types Reference

### Protocol Types

These types cross the engine boundary they're either sent over HTTP or emitted as events.

**TransferFile** represents a single file in a transfer, with an ID, name, size, optional MIME type, and optional relative path for directory transfers.

**TransferRequest** is the wire format for initiating a transfer, containing the transfer ID, sender name, file list, and total size.

**TransferResponse** comes back from the receiver, indicating whether the transfer was accepted and providing an upload token if so.

**PendingTransfer** is the event payload for incoming transfer requests, adding the sender's IP and timestamp to the request data.

**TransferProgress** carries progress updates: transfer ID, current file name, bytes transferred, total bytes, and speed in bytes per second.

### Domain Types

These types stay within the local process.

**Favorite** represents a saved peer with ID, display name, address, cached IP, and last used timestamp.

**TransferRecord** captures completed or failed transfer history: direction, status, peer address, file list, sizes, timestamps, and error message if applicable.

**NetworkInterface** describes a local network interface with its name, IP address, and loopback flag.

**ResolveResult** holds DNS resolution results: the original hostname, resolved IPs, success flag, and error message if resolution failed.

## Disclaimer

This project is independent and is not sponsored by, endorsed by, or affiliated with LocalSend or GitHub, Inc.

It is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability or fitness for a particular purpose. Use at your own risk.

## License

MIT - See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome. Please feel free to submit issues and pull requests.
