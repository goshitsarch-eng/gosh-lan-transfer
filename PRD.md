# Product Requirements Document

> Updated for 0.5.0: see [secure transfers](docs/SECURE_TRANSFERS.md) for TLS,
> authentication, browser access, checksums, durable resume and sender cancellation.
> The [rollout review](docs/ROLLOUT.md) defines scope and limitations, and
> [releasing](docs/RELEASING.md) describes automatic publication.


## Product Overview

gosh-lan-transfer is a Rust library that enables peer-to-peer file transfers over local networks. It provides the core transfer engine without GUI dependencies, allowing integration into CLI tools, desktop applications, mobile apps, or embedded systems.

### Objectives

1. Enable direct file transfers between devices on the same network without cloud intermediaries
2. Provide a simple, event-driven API suitable for various UI frameworks
3. Support LAN, VPN, and Tailscale network topologies
4. Maintain cross-platform compatibility (Linux, macOS, Windows)

## Target Users

### Primary Users

1. **Application Developers** - Building file sharing features into their applications
2. **CLI Tool Authors** - Creating command-line file transfer utilities
3. **Desktop App Developers** - Integrating LAN sharing into productivity or utility applications

### Use Cases

1. **Home Network Sharing** - Transfer photos, documents, and media between personal devices
2. **Office File Distribution** - Share files across workstations without uploading to cloud services
3. **Remote Team Collaboration** - Transfer files over VPN or Tailscale between distributed team members
4. **Embedded/IoT** - Enable file uploads to headless devices on local networks

## Functional Requirements

### Core Transfer Capabilities

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-1 | Send one or more files to a peer by IP address or hostname | Must Have | ✓ Implemented |
| FR-2 | Receive files from peers with approval workflow | Must Have | ✓ Implemented |
| FR-3 | Auto-accept transfers from configured trusted hosts | Must Have | ✓ Implemented |
| FR-4 | Track transfer progress with byte-level granularity | Must Have | ✓ Implemented |
| FR-5 | Cancel in-progress transfers | Should Have | ✓ Implemented |
| FR-6 | Resume interrupted transfers | Could Have | Not implemented |
| FR-6a | Send entire directories with structure preserved | Should Have | ✓ Implemented |
| FR-6b | Batch accept/reject all pending transfers | Should Have | ✓ Implemented |
| FR-6c | Automatic retry with exponential backoff | Should Have | ✓ Implemented |

### Server Operations

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-7 | Start HTTP server on configurable port | Must Have | ✓ Implemented |
| FR-8 | Stop server gracefully | Must Have | ✓ Implemented |
| FR-9 | Query server running status | Must Have | ✓ Implemented |
| FR-10 | Handle multiple concurrent transfer sessions | Must Have | ✓ Implemented |

### Network Utilities

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-11 | Resolve hostnames to IP addresses | Must Have | ✓ Implemented |
| FR-12 | Enumerate local network interfaces | Must Have | ✓ Implemented |
| FR-13 | Check peer reachability (health check) | Must Have | ✓ Implemented |
| FR-14 | Retrieve peer device information | Should Have | ✓ Implemented |

### Configuration

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-15 | Configure server port | Must Have | ✓ Implemented |
| FR-16 | Configure device display name | Must Have | ✓ Implemented |
| FR-17 | Configure download directory | Must Have | ✓ Implemented |
| FR-18 | Manage trusted hosts list | Must Have | ✓ Implemented |
| FR-19 | Enable receive-only mode | Should Have | ✓ Implemented |
| FR-19a | Configure retry attempts and delay | Should Have | ✓ Implemented |
| FR-19b | Configure bandwidth limit | Could Have | Config added, throttling not implemented |

### Event System

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-20 | Emit events for transfer requests | Must Have | ✓ Implemented |
| FR-21 | Emit events for transfer progress | Must Have | ✓ Implemented |
| FR-22 | Emit events for transfer completion/failure | Must Have | ✓ Implemented |
| FR-23 | Support channel-based event delivery | Must Have | ✓ Implemented |
| FR-24 | Support callback-based event delivery | Must Have | ✓ Implemented |
| FR-25 | Support SSE streaming for web clients | Should Have | ✓ Implemented |
| FR-25a | Emit events for retry attempts | Should Have | ✓ Implemented |

### Persistence (Trait-based)

| ID | Requirement | Priority | Status |
|----|-------------|----------|--------|
| FR-26 | Define trait for favorites persistence | Must Have | ✓ Implemented |
| FR-27 | Provide in-memory favorites implementation | Must Have | ✓ Implemented |
| FR-28 | CRUD operations for saved peers | Must Have | ✓ Implemented |
| FR-29 | Define trait for transfer history persistence | Should Have | ✓ Implemented |
| FR-30 | Provide in-memory history implementation | Should Have | ✓ Implemented |
| FR-31 | Automatic history recording on transfer complete/fail | Should Have | ✓ Implemented |

## Non-Functional Requirements

### Performance

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1 | Transfer speed | Line rate (limited only by network) |
| NFR-2 | Memory usage during transfer | < 10MB overhead regardless of file size |
| NFR-3 | Startup time | < 100ms to server ready |
| NFR-4 | Progress update latency | < 100ms from byte receipt to event emission |

### Reliability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-5 | Handle network interruptions gracefully | Emit failure event, clean up partial files |
| NFR-6 | Validate received file integrity | Reject files exceeding declared size |
| NFR-7 | Prevent filename collisions | Auto-rename with `(n)` suffix |

### Security

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-8 | Prevent path traversal attacks | Sanitize all received filenames |
| NFR-9 | Authorize uploads with tokens | UUID v4 tokens per transfer session |
| NFR-10 | Require explicit approval for untrusted sources | Default deny unless trusted |

### Compatibility

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-11 | Platform support | Linux, macOS, Windows |
| NFR-12 | Rust edition | 2021 |
| NFR-13 | Minimum Rust version | Stable (no nightly features) |

### Usability (API)

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-14 | Single entry point for all operations | `GoshTransferEngine` struct |
| NFR-15 | Builder pattern for configuration | `EngineConfig::builder()` |
| NFR-16 | Sensible defaults | Works with `EngineConfig::default()` |
| NFR-17 | Comprehensive error types | `EngineError` enum with descriptive variants |

## Success Metrics

1. **API Ergonomics** - Developers can send their first file in < 20 lines of code
2. **Transfer Reliability** - < 0.1% transfer failure rate on stable networks
3. **Performance Parity** - Transfer speeds within 5% of raw TCP file copy

## Constraints

1. **No GUI Dependencies** - Library must compile without any windowing system
2. **No Cloud Services** - All transfers must be direct peer-to-peer
3. **Tokio Runtime** - Assumes Tokio async runtime (not async-std or others)
4. **HTTP Protocol** - Uses HTTP/1.1 for maximum compatibility

## Assumptions

1. Peers are on the same network or connected via VPN/Tailscale
2. Network allows TCP connections on configured port (default 53317)
3. Users have filesystem permissions for source/destination paths
4. Clocks are reasonably synchronized for timestamp fields

## Future Considerations

1. **Peer Discovery** - Optional mDNS/Bonjour alongside existing UDP multicast
2. **Configurable Retention** - Application-specific durable-session quotas and expiry policies
3. **Compression** - Optional on-the-fly compression for slow networks
4. **Credential Management** - Multi-user identities and certificate enrollment
5. **Adaptive Bandwidth** - Dynamic bandwidth allocation across simultaneous transfers
6. **Concurrent Transfer Queue** - Queue and run multiple sends in parallel
