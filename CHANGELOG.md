# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-01-17

### Fixed

- Replace blocking filesystem operations with async in `send_directory()` to avoid blocking the Tokio runtime thread

## [0.2.0] - 2025-01-12

### Added

- **Runtime Port Change**
  - `change_port(new_port)` method to change server port while running
  - `change_port_with_options(new_port, rollback_on_failure)` for configurable rollback
  - `port()` getter for current server port
  - `PortChanged { old_port, new_port }` event emitted on port changes
  - Port validation: rejects port 0, warns on privileged ports (< 1024)
  - Automatic rollback to previous port if new port binding fails

- **Transfer History System**
  - `HistoryPersistence` trait for pluggable history storage
  - `InMemoryHistory` implementation with optional record limit
  - Automatic recording of completed and failed transfers
  - Methods: `list()`, `get()`, `add()`, `delete()`, `clear()`, `count()`, `list_paginated()`
  - Engine constructors: `with_history()`, `with_channel_events_and_history()`

- **Retry Logic with Exponential Backoff**
  - `max_retries` config option (default: 3)
  - `retry_delay_ms` config option (default: 1000ms)
  - Automatic retry for transient network errors
  - `TransferRetry` event emitted on each retry attempt

- **Batch Operations**
  - `accept_all_transfers()` - Accept all pending transfers at once
  - `reject_all_transfers()` - Reject all pending transfers at once
  - Returns per-transfer results for error handling

- **Directory Transfer Support**
  - `send_directory()` method for recursive directory transfers
  - `relative_path` field on `TransferFile` for preserving directory structure
  - Server automatically creates subdirectories when receiving
  - Path sanitization to prevent directory traversal attacks

- **Bandwidth Limiting Configuration**
  - `bandwidth_limit_bps` config option for rate limiting (implementation ready)

- Transfer cancellation via `cancel_transfer()` method
- Speed calculation (`speed_bps`) in `TransferProgress` events
- IPv6 dual-stack support with automatic fallback to IPv4
- `resolve_address_or_err()` method for DNS resolution with error handling
- `TransferCancelled` error variant

### Changed

- `accept_transfer()` and `reject_transfer()` now return `ServerNotRunning` error if server is not running
- Server now binds to `[::]` (IPv6) first, falling back to `0.0.0.0` (IPv4) if unavailable
- Progress events now include actual transfer speed
- `TransferClient` now accepts configuration for retry settings
- Engine `update_config()` now updates client retry settings

### Removed

- `InvalidToken` error variant (HTTP 401 responses used instead)

## [0.1.0] - 2024-01-01

### Added

- Initial release of gosh-lan-transfer library
- `GoshTransferEngine` as the main entry point for all operations
- HTTP server for receiving file transfers with endpoints:
  - `/health` - Health check
  - `/info` - Device information
  - `/transfer` - Transfer request initiation
  - `/transfer/status` - Approval status polling
  - `/chunk` - File data upload
  - `/events` - SSE stream for real-time events
- HTTP client for sending files with:
  - DNS resolution with multi-IP fallback
  - Transfer request/approval workflow
  - Streaming uploads with progress tracking
- Event system with three handler implementations:
  - `ChannelEventHandler` - Tokio broadcast channel for async apps
  - `CallbackEventHandler` - Closure-based for simple use cases
  - `NoopEventHandler` - Discards events for testing
- `EngineConfig` with builder pattern for configuration
- Trust-based auto-acceptance for transfers from trusted hosts
- `FavoritesPersistence` trait with `InMemoryFavorites` implementation
- Network utilities for interface enumeration and peer discovery
- Comprehensive error types via `EngineError` enum
