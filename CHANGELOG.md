# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Transfer cancellation via `cancel_transfer()` method
- Speed calculation (`speed_bps`) in `TransferProgress` events
- IPv6 dual-stack support with automatic fallback to IPv4
- `resolve_address_or_err()` method for DNS resolution with error handling
- `TransferCancelled` error variant

### Changed

- `accept_transfer()` and `reject_transfer()` now return `ServerNotRunning` error if server is not running
- Server now binds to `[::]` (IPv6) first, falling back to `0.0.0.0` (IPv4) if unavailable
- Progress events now include actual transfer speed

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
