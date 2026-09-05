# 0.5.0 rollout review

## Implemented and covered by automated tests

| Area | Behavior |
| --- | --- |
| Transport | Optional Rustls HTTPS, verified certificate chain and hostname, no downgrade |
| Pairing | Bearer authentication on every endpoint; no plaintext token configuration |
| Browser | Exact authenticated HTTPS origins, preflight and fetch/SSE access |
| Integrity | SHA-256 manifests and verification before final publication |
| Resume | Persistent approval, byte offsets, cancellation and completion across restart |
| Cancellation | Outgoing handle interrupts approval and active upload; receiver removes partials |
| Publication | No overwrite, separate per-session partial files, crash receipt recovery |
| State | Exclusive journal ownership, reserved metadata namespace, terminal-session cleanup |
| Compatibility | Explicit v1 send path for pre-0.5 peers; existing receive endpoints retained |
| Release | Linux/macOS/Windows CI, GitHub release, crates.io OIDC publication and docs.rs |

The 0.4 approval, path handling, progress, retry, lifecycle and configuration
regressions remain in the test suite. See the changelog for that release's fixes.

## Deployment requirements and limits

- This is a Rust library, not a standalone app, CLI or graphical interface.
- Configure HTTPS, certificate trust and pairing before using untrusted networks.
  HTTP remains the default for private-LAN compatibility. Discovery is not authenticated.
- Default engine sends require a 0.5+ receiver. Legacy methods retain v1 behavior
  without persistent resume/checksums. Use separate client configurations for
  peers with different credentials or transport modes.
- Save prepared plans before sending for sender restart recovery. Keep source
  files unchanged. Checksums add full-file disk reads before sending/publication.
- V2 receive requires a hard-link-capable filesystem. FAT/exFAT are unsupported.
  Application-owned directories and appropriate ACLs are required. This is not
  a sandbox against hostile local filesystem races.
- Retained v2 sessions are capped at 1,024 and require explicit terminal cleanup.
  Partial files are retained after interruptions; cancellation deletes partials.
  Completed files remain. Empty directories and symlinks are not reproduced.
- Bearer authentication represents one shared trusted identity, not user accounts.
  TLS identity changes and revocation of existing streams require listener restart.
- Discovery socket/timer changes require restart; use `change_port` to rebind.
  Stop, update configuration and start to change download directory or TLS identity.
- Browser certificates must be trusted by the browser and local-network access
  may need browser/OS permission. The engine supports the authenticated API;
  your application supplies credential entry, approval UI and the SSE parser.

## Rollout checks on your actual devices

CI exercises local loopback, TLS, file I/O and recovery on three operating systems.
It does not establish multicast reachability through a real router/firewall/VPN.
Before broad distribution, verify discovery/direct addressing and a large transfer
between two target devices, interrupt/restart both applications, check their saved
plan recovery, and exercise the browser UI with its production certificate/origin.
See [secure transfers](SECURE_TRANSFERS.md) for the deployment/API contract.
