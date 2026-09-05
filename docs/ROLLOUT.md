# 0.4.0 rollout review

## Fixed in this release

| Area | Previous failure | Change |
| --- | --- | --- |
| Approval | Reused IDs replaced approved metadata and rotated tokens | Serialize decisions; exact request retries return the same token; conflicts rejected |
| Authorization | Any peer could poll another transfer's token | Bind status-token disclosure and uploads to the request's source IP |
| Browser access | Wildcard CORS let websites interact with trusted local receivers | Remove wildcard CORS; reject transfer requests carrying Origin |
| File destinations | Unsafe fallback IDs, Windows names, existing directory symlinks | Sanitize names and fallbacks; reject unsafe receive directories |
| Metadata | Duplicate file IDs prevented completion; size sum could overflow | Validate IDs and use checked addition |
| Cancellation | Active or stalled uploads continued after cancellation | Interrupt the body stream; remove incomplete file; suppress completion |
| Retries | File uploads did not retry; repeated uploads created extra files | Retry transient failures, serialize transfer uploads and retain file receipts |
| Progress | Retried uploads could inflate counters | Roll back failed receive bytes and reset send attempt counters |
| Lifecycle | Port zero reported zero; immediate restart could race shutdown | Report actual bound port, await shutdown, explicitly enable dual stack |
| Configuration | Live config could report an unbound port; discovery advertised old port | Preserve live port in update_config; update discovery when binding changes |
| Directory send | File symlinks could send data outside the selected tree | Skip symlinks during directory enumeration |
| Packaging | Clean Linux builds required OpenSSL development packages | Use Rustls and disable automatic HTTP proxies/redirects for direct transfers |
| Release engineering | No CI, GitHub release or crates.io workflow | Three-platform CI, locked dependency resolution, validated automatic releases |

## Supported scope and limits

- This is a Rust engine library, not an end-user app or TUI.
- HTTP transport is unencrypted and IP trust is not cryptographic authentication.
  Use a trusted LAN or encrypted VPN. Do not expose the listener directly to the Internet.
- `/events` is a network-readable metadata stream for native clients. Put an
  authenticated backend between this engine and browser UIs. Browser writes
  with an Origin header are rejected, including same-origin writes.
- Receiver download directories must be owned by the application and not
  concurrently modified by another local process. Existing symlinks/junctions
  are rejected; this is not a sandbox against hostile local filesystem races.
- Retries resend a complete failed file, not byte-range resume. Progress may
  move backward at retry boundaries. Receipts survive for one idle hour while
  the engine exists; they do not survive process restart. At most 1,024 retained
  sessions are accepted. Idle sessions are pruned when a new request arrives.
- Cancellation affects receiving transfers. There is no public sender-side
  transfer handle/cancel API. Successfully received earlier files are retained.
- Empty directories and symlinks are not reproduced. Files are size-checked;
  there is no application-level checksum or cryptographic integrity check.
- Configuration changes to discovery sockets/timers require discovery restart.
  Use `change_port` for a running HTTP listener. `update_config` preserves its port.
- Multicast and interface enumeration can be blocked by containers, firewalls
  or VPNs. Direct IP/hostname transfer is the fallback. Test on the actual LAN.

## Follow-up improvements

Authenticated/encrypted transport, sender cancellation handles, durable resume,
checksums, configurable session/time limits, and an authenticated browser adapter
are separate feature work. None should be represented as implemented in 0.4.0.
