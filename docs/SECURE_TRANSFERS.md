# Secure, resumable transfers (0.5)

`GoshTransferEngine::send_files` and `send_directory` now use protocol v2:
SHA-256 manifests, durable receive offsets and verified publication. Both peers
must run 0.5 or newer. There is no silent fallback. For older peers explicitly
use `send_files_legacy`; the lower-level `TransferClient::send_files` and
`send_directory` retain protocol v1 for compatibility.

## TLS and pairing

Transport remains HTTP by default for existing private-LAN deployments. Configure
`SecurityConfig` for HTTPS. The receiving certificate must have a subjectAltName
matching the hostname/IP used by senders. Supply its PEM certificate chain and
matching unencrypted PEM private key, and protect the key with filesystem access
controls. Use a CA-issued certificate or install a private CA/certificate in each
client's trust configuration through a trusted channel.

```rust,no_run
use gosh_lan_transfer::{EngineConfig, GoshTransferEngine, SecurityConfig, TlsIdentity};
use std::path::PathBuf;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let token = SecurityConfig::generate_token(); // Store securely; share out of band.
let receiver_config = EngineConfig::builder()
    .download_dir("downloads")
    .security(SecurityConfig {
        identity: Some(TlsIdentity {
            certificate: PathBuf::from("receiver-chain.pem"),
            private_key: PathBuf::from("receiver-key.pem"),
        }),
        auth_token: Some(token.clone()),
        allowed_origins: vec!["https://transfer.example.com".into()],
        ..Default::default()
    })
    .build();
let mut receiver = GoshTransferEngine::new(receiver_config, gosh_lan_transfer::noop_handler());
receiver.start_server().await?;

let sender = GoshTransferEngine::new(EngineConfig::builder()
    .security(SecurityConfig {
        https: true,
        trusted_certificates: vec![PathBuf::from("receiver-ca.pem")],
        peer_token: Some(token),
        ..Default::default()
    })
    .build(), gosh_lan_transfer::noop_handler());
// The receiver application still approves through accept_transfer(id).
sender.send_files("receiver.example.com", 53317, vec!["report.pdf".into()]).await?;
# Ok(()) }
```

Certificate chain and hostname checks are always enabled. HTTPS never downgrades
to HTTP; redirects and proxy environment variables are ignored. Bearer tokens
must be at least 32 bytes and cannot be configured over plaintext. A configured
incoming token protects all endpoints, including health, info and events.
Authentication does not grant automatic transfer approval: approval remains a
local API decision unless the source IP is in `trusted_hosts`.

The bearer token identifies a trusted peer/group; everyone holding the same
secret has the same identity and can read its transfer status and cancel its
transfers. This is not a multi-user account system or mutual TLS. Configure a
separate engine/client for peers with different credentials. Resume is bound to
the token identity in authenticated mode, and to the source IP in plaintext
mode. Rotating a token invalidates access to old sessions; cancel/forget them
through the receiver API as appropriate. Never log tokens or put them in URLs.

Port, download directory and TLS identity are preserved by live `update_config`.
To change storage or TLS identity, stop the listener, update configuration, then
start it again. Live incoming token/origin
changes apply to subsequent requests; already established event streams must be
closed by restarting the server when revoking access. Discovery announcements
remain unauthenticated and do not distribute certificates or credentials. Set
outgoing `https` explicitly using your paired-peer configuration.

## Saving, resuming and cancelling

```rust,no_run
use gosh_lan_transfer::{EngineConfig, GoshTransferEngine, PreparedTransfer};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let engine = GoshTransferEngine::new(EngineConfig::default(), gosh_lan_transfer::noop_handler());
let plan = engine.prepare_files("192.168.1.50", 53317, vec!["video.mp4".into()]).await?;
plan.save("video-transfer.json").await?;
let handle = engine.start_prepared(plan);
// In response to a user's Cancel action: handle.cancel();
handle.wait().await?;

// After a network failure or process restart, reuse the saved IDs:
let saved = PreparedTransfer::load("video-transfer.json").await?;
engine.send_prepared(saved).await?;
# Ok(()) }
```

Preparing hashes every source file. Sending rechecks source sizes and hashes;
changed sources are rejected. This adds disk reads but catches changed sources
before resuming. Keep sources unchanged until the transfer completes. The
receiver independently hashes the complete file before exposing its final name.
A checksum mismatch discards the partial file and returns an error; retrying the
same saved plan starts that file from zero.

The receiver stores journals and partial files under `download_dir/.gosh-transfer`.
Journals use atomic replacement and a receiver lock prevents two engines from
owning the same state directory. Disconnected uploads retain their received
prefix; the next attempt fetches the actual offset and sends only missing bytes.
Process restart reloads approval, cancellation and completion receipts. A retry
after completion does not create another destination file. The state directory
is reserved and excluded from directory sends.

Save the prepared plan before starting if you need sender restart recovery.
Calling `send_files` again creates new IDs and a new transfer. Do not edit a saved
plan or delete receiver state while it is active. The plan contains local source
paths, not bearer credentials. Store it in an application-owned directory. Unix
plans/journals are owner-only; on Windows use an appropriately restricted ACL on
the application directory.

`TransferHandle::cancel()` interrupts approval waits and active outgoing uploads.
Dropping an unfinished handle also cancels. Cancellation attempts to notify the
receiver (bounded to three seconds); if unreachable, the receiver retains state
until it is explicitly cancelled there. Cancellation is terminal for that ID;
it does not mean pause. Network interruption or process shutdown retains a plan
for resume. Receiver `cancel_transfer(id)` removes incomplete data. Files already
published stay in place, and cancellation cannot recall a completed transfer.

There are at most 1,024 retained v2 sessions. They do not expire automatically.
After the sender has acknowledged completion, call
`forget_received_transfer(id)` to remove terminal receipts and free capacity;
delivered files remain. Cancel active sessions before forgetting them. Forgetting
an ID removes duplicate-retry protection for that ID. Approval waits time out
after 120 seconds; resume the saved plan after approval to continue.

Publication uses a hard link within the download filesystem so existing files
are never overwritten. The filesystem must support hard links (for example
NTFS, ext4 or APFS; FAT/exFAT are not supported for v2 receive). Existing different
content gets a numbered destination. Matching content can reuse an existing
file. Journals and files are synced; crash consistency ultimately depends on
the filesystem/storage's durability guarantees. Download directories must not
be concurrently changed by an untrusted local process. Empty directories and
symlinks are not reproduced.

## Authenticated browser API

Allow exact origins in `allowed_origins` on an authenticated TLS listener. No
wildcards or trailing slash. The browser must trust the receiver's certificate;
the engine cannot bypass browser certificate checks. A hosted UI may also require
browser/OS local-network permission. Unknown origins are rejected on every
endpoint. Approved origins receive preflight responses, but all data requests
still require a bearer token. Do not embed a pairing secret in public JavaScript;
obtain it from the user or your application's secure credential flow.

```javascript
const headers = { Authorization: `Bearer ${pairingToken}` };
const response = await fetch(`${peerHttpsUrl}/info`, { headers });
if (!response.ok) throw new Error(`Peer returned ${response.status}`);
const info = await response.json();
// SSE uses fetch with headers and an SSE parser over response.body.
// Native EventSource cannot attach the required Authorization header.
```

The same authenticated endpoints support a browser sender:

| Endpoint | Method | Body / parameters | Result |
| --- | --- | --- | --- |
| `/v2/transfer` | POST | JSON `TransferManifest` (`request` plus `sha256` map) | `ResumeStatus` |
| `/v2/status` | GET | `transfer_id` query | Approval, upload token, per-file offsets and completion |
| `/v2/chunk` | POST | `transfer_id`, `file_id`, `offset` query; `X-Transfer-Token` header; remaining file bytes | 200 after verified publication |
| `/v2/cancel` | POST | `transfer_id` query | Cancel the caller's transfer |
| `/events` | GET | Authorization header | SSE events |

`PreparedTransfer::manifest()` produces the registration envelope without local
paths. Wire fields use camelCase (`transferId`, `fileId`, `totalSize`); IDs must be
canonical lowercase hyphenated UUIDs. SHA-256 values are lowercase hex keyed by
file ID. Browser clients compute SHA-256, persist the manifest/IDs, poll approval,
then send `file.slice(offset)` with the approved upload token. A 409 requires a
fresh status fetch; 410 means cancellation; 422 means checksum mismatch and the
partial was discarded. A short request body retains its prefix and returns 400;
transient network errors, 408 and 5xx can be retried after fetching status.

Approval, rejection and forgetting receipts stay local Rust API operations;
there is no remote administrative approval endpoint. Native Rust applications
can keep using channel events and the existing approval APIs.
