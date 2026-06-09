// SPDX-License-Identifier: MIT
// gosh-lan-transfer - UDP multicast peer discovery

//! UDP multicast peer discovery.
//!
//! Devices periodically announce themselves as small JSON datagrams on a
//! multicast group (default `224.0.0.167:53318`). Listeners maintain a peer
//! table keyed by fingerprint, reply unicast so the announcer also learns
//! about them, and expire peers that stop announcing.
//!
//! Announcements are unauthenticated and therefore advisory only: discovery
//! feeds a peer list, it never authorizes transfers. Incoming transfers still
//! require explicit approval (or a trusted-host entry).

use crate::config::EngineConfig;
use crate::error::{EngineError, EngineResult};
use crate::events::EventHandler;
use crate::protocol::{DiscoveredPeer, DiscoveryAnnouncement, EngineEvent};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{oneshot, RwLock};

/// Application identifier expected in announcements
const APP_ID: &str = "gosh-lan-transfer";

/// Maximum accepted device name length in characters
const MAX_DEVICE_NAME_LEN: usize = 64;

/// Receive buffer size; announcements are well under this
const RECV_BUF_SIZE: usize = 2048;

/// Handle for controlling a running discovery session
pub struct DiscoveryHandle {
    shutdown_tx: oneshot::Sender<()>,
}

impl DiscoveryHandle {
    /// Signal the discovery task to shut down
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Shared state for the discovery task
pub struct DiscoveryState {
    /// Discovered peers keyed by fingerprint
    peers: RwLock<HashMap<String, DiscoveredPeer>>,
    /// Our own stable per-engine-instance identifier
    fingerprint: String,
    /// Engine configuration (device name, ports, intervals)
    config: RwLock<EngineConfig>,
    /// Event handler for PeerDiscovered / PeerLost
    event_handler: Arc<dyn EventHandler>,
}

impl DiscoveryState {
    /// Create discovery state with a freshly generated fingerprint
    pub fn new(config: EngineConfig, event_handler: Arc<dyn EventHandler>) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            fingerprint: uuid::Uuid::new_v4().to_string(),
            config: RwLock::new(config),
            event_handler,
        }
    }

    /// Our stable per-engine-instance identifier
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Update the configuration. Changes to multicast group, discovery port,
    /// or intervals take effect the next time discovery is started.
    pub async fn update_config(&self, config: EngineConfig) {
        *self.config.write().await = config;
    }

    /// Snapshot of currently known peers
    pub async fn peers(&self) -> Vec<DiscoveredPeer> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Clear the peer table (used when discovery stops)
    pub(crate) async fn clear_peers(&self) {
        self.peers.write().await.clear();
    }

    /// Build our announcement payload
    async fn our_announcement(&self, announce: bool) -> DiscoveryAnnouncement {
        let config = self.config.read().await;
        DiscoveryAnnouncement {
            app: APP_ID.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            fingerprint: self.fingerprint.clone(),
            device_name: config.device_name.clone(),
            port: config.port,
            announce,
        }
    }

    /// Process an incoming datagram. Returns a serialized unicast reply if
    /// the sender should learn about us (i.e. the packet was a multicast
    /// announcement, not already a reply).
    async fn handle_packet(&self, data: &[u8], src: SocketAddr) -> Option<Vec<u8>> {
        let msg: DiscoveryAnnouncement = serde_json::from_slice(data).ok()?;

        if msg.app != APP_ID || msg.fingerprint == self.fingerprint || msg.port == 0 {
            return None;
        }

        let peer = DiscoveredPeer {
            fingerprint: msg.fingerprint.clone(),
            device_name: sanitize_device_name(&msg.device_name),
            address: src.ip().to_string(),
            port: msg.port,
            version: msg.version.clone(),
            last_seen: chrono::Utc::now(),
        };

        let is_new = {
            let mut peers = self.peers.write().await;
            peers.insert(msg.fingerprint.clone(), peer.clone()).is_none()
        };

        if is_new {
            tracing::info!(
                "Discovered peer: {} at {}:{}",
                peer.device_name,
                peer.address,
                peer.port
            );
            self.event_handler
                .on_event(EngineEvent::PeerDiscovered(peer));
        }

        if msg.announce {
            let reply = self.our_announcement(false).await;
            serde_json::to_vec(&reply).ok()
        } else {
            None
        }
    }

    /// Remove peers not heard from within `timeout`, emitting PeerLost
    async fn expire_stale_peers(&self, timeout: Duration) {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::seconds(15));

        let lost: Vec<DiscoveredPeer> = {
            let mut peers = self.peers.write().await;
            let stale: Vec<String> = peers
                .iter()
                .filter(|(_, p)| p.last_seen < cutoff)
                .map(|(k, _)| k.clone())
                .collect();
            stale.iter().filter_map(|k| peers.remove(k)).collect()
        };

        for peer in lost {
            tracing::info!("Lost peer: {} ({})", peer.device_name, peer.address);
            self.event_handler.on_event(EngineEvent::PeerLost {
                fingerprint: peer.fingerprint,
                device_name: peer.device_name,
            });
        }
    }
}

/// Strip control characters, trim, and truncate a peer-supplied device name
fn sanitize_device_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_DEVICE_NAME_LEN)
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        "Unknown Device".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Create a UDP socket bound to the discovery port and joined to the
/// multicast group. SO_REUSEADDR/SO_REUSEPORT allow multiple engines on one
/// host (and are required on macOS for shared binds); multicast loopback is
/// enabled so same-host peers can discover each other.
fn create_multicast_socket(group: Ipv4Addr, port: u16) -> std::io::Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)).into())?;
    socket.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)?;
    socket.set_multicast_loop_v4(true)?;

    UdpSocket::from_std(socket.into())
}

/// Start the discovery task: announce periodically, listen for peers,
/// reply unicast to announcers, and expire stale peers.
pub(crate) async fn start_discovery(state: Arc<DiscoveryState>) -> EngineResult<DiscoveryHandle> {
    let (group, port, announce_interval, peer_timeout) = {
        let config = state.config.read().await;
        (
            config.discovery_multicast_addr,
            config.discovery_port,
            Duration::from_secs(config.discovery_announce_interval_secs.max(1)),
            Duration::from_secs(config.discovery_peer_timeout_secs.max(1)),
        )
    };

    let socket = create_multicast_socket(group, port).map_err(|e| {
        EngineError::Network(format!(
            "Failed to join multicast group {}:{}: {}",
            group, port, e
        ))
    })?;

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut announce_timer = tokio::time::interval(announce_interval);
        // Check for stale peers a few times per timeout window
        let mut expiry_timer = tokio::time::interval(peer_timeout / 3);
        let mut buf = [0u8; RECV_BUF_SIZE];

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    tracing::info!("Discovery shutting down");
                    break;
                }
                _ = announce_timer.tick() => {
                    let announcement = state.our_announcement(true).await;
                    if let Ok(payload) = serde_json::to_vec(&announcement) {
                        if let Err(e) = socket.send_to(&payload, (group, port)).await {
                            tracing::warn!("Failed to send discovery announcement: {}", e);
                        }
                    }
                }
                _ = expiry_timer.tick() => {
                    state.expire_stale_peers(peer_timeout).await;
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src)) => {
                            if let Some(reply) = state.handle_packet(&buf[..len], src).await {
                                let _ = socket.send_to(&reply, src).await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Discovery receive error: {}", e);
                        }
                    }
                }
            }
        }
    });

    Ok(DiscoveryHandle { shutdown_tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{channel_handler, noop_handler};

    fn test_state() -> Arc<DiscoveryState> {
        let config = EngineConfig::builder().device_name("Test Device").build();
        Arc::new(DiscoveryState::new(config, noop_handler()))
    }

    fn announcement_from(fingerprint: &str, name: &str, port: u16, announce: bool) -> Vec<u8> {
        serde_json::to_vec(&DiscoveryAnnouncement {
            app: APP_ID.to_string(),
            version: "0.3.0".to_string(),
            fingerprint: fingerprint.to_string(),
            device_name: name.to_string(),
            port,
            announce,
        })
        .unwrap()
    }

    fn src() -> SocketAddr {
        "192.168.1.50:53318".parse().unwrap()
    }

    #[test]
    fn test_announcement_serde_roundtrip() {
        let original = DiscoveryAnnouncement {
            app: APP_ID.to_string(),
            version: "0.3.0".to_string(),
            fingerprint: "abc-123".to_string(),
            device_name: "My Device".to_string(),
            port: 53317,
            announce: true,
        };
        let json = serde_json::to_string(&original).unwrap();
        // Wire format uses camelCase
        assert!(json.contains("\"deviceName\""));
        assert!(json.contains("\"fingerprint\""));
        let parsed: DiscoveryAnnouncement = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.fingerprint, original.fingerprint);
        assert_eq!(parsed.device_name, original.device_name);
        assert_eq!(parsed.port, original.port);
    }

    #[tokio::test]
    async fn test_handle_packet_adds_peer_and_replies() {
        let (handler, mut events) = channel_handler(16);
        let config = EngineConfig::builder().device_name("Receiver").build();
        let state = Arc::new(DiscoveryState::new(config, handler));

        let packet = announcement_from("peer-1", "Sender", 53317, true);
        let reply = state.handle_packet(&packet, src()).await;

        // Announcement should produce a unicast reply with announce=false
        let reply: DiscoveryAnnouncement = serde_json::from_slice(&reply.unwrap()).unwrap();
        assert!(!reply.announce);
        assert_eq!(reply.fingerprint, state.fingerprint());

        let peers = state.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].device_name, "Sender");
        assert_eq!(peers[0].address, "192.168.1.50");
        assert_eq!(peers[0].port, 53317);

        match events.try_recv().unwrap() {
            EngineEvent::PeerDiscovered(p) => assert_eq!(p.fingerprint, "peer-1"),
            other => panic!("expected PeerDiscovered, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_handle_packet_reply_does_not_reply_again() {
        let state = test_state();
        let packet = announcement_from("peer-1", "Sender", 53317, false);
        // announce=false (a reply) must not trigger another reply
        assert!(state.handle_packet(&packet, src()).await.is_none());
        // ...but the peer is still recorded
        assert_eq!(state.peers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_handle_packet_ignores_self() {
        let state = test_state();
        let fingerprint = state.fingerprint().to_string();
        let packet = announcement_from(&fingerprint, "Me", 53317, true);
        assert!(state.handle_packet(&packet, src()).await.is_none());
        assert!(state.peers().await.is_empty());
    }

    #[tokio::test]
    async fn test_handle_packet_ignores_wrong_app_and_junk() {
        let state = test_state();

        let mut wrong_app: serde_json::Value =
            serde_json::from_slice(&announcement_from("p", "X", 1, true)).unwrap();
        wrong_app["app"] = "other-app".into();
        let wrong_app = serde_json::to_vec(&wrong_app).unwrap();

        assert!(state.handle_packet(&wrong_app, src()).await.is_none());
        assert!(state.handle_packet(b"not json at all", src()).await.is_none());
        assert!(state.handle_packet(&[], src()).await.is_none());
        assert!(state.peers().await.is_empty());
    }

    #[tokio::test]
    async fn test_handle_packet_rejects_port_zero() {
        let state = test_state();
        let packet = announcement_from("peer-1", "Sender", 0, true);
        assert!(state.handle_packet(&packet, src()).await.is_none());
        assert!(state.peers().await.is_empty());
    }

    #[tokio::test]
    async fn test_hostile_device_name_sanitized() {
        let state = test_state();
        let hostile = format!("\x1b[2J\x07{}", "A".repeat(500));
        let packet = announcement_from("peer-1", &hostile, 53317, true);
        state.handle_packet(&packet, src()).await;

        let peers = state.peers().await;
        assert_eq!(peers[0].device_name.chars().count(), MAX_DEVICE_NAME_LEN);
        assert!(peers[0].device_name.chars().all(|c| !c.is_control()));
    }

    #[test]
    fn test_sanitize_empty_and_control_only_names() {
        assert_eq!(sanitize_device_name(""), "Unknown Device");
        assert_eq!(sanitize_device_name("\x00\x1b\x07"), "Unknown Device");
        assert_eq!(sanitize_device_name("   "), "Unknown Device");
        assert_eq!(sanitize_device_name("  ok  "), "ok");
    }

    #[tokio::test]
    async fn test_expire_stale_peers() {
        let (handler, mut events) = channel_handler(16);
        let config = EngineConfig::default();
        let state = Arc::new(DiscoveryState::new(config, handler));

        let packet = announcement_from("peer-1", "Sender", 53317, false);
        state.handle_packet(&packet, src()).await;
        let _ = events.try_recv(); // drain PeerDiscovered

        // Backdate the peer beyond the timeout
        {
            let mut peers = state.peers.write().await;
            peers.get_mut("peer-1").unwrap().last_seen =
                chrono::Utc::now() - chrono::Duration::seconds(60);
        }

        state.expire_stale_peers(Duration::from_secs(15)).await;
        assert!(state.peers().await.is_empty());

        match events.try_recv().unwrap() {
            EngineEvent::PeerLost { fingerprint, .. } => assert_eq!(fingerprint, "peer-1"),
            other => panic!("expected PeerLost, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_fresh_peer_not_expired() {
        let state = test_state();
        let packet = announcement_from("peer-1", "Sender", 53317, false);
        state.handle_packet(&packet, src()).await;

        state.expire_stale_peers(Duration::from_secs(15)).await;
        assert_eq!(state.peers().await.len(), 1);
    }
}
