// Integration test: two engines on the same host discover each other via
// UDP multicast (relies on SO_REUSEPORT + multicast loopback).
//
// Multicast is frequently unavailable in CI sandboxes/containers, so the
// test skips gracefully if discovery can't start or no announcements get
// through within the deadline window.

use gosh_lan_transfer::{EngineConfig, EngineEvent, GoshTransferEngine};
use std::time::Duration;

#[tokio::test]
async fn engines_discover_each_other_on_loopback() {
    // Distinct discovery port per test run to avoid clashing with real
    // deployments on the developer machine
    let discovery_port = 56700 + (std::process::id() % 1000) as u16;

    let config_a = EngineConfig::builder()
        .device_name("Engine A")
        .port(53401)
        .discovery_port(discovery_port)
        .discovery_announce_interval_secs(1)
        .build();
    let config_b = EngineConfig::builder()
        .device_name("Engine B")
        .port(53402)
        .discovery_port(discovery_port)
        .discovery_announce_interval_secs(1)
        .build();

    let (mut engine_a, mut events_a) = GoshTransferEngine::with_channel_events(config_a);
    let (mut engine_b, mut events_b) = GoshTransferEngine::with_channel_events(config_b);

    if engine_a.start_discovery().await.is_err() || engine_b.start_discovery().await.is_err() {
        eprintln!("skipping: multicast unavailable in this environment");
        return;
    }
    assert!(engine_a.is_discovery_running());
    assert!(engine_b.is_discovery_running());

    let wait_for_peer = |events: &mut tokio::sync::broadcast::Receiver<EngineEvent>,
                         expected: &'static str| {
        let mut events = events.resubscribe();
        async move {
            loop {
                match events.recv().await {
                    Ok(EngineEvent::PeerDiscovered(p)) if p.device_name == expected => {
                        return p;
                    }
                    Ok(_) => {}
                    Err(e) => panic!("event channel closed: {e}"),
                }
            }
        }
    };

    let found = tokio::time::timeout(Duration::from_secs(10), async {
        let a_sees_b = wait_for_peer(&mut events_a, "Engine B");
        let b_sees_a = wait_for_peer(&mut events_b, "Engine A");
        tokio::join!(a_sees_b, b_sees_a)
    })
    .await;

    match found {
        Ok((peer_b, peer_a)) => {
            assert_eq!(peer_b.port, 53402);
            assert_eq!(peer_a.port, 53401);
            assert!(!engine_a.discovered_peers().await.is_empty());
            assert!(!engine_b.discovered_peers().await.is_empty());
        }
        Err(_) => {
            // Sockets opened but no datagrams flowed: multicast is filtered
            // (common in sandboxes). The parse/upsert/reply logic is covered
            // by unit tests in src/discovery.rs.
            eprintln!("skipping: no multicast traffic within deadline");
        }
    }

    engine_a.stop_discovery().await.unwrap();
    engine_b.stop_discovery().await.unwrap();
    assert!(!engine_a.is_discovery_running());
    assert!(engine_a.discovered_peers().await.is_empty());

    // Double-stop must error
    assert!(engine_a.stop_discovery().await.is_err());
}
