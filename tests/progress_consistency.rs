// Integration test: receiver-side progress events must report transfer-wide
// totals (matching the sender's semantics), not per-file totals.

use gosh_lan_transfer::{EngineConfig, EngineEvent, GoshTransferEngine};
use std::io::Write;
use std::time::Duration;

async fn find_available_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn receiver_progress_is_transfer_wide_and_monotonic() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();

    // Receiver: trusts localhost so the transfer auto-accepts
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();

    // Two files of different sizes so per-file vs transfer-wide totals differ
    let src_dir = tempfile::tempdir().unwrap();
    let file_a = src_dir.path().join("a.bin");
    let file_b = src_dir.path().join("b.bin");
    std::fs::File::create(&file_a)
        .unwrap()
        .write_all(&vec![0xAA; 100_000])
        .unwrap();
    std::fs::File::create(&file_b)
        .unwrap()
        .write_all(&vec![0xBB; 50_000])
        .unwrap();
    let total_size: u64 = 150_000;

    let sender_config = EngineConfig::builder().device_name("Sender").build();
    let sender = GoshTransferEngine::new(sender_config, gosh_lan_transfer::noop_handler());

    sender
        .send_files("127.0.0.1", port, vec![file_a, file_b])
        .await
        .unwrap();

    // Collect receiver-side events until TransferComplete
    let mut progress_events = Vec::new();
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            event = events.recv() => match event.unwrap() {
                EngineEvent::TransferProgress(p) => progress_events.push(p),
                EngineEvent::TransferComplete { .. } => break,
                EngineEvent::TransferFailed { error, .. } => panic!("transfer failed: {error}"),
                _ => {}
            },
            _ = &mut deadline => panic!("timed out waiting for TransferComplete"),
        }
    }

    assert!(
        !progress_events.is_empty(),
        "expected at least one progress event"
    );

    // Every event reports the whole transfer's size, not a single file's
    for p in &progress_events {
        assert_eq!(
            p.total_bytes, total_size,
            "receiver progress must use transfer-wide total_bytes"
        );
        assert!(p.bytes_transferred <= total_size);
    }

    // Cumulative bytes never go backwards across file boundaries
    let mut last = 0;
    for p in &progress_events {
        assert!(
            p.bytes_transferred >= last,
            "bytes_transferred regressed: {} -> {}",
            last,
            p.bytes_transferred
        );
        last = p.bytes_transferred;
    }
    assert_eq!(last, total_size, "final progress should reach total size");

    receiver.stop_server().await.unwrap();
}
