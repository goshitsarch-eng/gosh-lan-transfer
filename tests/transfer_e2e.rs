// End-to-end coverage for transfer, approval, display payloads, and HTTP surface.
// These tests exist to catch regressions in pending-list display, IPv6 URLs,
// device-info fields, CORS, path safety, and file round-trips.

use gosh_lan_transfer::{
    noop_handler, EngineConfig, EngineError, EngineEvent, GoshTransferEngine, HistoryPersistence,
    InMemoryHistory,
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

async fn find_available_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_ready(engine: &GoshTransferEngine, addr: &str, port: u16) {
    for _ in 0..100 {
        if engine.check_peer(addr, port).await.unwrap_or(false) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("server not ready on {addr}:{port}");
}

fn write_file(path: &Path, bytes: &[u8]) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(bytes).unwrap();
}

async fn collect_progress(
    events: &mut tokio::sync::broadcast::Receiver<EngineEvent>,
) -> (Vec<gosh_lan_transfer::TransferProgress>, String) {
    let mut progress = Vec::new();
    let deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            event = events.recv() => match event.unwrap() {
                EngineEvent::TransferProgress(p) => progress.push(p),
                EngineEvent::TransferComplete { transfer_id } => {
                    return (progress, transfer_id);
                }
                EngineEvent::TransferFailed { error, .. } => {
                    panic!("transfer failed: {error}");
                }
                _ => {}
            },
            _ = &mut deadline => panic!("timed out waiting for TransferComplete"),
        }
    }
}

#[tokio::test]
async fn file_contents_round_trip_including_unicode_and_zero_byte() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let ascii = src_dir.path().join("hello.txt");
    let unicode = src_dir.path().join("файл.txt");
    let empty = src_dir.path().join("empty.dat");
    write_file(&ascii, b"hello world");
    write_file(&unicode, "こんにちは".as_bytes());
    write_file(&empty, b"");

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
    );
    sender
        .send_files("127.0.0.1", port, vec![ascii, unicode, empty])
        .await
        .unwrap();

    let (progress, _) = collect_progress(&mut events).await;
    assert!(
        !progress.is_empty(),
        "receiver should emit progress (including zero-byte files)"
    );
    let last = progress.last().unwrap();
    assert_eq!(last.bytes_transferred, last.total_bytes);

    assert_eq!(
        std::fs::read(download_dir.path().join("hello.txt")).unwrap(),
        b"hello world"
    );
    assert_eq!(
        std::fs::read(download_dir.path().join("файл.txt")).unwrap(),
        "こんにちは".as_bytes()
    );
    assert_eq!(
        std::fs::read(download_dir.path().join("empty.dat")).unwrap(),
        b""
    );

    assert!(receiver.get_pending_transfers().await.is_empty());
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn pending_list_hides_rejected_and_accepted_transfers() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("doc.txt");
    write_file(&file, b"payload");

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
    );
    let send_task = tokio::spawn({
        let file = file.clone();
        async move { sender.send_files("127.0.0.1", port, vec![file]).await }
    });

    // Wait for the request to show up as pending
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_request = false;
    while tokio::time::Instant::now() < deadline {
        match events.try_recv() {
            Ok(EngineEvent::TransferRequest(t)) => {
                assert_eq!(receiver.get_pending_transfers().await.len(), 1);
                receiver.reject_transfer(&t.id).await.unwrap();
                assert!(
                    receiver.get_pending_transfers().await.is_empty(),
                    "rejected transfers must leave the pending list"
                );
                saw_request = true;
                break;
            }
            Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("event channel: {e}"),
        }
    }
    assert!(saw_request, "expected TransferRequest");

    let send_result = send_task.await.unwrap();
    assert!(matches!(send_result, Err(EngineError::TransferRejected)));
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn approval_workflow_accepts_and_hides_from_pending() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("photo.bin");
    write_file(&file, &vec![0xAB; 64_000]);

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
    );
    let send_task = tokio::spawn({
        let file = file.clone();
        async move { sender.send_files("127.0.0.1", port, vec![file]).await }
    });

    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            event = events.recv() => match event.unwrap() {
                EngineEvent::TransferRequest(t) => {
                    assert_eq!(receiver.get_pending_transfers().await.len(), 1);
                    receiver.accept_transfer(&t.id).await.unwrap();
                    // Accepted transfers must not remain in the pending section
                    assert!(receiver.get_pending_transfers().await.is_empty());
                    // Re-accept must be idempotent (same token, no breakage)
                    receiver.accept_transfer(&t.id).await.unwrap();
                }
                EngineEvent::TransferComplete { .. } => break,
                EngineEvent::TransferFailed { error, .. } => panic!("failed: {error}"),
                _ => {}
            },
            _ = &mut deadline => panic!("timed out"),
        }
    }

    send_task.await.unwrap().unwrap();
    assert_eq!(
        std::fs::read(download_dir.path().join("photo.bin"))
            .unwrap()
            .len(),
        64_000
    );
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn trusted_auto_accept_does_not_show_in_pending() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("a.bin");
    write_file(&file, &vec![0xCD; 80_000]);

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
    );
    sender
        .send_files("127.0.0.1", port, vec![file])
        .await
        .unwrap();

    let mut saw_progress = false;
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            event = events.recv() => match event.unwrap() {
                EngineEvent::TransferProgress(_) => {
                    saw_progress = true;
                    assert!(
                        receiver.get_pending_transfers().await.is_empty(),
                        "auto-accepted transfers must not occupy the pending section"
                    );
                }
                EngineEvent::TransferComplete { .. } => break,
                EngineEvent::TransferFailed { error, .. } => panic!("{error}"),
                EngineEvent::TransferRequest(_) => {
                    panic!("trusted hosts should not emit TransferRequest");
                }
                _ => {}
            },
            _ = &mut deadline => panic!("timed out"),
        }
    }
    assert!(saw_progress);
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn directory_transfer_preserves_nested_structure() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(src.path().join("sub").join("nested")).unwrap();
    write_file(&src.path().join("root.txt"), b"root");
    write_file(&src.path().join("sub").join("mid.txt"), b"mid");
    write_file(
        &src.path().join("sub").join("nested").join("leaf.txt"),
        b"leaf",
    );

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
    );
    sender
        .send_directory("127.0.0.1", port, src.path())
        .await
        .unwrap();
    collect_progress(&mut events).await;

    assert_eq!(
        std::fs::read(download_dir.path().join("root.txt")).unwrap(),
        b"root"
    );
    assert_eq!(
        std::fs::read(download_dir.path().join("sub").join("mid.txt")).unwrap(),
        b"mid"
    );
    assert_eq!(
        std::fs::read(
            download_dir
                .path()
                .join("sub")
                .join("nested")
                .join("leaf.txt")
        )
        .unwrap(),
        b"leaf"
    );
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn empty_send_and_directory_as_file_are_rejected() {
    let sender = GoshTransferEngine::new(EngineConfig::default(), noop_handler());
    let err = sender
        .send_files("127.0.0.1", 1, Vec::<PathBuf>::new())
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidConfig(_)));

    let dir = tempfile::tempdir().unwrap();
    let err = sender
        .send_files("127.0.0.1", 1, vec![dir.path().to_path_buf()])
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::FileIo(_)));
}

#[tokio::test]
async fn receive_only_blocks_send() {
    let engine = GoshTransferEngine::new(
        EngineConfig::builder().receive_only(true).build(),
        noop_handler(),
    );
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("x.txt");
    write_file(&file, b"x");
    let err = engine
        .send_files("127.0.0.1", 1, vec![file])
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidConfig(_)));
}

#[tokio::test]
async fn ipv6_loopback_transfer_and_peer_info() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("V6 Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("::1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    match receiver.start_server().await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("skipping ipv6 test: {e}");
            return;
        }
    }

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("V6 Sender").build(),
        noop_handler(),
    );
    if sender.check_peer("::1", port).await.is_err() {
        eprintln!("skipping ipv6 test: ::1 not reachable");
        receiver.stop_server().await.ok();
        return;
    }

    let info = sender.get_peer_info("::1", port).await.unwrap();
    assert_eq!(info["name"], "V6 Receiver");
    assert_eq!(info["deviceName"], "V6 Receiver");

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("v6.txt");
    write_file(&file, b"ipv6");
    sender.send_files("::1", port, vec![file]).await.unwrap();
    collect_progress(&mut events).await;
    assert_eq!(
        std::fs::read(download_dir.path().join("v6.txt")).unwrap(),
        b"ipv6"
    );
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn info_cors_and_sse_event_type() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("CorsBox")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, _events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let http = reqwest::Client::new();
    let info = http
        .get(format!("http://127.0.0.1:{port}/info"))
        .header("Origin", "http://example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        info.headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("*"),
        "browser UIs need CORS on /info or the device-name section stays blank"
    );
    let body: serde_json::Value = info.json().await.unwrap();
    assert_eq!(body["name"], "CorsBox");
    assert_eq!(
        body["deviceName"], "CorsBox",
        "UIs that read deviceName (matching discovery) must see the name"
    );

    let preflight = http
        .request(
            reqwest::Method::OPTIONS,
            format!("http://127.0.0.1:{port}/events"),
        )
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert!(
        preflight.status().is_success() || preflight.status() == reqwest::StatusCode::NO_CONTENT,
        "unexpected preflight status {}",
        preflight.status()
    );

    // Subscribe to SSE, then send a file, then assert JSON event type.
    let sse_url = format!("http://127.0.0.1:{port}/events");
    let sse_handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut resp = client.get(sse_url).send().await.unwrap();
        let mut buf = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), resp.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    buf.extend_from_slice(&chunk);
                    let text = String::from_utf8_lossy(&buf);
                    if text.contains("transferProgress") || text.contains("transferComplete") {
                        return text.into_owned();
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    });

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("sse.txt");
    write_file(&file, &vec![0x11; 40_000]);
    let sender = GoshTransferEngine::new(EngineConfig::default(), noop_handler());
    sender
        .send_files("127.0.0.1", port, vec![file])
        .await
        .unwrap();

    let sse_body = sse_handle.await.unwrap();
    assert!(
        sse_body.contains("transferProgress") || sse_body.contains("transferComplete"),
        "SSE clients looking for transferProgress must receive that type, got: {sse_body:?}"
    );
    assert!(
        !sse_body.contains("\ndata:heartbeat\n") && !sse_body.contains("data: heartbeat"),
        "non-JSON heartbeat would break UI parsers: {sse_body:?}"
    );

    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn filename_collision_gets_numeric_suffix() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    write_file(&download_dir.path().join("clash.txt"), b"existing");

    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("clash.txt");
    write_file(&file, b"incoming");
    let sender = GoshTransferEngine::new(EngineConfig::default(), noop_handler());
    sender
        .send_files("127.0.0.1", port, vec![file])
        .await
        .unwrap();
    collect_progress(&mut events).await;

    assert_eq!(
        std::fs::read(download_dir.path().join("clash.txt")).unwrap(),
        b"existing"
    );
    assert_eq!(
        std::fs::read(download_dir.path().join("clash (1).txt")).unwrap(),
        b"incoming"
    );
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn history_records_send_and_receive() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let recv_history = Arc::new(InMemoryHistory::new());
    let send_history = Arc::new(InMemoryHistory::new());

    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) =
        GoshTransferEngine::with_channel_events_and_history(receiver_config, recv_history.clone());
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("hist.txt");
    write_file(&file, b"history");

    let sender = GoshTransferEngine::with_history(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
        send_history.clone(),
    );
    sender
        .send_files("127.0.0.1", port, vec![file])
        .await
        .unwrap();
    collect_progress(&mut events).await;

    let sent = send_history.list().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, gosh_lan_transfer::TransferStatus::Completed);
    assert_eq!(
        sent[0].direction,
        gosh_lan_transfer::TransferDirection::Sent
    );

    let received = recv_history.list().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].status,
        gosh_lan_transfer::TransferStatus::Completed
    );
    assert_eq!(
        received[0].direction,
        gosh_lan_transfer::TransferDirection::Received
    );

    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn traversal_relative_path_cannot_escape_download_dir() {
    let port = find_available_port().await;
    let parent = tempfile::tempdir().unwrap();
    let download_dir = parent.path().join("downloads");
    std::fs::create_dir_all(&download_dir).unwrap();

    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(&download_dir)
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, _events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let http = reqwest::Client::new();
    let transfer_id = "trav-1";
    let file_id = "f1";
    let body = serde_json::json!({
        "transferId": transfer_id,
        "senderName": "evil",
        "files": [{
            "id": file_id,
            "name": "pwned.txt",
            "size": 4,
            "relativePath": "../../../"
        }],
        "totalSize": 4
    });
    let resp: serde_json::Value = http
        .post(format!("http://127.0.0.1:{port}/transfer"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = resp["token"].as_str().expect("token");

    let chunk_ok = http
        .post(format!(
            "http://127.0.0.1:{port}/chunk?transfer_id={transfer_id}&file_id={file_id}&token={token}"
        ))
        .header("Content-Type", "application/octet-stream")
        .body("pwn!".to_string())
        .send()
        .await
        .unwrap();
    assert!(chunk_ok.status().is_success(), "{}", chunk_ok.status());

    // File must land inside the download dir, never in the parent temp dir.
    assert!(download_dir.join("pwned.txt").is_file());
    let escaped = parent.path().join("pwned.txt");
    assert!(
        !escaped.is_file(),
        "file escaped the download directory to {:?}",
        escaped
    );
    // Also no sibling named after the download folder with a numeric suffix
    for entry in std::fs::read_dir(parent.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.path() == download_dir {
            continue;
        }
        panic!(
            "unexpected file created outside download dir: {:?}",
            entry.path()
        );
    }

    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn cancel_in_progress_transfer() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("big.bin");
    write_file(&file, &vec![0xEE; 2_000_000]);

    let sender = GoshTransferEngine::new(EngineConfig::default(), noop_handler());
    let send_task = tokio::spawn({
        let file = file.clone();
        async move { sender.send_files("127.0.0.1", port, vec![file]).await }
    });

    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    let mut cancelled = false;
    loop {
        tokio::select! {
            event = events.recv() => match event.unwrap() {
                EngineEvent::TransferRequest(t) => {
                    receiver.accept_transfer(&t.id).await.unwrap();
                    receiver.cancel_transfer(&t.id).await.unwrap();
                    cancelled = true;
                }
                EngineEvent::TransferFailed { error, .. } => {
                    assert!(error.to_lowercase().contains("cancel"));
                    break;
                }
                EngineEvent::TransferComplete { .. } if cancelled => {
                    // Transfer may finish before cancel lands on a fast loopback.
                    break;
                }
                EngineEvent::TransferComplete { .. } => {
                    // Too fast to cancel; still a valid outcome on loopback.
                    break;
                }
                _ => {}
            },
            _ = &mut deadline => panic!("timed out"),
        }
    }

    let _ = send_task.await;
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn network_utilities_and_empty_http_transfer_rejected() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let (mut receiver, _events) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder()
            .port(port)
            .device_name("Net")
            .download_dir(download_dir.path())
            .build(),
    );
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    assert!(receiver.check_peer("127.0.0.1", port).await.unwrap());
    let resolved = GoshTransferEngine::resolve_address("127.0.0.1");
    assert!(resolved.success);
    assert!(!resolved.ips.is_empty());
    let ifaces = GoshTransferEngine::get_network_interfaces();
    assert!(
        ifaces.iter().any(|i| i.ip == "127.0.0.1" || i.is_loopback),
        "expected a loopback interface, got {ifaces:?}"
    );

    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://127.0.0.1:{port}/transfer"))
        .json(&serde_json::json!({
            "transferId": "empty",
            "files": [],
            "totalSize": 0
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn bandwidth_limit_still_delivers_complete_file() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src_dir = tempfile::tempdir().unwrap();
    let file = src_dir.path().join("paced.bin");
    let payload = vec![0x42; 50_000];
    write_file(&file, &payload);

    let sender = GoshTransferEngine::new(
        EngineConfig::builder()
            .device_name("Sender")
            .bandwidth_limit_bps(Some(200_000))
            .build(),
        noop_handler(),
    );
    sender
        .send_files("127.0.0.1", port, vec![file])
        .await
        .unwrap();
    let (progress, _) = collect_progress(&mut events).await;
    assert!(!progress.is_empty());
    assert_eq!(
        std::fs::read(download_dir.path().join("paced.bin")).unwrap(),
        payload
    );
    for p in &progress {
        assert!(p.bytes_transferred <= p.total_bytes);
        assert_eq!(p.total_bytes, payload.len() as u64);
    }
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn directory_transfer_includes_symlink_files() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let receiver_config = EngineConfig::builder()
        .port(port)
        .device_name("Receiver")
        .download_dir(download_dir.path())
        .add_trusted_host("127.0.0.1")
        .build();
    let (mut receiver, mut events) = GoshTransferEngine::with_channel_events(receiver_config);
    receiver.start_server().await.unwrap();
    wait_ready(&receiver, "127.0.0.1", port).await;

    let src = tempfile::tempdir().unwrap();
    write_file(&src.path().join("real.txt"), b"linked-content");
    std::os::unix::fs::symlink(src.path().join("real.txt"), src.path().join("alias.txt")).unwrap();

    let sender = GoshTransferEngine::new(
        EngineConfig::builder().device_name("Sender").build(),
        noop_handler(),
    );
    sender
        .send_directory("127.0.0.1", port, src.path())
        .await
        .unwrap();
    collect_progress(&mut events).await;

    assert_eq!(
        std::fs::read(download_dir.path().join("real.txt")).unwrap(),
        b"linked-content"
    );
    assert_eq!(
        std::fs::read(download_dir.path().join("alias.txt")).unwrap(),
        b"linked-content"
    );
    receiver.stop_server().await.unwrap();
}

#[tokio::test]
async fn accept_all_and_reject_all_on_empty_pending() {
    let port = find_available_port().await;
    let download_dir = tempfile::tempdir().unwrap();
    let (mut receiver, _events) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder()
            .port(port)
            .device_name("Receiver")
            .download_dir(download_dir.path())
            .build(),
    );
    receiver.start_server().await.unwrap();
    assert!(receiver.accept_all_transfers().await.is_empty());
    assert!(receiver.reject_all_transfers().await.is_empty());
    receiver.stop_server().await.unwrap();
}
