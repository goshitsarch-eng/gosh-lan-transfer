use gosh_lan_transfer::{EngineConfig, EngineEvent, GoshTransferEngine};
use serde_json::{json, Value};
use std::time::Duration;

async fn receiver() -> (GoshTransferEngine, tempfile::TempDir, reqwest::Client) {
    let dir = tempfile::tempdir().unwrap();
    let (mut engine, _) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder()
            .port(0)
            .download_dir(dir.path())
            .add_trusted_host("127.0.0.1")
            .build(),
    );
    engine.start_server().await.unwrap();
    (
        engine,
        dir,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
}
fn request(id: &str) -> Value {
    json!({"transferId":id,"files":[{"id":"f","name":"data.txt","size":3}],"totalSize":3})
}
fn url(e: &GoshTransferEngine, path: &str) -> String {
    format!("http://127.0.0.1:{}{path}", e.port())
}
async fn register(e: &GoshTransferEngine, c: &reqwest::Client, r: &Value) -> String {
    let response = c.post(url(e, "/transfer")).json(r).send().await.unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    response.json::<Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .into()
}
fn chunk(e: &GoshTransferEngine, id: &str, token: &str) -> String {
    url(
        e,
        &format!("/chunk?transfer_id={id}&file_id=f&token={token}"),
    )
}
#[tokio::test]
async fn registration_is_idempotent_and_cannot_replace_approved_files() {
    let (mut e, _, c) = receiver().await;
    let r = request("repeat");
    let token = register(&e, &c, &r).await;
    assert_eq!(register(&e, &c, &r).await, token);
    let mut changed = r.clone();
    changed["files"][0]["name"] = json!("evil.txt");
    assert_eq!(
        c.post(url(&e, "/transfer"))
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn duplicate_uploads_only_write_one_file_and_complete_once() {
    let dir = tempfile::tempdir().unwrap();
    let (mut e, mut events) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder()
            .port(0)
            .download_dir(dir.path())
            .add_trusted_host("127.0.0.1")
            .build(),
    );
    e.start_server().await.unwrap();
    let c = reqwest::Client::builder().no_proxy().build().unwrap();
    let token = register(&e, &c, &request("twice")).await;
    let target = chunk(&e, "twice", &token);
    let (a, b) = tokio::join!(
        c.post(&target).body("abc").send(),
        c.post(&target).body("abc").send()
    );
    assert!(a.unwrap().status().is_success());
    assert!(b.unwrap().status().is_success());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    assert_eq!(std::fs::read(dir.path().join("data.txt")).unwrap(), b"abc");
    let mut completions = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, EngineEvent::TransferComplete { .. }) {
            completions += 1;
        }
    }
    assert_eq!(completions, 1);
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn malformed_metadata_and_browser_requests_are_rejected() {
    let (mut e, _, c) = receiver().await;
    let mut duplicate = request("dupe");
    duplicate["files"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"f","name":"b","size":0}));
    let mut overflow = request("overflow");
    overflow["files"][0]["size"] = json!(u64::MAX);
    overflow["files"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"b","name":"b","size":1}));
    for r in [duplicate, overflow, request("")] {
        assert_eq!(
            c.post(url(&e, "/transfer"))
                .json(&r)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    assert_eq!(
        c.post(url(&e, "/transfer"))
            .header("Origin", "https://evil.example")
            .json(&request("web"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn cancellation_interrupts_an_upload_with_no_more_body_data() {
    let (mut e, dir, c) = receiver().await;
    let token = register(&e, &c, &request("cancel")).await;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);
    let target = chunk(&e, "cancel", &token);
    let upload = tokio::spawn(async move {
        c.post(target)
            .body(reqwest::Body::wrap_stream(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            ))
            .send()
            .await
    });
    tx.send(Ok(bytes::Bytes::from_static(b"a"))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dir.path().join("data.txt").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    e.cancel_transfer("cancel").await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), upload)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        410
    );
    assert!(!dir.path().join("data.txt").exists());
    drop(tx);
    e.stop_server().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn existing_directory_symlink_cannot_escape_receive_root() {
    let (mut e, dir, c) = receiver().await;
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("escape")).unwrap();
    let mut r = request("link");
    r["files"][0]["relativePath"] = json!("escape/new/data.txt");
    let token = register(&e, &c, &r).await;
    assert!(!c
        .post(chunk(&e, "link", &token))
        .body("abc")
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    e.stop_server().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn status_token_and_upload_are_bound_to_original_source_ip() {
    let (mut e, _, c) = receiver().await;
    let token = register(&e, &c, &request("owner")).await;
    let other = reqwest::Client::builder().no_proxy().build().unwrap();
    let status = other
        .get(url(&e, "/transfer/status?transfer_id=owner").replace("127.0.0.1", "[::1]"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert!(status["token"].is_null());
    assert_eq!(
        other
            .post(chunk(&e, "owner", &token).replace("127.0.0.1", "[::1]"))
            .body("abc")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn restart_rebinds_and_runtime_config_reports_actual_port() {
    let (mut e, _, _) = receiver().await;
    let port = e.port();
    assert_ne!(port, 0);
    let download = e.config().download_dir.clone();
    let config = EngineConfig::builder()
        .port(12345)
        .download_dir("changed-location")
        .build();
    e.update_config(config).await;
    assert_eq!(e.port(), port);
    assert_eq!(e.config().download_dir, download);
    e.stop_server().await.unwrap();
    e.start_server().await.unwrap();
    assert!(e.check_peer("127.0.0.1", port).await.unwrap());
    e.stop_server().await.unwrap();
}

#[tokio::test]
async fn upload_retries_server_failures_but_not_client_errors() {
    use axum::{
        body::{to_bytes, Body},
        extract::State,
        http::StatusCode,
        routing::post,
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    #[derive(Clone)]
    struct Mock {
        attempts: Arc<AtomicUsize>,
        failure: StatusCode,
    }
    async fn upload(State(state): State<Mock>, body: Body) -> StatusCode {
        assert_eq!(to_bytes(body, 1024).await.unwrap().as_ref(), b"abc");
        if state.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            state.failure
        } else {
            StatusCode::OK
        }
    }
    for (status, expected, succeeds) in [
        (StatusCode::INTERNAL_SERVER_ERROR, 2, true),
        (StatusCode::BAD_REQUEST, 1, false),
    ] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/transfer",
                post(|| async { Json(json!({"accepted":true,"token":"t"})) }),
            )
            .route("/chunk", post(upload))
            .with_state(Mock {
                attempts: attempts.clone(),
                failure: status,
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"abc").unwrap();
        let (sender, mut events) = GoshTransferEngine::with_channel_events(
            EngineConfig::builder()
                .retry_delay_ms(1)
                .max_retries(1)
                .build(),
        );
        assert_eq!(
            sender
                .send_files_legacy("127.0.0.1", port, vec![path])
                .await
                .is_ok(),
            succeeds
        );
        assert_eq!(attempts.load(Ordering::SeqCst), expected);
        let mut retry_events = 0;
        while let Ok(event) = events.try_recv() {
            match event {
                EngineEvent::TransferRetry { .. } => retry_events += 1,
                EngineEvent::TransferProgress(p) => assert!(p.bytes_transferred <= p.total_bytes),
                _ => {}
            }
        }
        assert_eq!(retry_events, expected - 1);
        server.abort();
    }
}
