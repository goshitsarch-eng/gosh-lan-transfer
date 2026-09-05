use gosh_lan_transfer::resumable::ResumeStatus;
use gosh_lan_transfer::{
    EngineConfig, EngineError, EngineEvent, GoshTransferEngine, PreparedTransfer, SecurityConfig,
    TlsIdentity,
};
use serde_json::Value;
use std::{path::Path, time::Duration};

async fn receiver(
    dir: &Path,
    port: u16,
    trusted: bool,
) -> (
    GoshTransferEngine,
    tokio::sync::broadcast::Receiver<EngineEvent>,
) {
    let mut config = EngineConfig::builder().port(port).download_dir(dir);
    if trusted {
        config = config.add_trusted_host("127.0.0.1");
    }
    let (mut engine, events) = GoshTransferEngine::with_channel_events(config.build());
    engine.start_server().await.unwrap();
    (engine, events)
}
fn http() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}
fn url(e: &GoshTransferEngine, path: &str) -> String {
    format!("http://127.0.0.1:{}{path}", e.port())
}
async fn plan(source: &Path, e: &GoshTransferEngine, bytes: &[u8]) -> PreparedTransfer {
    let file = source.join("data.bin");
    std::fs::write(&file, bytes).unwrap();
    GoshTransferEngine::new(EngineConfig::default(), gosh_lan_transfer::noop_handler())
        .prepare_files("127.0.0.1", e.port(), vec![file])
        .await
        .unwrap()
}
async fn register(e: &GoshTransferEngine, p: &PreparedTransfer) -> ResumeStatus {
    let response = http()
        .post(url(e, "/v2/transfer"))
        .json(&p.manifest().unwrap())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    response.json().await.unwrap()
}
async fn status(e: &GoshTransferEngine, p: &PreparedTransfer) -> ResumeStatus {
    http()
        .get(url(e, "/v2/status"))
        .query(&[("transfer_id", &p.transfer_id)])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn chunk(
    e: &GoshTransferEngine,
    p: &PreparedTransfer,
    token: &str,
    offset: u64,
    bytes: &[u8],
) -> reqwest::StatusCode {
    http()
        .post(url(e, "/v2/chunk"))
        .query(&[
            ("transfer_id", p.transfer_id.clone()),
            ("file_id", p.files[0].info.id.clone()),
            ("offset", offset.to_string()),
        ])
        .header("X-Transfer-Token", token)
        .body(bytes.to_vec())
        .send()
        .await
        .unwrap()
        .status()
}
fn tls(dir: &Path) -> (SecurityConfig, SecurityConfig) {
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".into(),
        "127.0.0.1".into(),
        "::1".into(),
    ])
    .unwrap();
    let certpath = dir.join("cert.pem");
    let keypath = dir.join("key.pem");
    std::fs::write(&certpath, cert.cert.pem()).unwrap();
    std::fs::write(&keypath, cert.key_pair.serialize_pem()).unwrap();
    let token = SecurityConfig::generate_token();
    (
        SecurityConfig {
            identity: Some(TlsIdentity {
                certificate: certpath.clone(),
                private_key: keypath,
            }),
            auth_token: Some(token.clone()),
            allowed_origins: vec!["https://app.example".into()],
            ..Default::default()
        },
        SecurityConfig {
            https: true,
            trusted_certificates: vec![certpath],
            peer_token: Some(token),
            ..Default::default()
        },
    )
}
#[tokio::test]
async fn verified_https_authentication_and_browser_access() {
    let dir = tempfile::tempdir().unwrap();
    let certs = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (server_security, client_security) = tls(certs.path());
    assert!(!format!("{server_security:?}").contains(server_security.auth_token.as_ref().unwrap()));
    let (mut e, _) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder()
            .port(0)
            .download_dir(dir.path())
            .add_trusted_host("127.0.0.1")
            .security(server_security.clone())
            .build(),
    );
    e.start_server().await.unwrap();
    let sender = GoshTransferEngine::new(
        EngineConfig::builder()
            .security(client_security.clone())
            .build(),
        gosh_lan_transfer::noop_handler(),
    );
    assert!(sender.check_peer("127.0.0.1", e.port()).await.unwrap());
    std::fs::write(src.path().join("secure.txt"), b"verified TLS data").unwrap();
    sender
        .send_files("127.0.0.1", e.port(), vec![src.path().join("secure.txt")])
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("secure.txt")).unwrap(),
        b"verified TLS data"
    );
    let cert = reqwest::Certificate::from_pem(
        &std::fs::read(&client_security.trusted_certificates[0]).unwrap(),
    )
    .unwrap();
    let c = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let base = format!("https://127.0.0.1:{}", e.port());
    for endpoint in [
        "/info",
        "/events",
        "/health",
        "/v2/status?transfer_id=unknown",
    ] {
        assert_eq!(
            c.get(format!("{base}{endpoint}"))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    assert_eq!(
        c.get(format!("{base}/info"))
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let preflight = c
        .request(reqwest::Method::OPTIONS, format!("{base}/v2/transfer"))
        .header("Origin", "https://app.example")
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "authorization,content-type",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert_eq!(
        preflight.headers()["access-control-allow-origin"],
        "https://app.example"
    );
    let allowed = c
        .get(format!("{base}/info"))
        .header("Origin", "https://app.example")
        .bearer_auth(server_security.auth_token.as_ref().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 200);
    assert_eq!(
        allowed.headers()["access-control-allow-origin"],
        "https://app.example"
    );
    assert_eq!(
        c.get(format!("{base}/info"))
            .header("Origin", "https://evil.example")
            .bearer_auth(server_security.auth_token.as_ref().unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    // Trusting neither this self-signed certificate nor an unrelated certificate must fail.
    assert!(http().get(format!("{base}/health")).send().await.is_err());
    let other = tempfile::tempdir().unwrap();
    let (_, mut wrong) = tls(other.path());
    wrong.peer_token = client_security.peer_token.clone();
    let wrong = GoshTransferEngine::new(
        EngineConfig::builder().security(wrong).build(),
        gosh_lan_transfer::noop_handler(),
    );
    assert!(wrong.check_peer("127.0.0.1", e.port()).await.is_err());
    // A trusted cert still cannot authenticate an unrelated host name.
    let c = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(
                &std::fs::read(&client_security.trusted_certificates[0]).unwrap(),
            )
            .unwrap(),
        )
        .resolve(
            "wrong.example",
            format!("127.0.0.1:{}", e.port()).parse().unwrap(),
        )
        .build()
        .unwrap();
    assert!(c
        .get(format!("https://wrong.example:{}/health", e.port()))
        .send()
        .await
        .is_err());
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn credentials_cannot_be_configured_over_plain_http() {
    let security = SecurityConfig {
        auth_token: Some(SecurityConfig::generate_token()),
        ..Default::default()
    };
    let (mut engine, _) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder().port(0).security(security).build(),
    );
    assert!(engine.start_server().await.is_err());
    let engine = GoshTransferEngine::new(
        EngineConfig::builder()
            .security(SecurityConfig {
                peer_token: Some(SecurityConfig::generate_token()),
                ..Default::default()
            })
            .build(),
        gosh_lan_transfer::noop_handler(),
    );
    assert!(matches!(
        engine.check_peer("127.0.0.1", 1).await,
        Err(EngineError::InvalidConfig(_))
    ));
}
#[tokio::test]
async fn restart_resumes_saved_plan_at_the_durable_offset() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, _) = receiver(dir.path(), 0, true).await;
    let port = e.port();
    let p = plan(src.path(), &e, b"0123456789").await;
    let saved = src.path().join("plan.json");
    p.save(&saved).await.unwrap();
    let accepted = register(&e, &p).await;
    assert_eq!(
        chunk(&e, &p, accepted.token.as_ref().unwrap(), 0, b"0123").await,
        400
    );
    assert_eq!(status(&e, &p).await.files[0].offset, 4);
    assert!(!dir.path().join("data.bin").exists());
    e.stop_server().await.unwrap();
    drop(e);
    let (mut e, _) = receiver(dir.path(), port, true).await;
    assert_eq!(status(&e, &p).await.files[0].offset, 4);
    let sender =
        GoshTransferEngine::new(EngineConfig::default(), gosh_lan_transfer::noop_handler());
    sender
        .send_prepared(PreparedTransfer::load(&saved).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("data.bin")).unwrap(),
        b"0123456789"
    );
    // Completed receipts also survive restart and prevent duplicate files.
    e.stop_server().await.unwrap();
    drop(e);
    let (mut e, _) = receiver(dir.path(), port, true).await;
    sender.send_prepared(p.clone()).await.unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    assert!(status(&e, &p).await.complete);
    e.forget_received_transfer(&p.transfer_id).await.unwrap();
    assert!(dir.path().join("data.bin").exists());
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn checksum_mismatch_is_discarded_and_source_changes_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, _) = receiver(dir.path(), 0, true).await;
    let p = plan(src.path(), &e, b"abcdef").await;
    let accepted = register(&e, &p).await;
    assert_eq!(
        chunk(&e, &p, accepted.token.as_ref().unwrap(), 0, b"xxxxxx").await,
        422
    );
    assert!(!dir.path().join("data.bin").exists());
    assert_eq!(status(&e, &p).await.files[0].offset, 0);
    std::fs::write(&p.files[0].path, b"ABCDEF").unwrap();
    let sender =
        GoshTransferEngine::new(EngineConfig::default(), gosh_lan_transfer::noop_handler());
    assert!(matches!(
        sender.send_prepared(p).await,
        Err(EngineError::FileIo(_))
    ));
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn sender_can_cancel_while_waiting_for_approval() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, mut events) = receiver(dir.path(), 0, false).await;
    let p = plan(src.path(), &e, b"cancel me").await;
    let sender =
        GoshTransferEngine::new(EngineConfig::default(), gosh_lan_transfer::noop_handler());
    let handle = sender.start_prepared(p.clone());
    assert_eq!(handle.id(), p.transfer_id);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                events.recv().await.unwrap(),
                EngineEvent::TransferRequest(_)
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    handle.cancel();
    assert!(matches!(
        handle.wait().await,
        Err(EngineError::TransferCancelled)
    ));
    assert!(status(&e, &p).await.cancelled);
    assert!(e.get_pending_transfers().await.is_empty());
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn sender_cancellation_interrupts_active_upload_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, mut events) = receiver(dir.path(), 0, true).await;
    let port = e.port();
    let p = plan(src.path(), &e, &vec![7; 256 * 1024]).await;
    let sender = GoshTransferEngine::new(
        EngineConfig::builder()
            .bandwidth_limit_bps(Some(64 * 1024))
            .build(),
        gosh_lan_transfer::noop_handler(),
    );
    let handle = sender.start_prepared(p.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                events.recv().await.unwrap(),
                EngineEvent::TransferProgress(_)
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    handle.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), handle.wait())
            .await
            .unwrap(),
        Err(EngineError::TransferCancelled)
    ));
    assert!(!dir.path().join("data.bin").exists());
    assert!(status(&e, &p).await.cancelled);
    e.stop_server().await.unwrap();
    drop(e);
    let (mut e, _) = receiver(dir.path(), port, true).await;
    assert!(status(&e, &p).await.cancelled);
    assert!(matches!(
        sender.send_prepared(p).await,
        Err(EngineError::TransferCancelled)
    ));
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn partial_files_are_isolated_between_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, _) = receiver(dir.path(), 0, true).await;
    let p = plan(src.path(), &e, b"abcdef").await;
    let mut other = p.clone();
    other.transfer_id = uuid::Uuid::new_v4().to_string();
    let a = register(&e, &p).await;
    let b = register(&e, &other).await;
    assert_eq!(
        chunk(&e, &p, a.token.as_ref().unwrap(), 0, b"ab").await,
        400
    );
    assert_eq!(status(&e, &other).await.files[0].offset, 0);
    assert_eq!(
        chunk(&e, &other, b.token.as_ref().unwrap(), 0, b"a").await,
        400
    );
    assert_eq!(status(&e, &p).await.files[0].offset, 2);
    assert_eq!(status(&e, &other).await.files[0].offset, 1);
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn only_one_receiver_may_own_a_journal_directory() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, _) = receiver(dir.path(), 0, true).await;
    let p = plan(src.path(), &e, b"a").await;
    register(&e, &p).await;
    let (mut other, _) = GoshTransferEngine::with_channel_events(
        EngineConfig::builder()
            .port(0)
            .download_dir(dir.path())
            .build(),
    );
    assert!(other.start_server().await.is_err());
    assert!(e.forget_received_transfer(&p.transfer_id).await.is_err());
    e.cancel_transfer(&p.transfer_id).await.unwrap();
    e.forget_received_transfer(&p.transfer_id).await.unwrap();
    e.stop_server().await.unwrap();
}
#[tokio::test]
async fn crash_after_publication_does_not_duplicate_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, _) = receiver(dir.path(), 0, true).await;
    let port = e.port();
    let p = plan(src.path(), &e, b"data").await;
    let sender =
        GoshTransferEngine::new(EngineConfig::default(), gosh_lan_transfer::noop_handler());
    sender.send_prepared(p.clone()).await.unwrap();
    e.stop_server().await.unwrap();
    drop(e);
    let journal = dir
        .path()
        .join(".gosh-transfer")
        .join(format!("{}.json", p.transfer_id));
    let mut record: Value = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
    record["complete"] = Value::Bool(false);
    record["published"] = serde_json::json!({});
    std::fs::write(&journal, serde_json::to_vec(&record).unwrap()).unwrap();
    let (mut e, _) = receiver(dir.path(), port, true).await;
    sender.send_prepared(p).await.unwrap();
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    assert_eq!(std::fs::read(dir.path().join("data.bin")).unwrap(), b"data");
    e.stop_server().await.unwrap();
}

#[tokio::test]
async fn journal_namespace_and_noncanonical_ids_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let src = tempfile::tempdir().unwrap();
    let (mut e, _) = receiver(dir.path(), 0, true).await;
    let mut p = plan(src.path(), &e, b"data").await;
    let mut manifest = p.manifest().unwrap();
    manifest.request.transfer_id = manifest.request.transfer_id.to_uppercase();
    assert_eq!(
        http()
            .post(url(&e, "/v2/transfer"))
            .json(&manifest)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    p.files[0].info.relative_path = Some(".GOSH-TRANSFER/receiver.lock".into());
    let response = register(&e, &p).await;
    assert_eq!(
        chunk(&e, &p, response.token.as_ref().unwrap(), 0, b"data").await,
        400
    );
    assert_eq!(
        std::fs::metadata(dir.path().join(".gosh-transfer/receiver.lock"))
            .unwrap()
            .len(),
        0
    );
    // Receiver state remains readable and cancellable after the rejected destination.
    e.cancel_transfer(&p.transfer_id).await.unwrap();
    e.forget_received_transfer(&p.transfer_id).await.unwrap();
    e.stop_server().await.unwrap();
}
