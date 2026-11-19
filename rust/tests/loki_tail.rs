//! Live tail over WebSocket against a local test server.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use grow_grafana_loki as sdk;
use sdk::core::ErrorCode;
use sdk::logging::{LogQueryClient, TailOptions};
use sdk::{LiveTailOptions, LokiClient, TailFrame};
use support::{client, closed_port, config, fixture, raw_server, ws_server, WsScript};
use wiremock::matchers::path;
use wiremock::{Mock, MockServer};

type Frames = Arc<Mutex<Vec<TailFrame>>>;

fn collector() -> (Frames, impl Fn(TailFrame) + Send + Sync + 'static) {
    let frames: Frames = Arc::new(Mutex::new(Vec::new()));
    let sink = frames.clone();
    (frames, move |f| sink.lock().unwrap().push(f))
}

async fn wait_until(f: impl Fn() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

#[tokio::test]
async fn tail_delivers_frames_with_handshake_headers() {
    let frames = vec![
        fixture("tail.ok").frame(),
        fixture("tail.categorized-labels").frame(),
        r#"{"streams":[],"dropped_entries":[{"labels":{"a":"b"},"timestamp":"1795035084588079388"},{"timestamp":"1"}]}"#.to_owned(),
    ];
    let (url, seen) = ws_server(WsScript::Frames(frames)).await;
    let c = LokiClient::new(
        config(&format!("{url}/base/"))
            .tenant_id("tenant-a")
            .bearer_token("tail-token-1")
            .header("x-extra", "1")
            .build()
            .unwrap(),
    )
    .unwrap();
    let (got, sink) = collector();
    let s = c
        .tail(
            r#"{run="x"}"#,
            sink,
            LiveTailOptions {
                start: Some(std::time::UNIX_EPOCH + Duration::from_secs(1)),
                limit: 50,
                delay_for: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
    wait_until(|| got.lock().unwrap().len() == 3).await;
    let frames = got.lock().unwrap().clone();
    assert_eq!(
        frames[0].streams[0].entries[0].line,
        "tail marker c023892b83"
    );
    assert_eq!(frames[0].streams[0].labels["service_name"], "sdk-verify");
    assert!(!frames[1].streams[0].entries[0]
        .structured_metadata
        .is_empty());
    assert_eq!(frames[2].dropped_entries.len(), 2);
    assert_eq!(frames[2].dropped_entries[0].labels["a"], "b");
    assert_eq!(s.dropped_count(), 2);
    assert!(format!("{s:?}").contains("TailSession"));
    let h = seen.lock().unwrap().clone();
    assert!(h.uri.starts_with("/base/loki/api/v1/tail?"), "{}", h.uri);
    assert!(
        h.uri.contains("start=1000000000")
            && h.uri.contains("limit=50")
            && h.uri.contains("delay_for=2")
    );
    assert_eq!(h.header("x-scope-orgid").as_deref(), Some("tenant-a"));
    assert_eq!(
        h.header("authorization").as_deref(),
        Some("Bearer tail-token-1")
    );
    assert_eq!(
        h.header("x-loki-response-encoding-flags").as_deref(),
        Some("categorize-labels")
    );
    assert_eq!(h.header("x-extra").as_deref(), Some("1"));
    assert!(h
        .header("user-agent")
        .unwrap()
        .starts_with("grow-codes-grafana-loki/"));
    assert!(!s.is_done());
    s.stop().await;
    s.stop().await;
    assert!(s.is_done());
    assert!(s.err().is_none());
}

#[tokio::test]
async fn tail_binary_frames_and_client_close() {
    let (url, _) = ws_server(WsScript::Binary(vec![fixture("tail.ok").frame()])).await;
    let c = client(&url);
    let (got, sink) = collector();
    let s = c
        .tail(r#"{a="b"}"#, sink, LiveTailOptions::default())
        .await
        .unwrap();
    wait_until(|| !got.lock().unwrap().is_empty()).await;
    let h = c.with_tenant("t").unwrap();
    // closing the client stops its tails
    c.close().await;
    assert!(s.is_done());
    assert_eq!(
        h.tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Cancelled
    );
}

#[tokio::test]
async fn tail_server_close_reasons() {
    for (code, reason, want) in [
        (
            1000u16,
            "only log selector is supported",
            Some(ErrorCode::Validation),
        ),
        (1000, "", None),
        (1001, "going away", None),
        (1011, "internal", Some(ErrorCode::Unavailable)),
    ] {
        let (url, _) = ws_server(WsScript::FramesThenClose(
            vec![fixture("tail.ok").frame()],
            code,
            reason.to_owned(),
        ))
        .await;
        let c = client(&url);
        let (got, sink) = collector();
        let s = c
            .tail(r#"{a="b"}"#, sink, LiveTailOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), s.wait())
            .await
            .unwrap();
        assert_eq!(got.lock().unwrap().len(), 1);
        assert_eq!(s.err().map(|e| e.code()), want, "{code} {reason}");
    }
}

#[tokio::test]
async fn tail_invalid_frame_and_connection_lost() {
    let (url, _) = ws_server(WsScript::Frames(vec!["not json".into()])).await;
    let c = client(&url);
    let s = c
        .tail(
            r#"{a="b"}"#,
            |_| panic!("callback must not run"),
            LiveTailOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.wait())
        .await
        .unwrap();
    let err = s.err().unwrap();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.message().contains("invalid frame"));

    for bad in [
        r#"{"streams":[{"stream":{},"values":[["x","l"]]}]}"#,
        r#"{"dropped_entries":[{"labels":{}}]}"#,
        r#"{"dropped_entries":[{"timestamp":"x"}]}"#,
    ] {
        let (url, _) = ws_server(WsScript::Frames(vec![bad.into()])).await;
        let s = client(&url)
            .tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), s.wait())
            .await
            .unwrap();
        assert!(
            s.err().unwrap().message().contains("invalid frame"),
            "{bad}"
        );
    }

    // a panicking callback does not stop the tail; a dropped TCP connection is UNAVAILABLE
    let (url, _) = ws_server(WsScript::FramesThenDrop(vec![
        fixture("tail.ok").frame(),
        fixture("tail.ok").frame(),
    ]))
    .await;
    let calls = Arc::new(Mutex::new(0));
    let counter = calls.clone();
    let s = client(&url)
        .tail(
            r#"{a="b"}"#,
            move |_| {
                *counter.lock().unwrap() += 1;
                panic!("boom");
            },
            LiveTailOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), s.wait())
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), 2);
    let err = s.err().unwrap();
    assert_eq!(err.code(), ErrorCode::Unavailable, "{err}");
}

#[tokio::test]
async fn tail_handshake_errors() {
    let server = MockServer::start().await;
    for (p, name) in [
        ("/mt", "tail.mt-no-tenant"),
        ("/bad", "tail.bad-query"),
        ("/delay", "tail.delay-too-high"),
    ] {
        Mock::given(path(format!("{p}/loki/api/v1/tail")))
            .respond_with(fixture(name).response())
            .mount(&server)
            .await;
    }
    let mt = client(&format!("{}/mt", server.uri()));
    let err = mt
        .tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(err.message().contains("no org id"));
    let bad = client(&format!("{}/bad", server.uri()));
    assert_eq!(
        bad.tail("{a=", |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    // client-side validation
    let c = client(&format!("{}/delay", server.uri()));
    for q in ["count_over_time({a=\"b\"}[1m])", "", "free text"] {
        assert_eq!(
            c.tail(q, |_| {}, LiveTailOptions::default())
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Validation
        );
    }
    let opts = LiveTailOptions {
        delay_for: Duration::from_secs(6),
        ..Default::default()
    };
    assert_eq!(
        c.tail(r#"{a="b"}"#, |_| {}, opts).await.unwrap_err().code(),
        ErrorCode::Validation
    );

    // unreachable server, wrong accept key, handshake timeout
    let down = client(&format!("http://127.0.0.1:{}", closed_port()));
    assert_eq!(
        down.tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Unavailable
    );
    let wrong = raw_server(
        "HTTP/1.1 101 Switching Protocols\r\nconnection: upgrade\r\nupgrade: websocket\r\nsec-websocket-accept: nope\r\n\r\n",
        Duration::from_millis(500),
    )
    .await;
    let err = client(&wrong)
        .tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
        .await
        .unwrap_err();
    assert!(err.message().contains("Sec-WebSocket-Accept"), "{err}");
    let silent = raw_server("", Duration::from_secs(5)).await;
    let slow = LokiClient::new(
        config(&silent)
            .connect_timeout(Duration::from_millis(100))
            .request_timeout(Duration::from_millis(100))
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        slow.tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Timeout
    );
}

#[tokio::test]
async fn tail_tls_and_proxy_settings() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
    let c = LokiClient::new(
        config(&format!("http://127.0.0.1:{}", closed_port()))
            .common(|b| {
                b.ca_cert_path(format!("{dir}/ca.pem"))
                    .ca_cert_pem(std::fs::read_to_string(format!("{dir}/ca.pem")).unwrap())
                    .client_cert(format!("{dir}/client.pem"), format!("{dir}/client.key"))
                    .insecure_skip_verify(true)
                    .proxy(format!("http://127.0.0.1:{}", closed_port()))
            })
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Unavailable
    );
}

#[tokio::test]
async fn log_query_tail() {
    let (url, seen) = ws_server(WsScript::Frames(vec![
        fixture("tail.categorized-labels").frame()
    ]))
    .await;
    let c = client(&url);
    let records = Arc::new(Mutex::new(Vec::new()));
    let sink = records.clone();
    let h = c
        .log_query()
        .tail(
            "marker",
            Arc::new(move |r| sink.lock().unwrap().push(r)),
            TailOptions::default(),
        )
        .await
        .unwrap();
    wait_until(|| !records.lock().unwrap().is_empty()).await;
    let r = records.lock().unwrap()[0].clone();
    assert_eq!(r.resource["service.name"], "sdk-verify");
    assert!(seen
        .lock()
        .unwrap()
        .uri
        .contains("query=%7Bservice_name%3D%7E%22.%2B%22%7D+%7C%3D+%22marker%22"));
    h.stop();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // the server ending the tail also ends the bridge task
    let (url, _) = ws_server(WsScript::FramesThenClose(vec![], 1000, String::new())).await;
    let h = client(&url)
        .log_query()
        .tail(r#"{a="b"}"#, Arc::new(|_| {}), TailOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!h.is_stopped());
    let down = client(&format!("http://127.0.0.1:{}", closed_port()));
    assert!(down
        .log_query()
        .tail("x", Arc::new(|_| {}), TailOptions::default())
        .await
        .is_err());
}
