//! Push: JSON / gzip / protobuf+Snappy bodies, label validation, cardinality guard, errors.

mod support;

use std::collections::BTreeMap;
use std::io::Read;
use std::time::{Duration, UNIX_EPOCH};

use grow_grafana_loki as sdk;
use sdk::core::ErrorCode;
use sdk::{
    label_set_string, normalize_label_name, validate_label, CardinalityGuard, Entry, LokiClient,
    PushFormat, PushOptions, Stream,
};
use serde_json::{json, Value};
use support::{client, config, fixture};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn push_server(fixture_name: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/loki/api/v1/push"))
        .respond_with(fixture(fixture_name).response())
        .mount(&server)
        .await;
    server
}

async fn last_request(server: &MockServer) -> Request {
    server.received_requests().await.unwrap().pop().unwrap()
}

fn ts(ns: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

#[tokio::test]
async fn push_json_with_guard() {
    let server = push_server("push.json-structured-metadata").await;
    let c = client(&server.uri());
    let stream = Stream::new([("service_name", "svc"), ("level", "info"), ("run", "r1")])
        .entry(
            Entry::new("first")
                .at(ts(1_795_034_767_064_713_072))
                .metadata("request_id", "req-1"),
        )
        .entry(
            Entry::new("second")
                .at(ts(1_795_034_767_064_713_073))
                .metadata("run", "own"),
        );
    c.push(&[stream], PushOptions::default()).await.unwrap();
    let req = last_request(&server).await;
    assert_eq!(req.headers["content-type"], "application/json");
    assert!(req.headers.get("content-encoding").is_none());
    let body: Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(
        body,
        json!({"streams": [{
            "stream": {"level": "info", "service_name": "svc"},
            "values": [
                ["1795034767064713072", "first", {"request_id": "req-1", "run": "r1"}],
                ["1795034767064713073", "second", {"run": "own"}]
            ]
        }]})
    );
}

#[tokio::test]
async fn push_gzip_and_default_timestamp() {
    let server = push_server("push.json-gzip").await;
    let c = client(&server.uri());
    let before = sdk_now_ns();
    c.push(
        &[Stream::new([("service_name", "svc")]).entry(Entry::new("zipped"))],
        PushOptions::json(true),
    )
    .await
    .unwrap();
    let req = last_request(&server).await;
    assert_eq!(req.headers["content-encoding"], "gzip");
    let mut text = String::new();
    flate2::read::GzDecoder::new(&req.body[..])
        .read_to_string(&mut text)
        .unwrap();
    let body: Value = serde_json::from_str(&text).unwrap();
    let values = &body["streams"][0]["values"][0];
    assert_eq!(values.as_array().unwrap().len(), 2);
    let ns: u128 = values[0].as_str().unwrap().parse().unwrap();
    assert!(ns >= before);
}

fn sdk_now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[tokio::test]
async fn push_protobuf_snappy() {
    let server = push_server("push.protobuf-snappy").await;
    let c = client(&server.uri());
    let stream = Stream::new([("service_name", "svc"), ("env", "test")])
        .entry(
            Entry::new("proto line")
                .at(ts(1_795_034_767_064_713_072))
                .metadata("k", "v"),
        )
        .entry(Entry::new("epoch").at(UNIX_EPOCH))
        .entry(Entry::new("before").at(UNIX_EPOCH - Duration::from_millis(1500)));
    let guarded = CardinalityGuard::new(["service_name", "env"]);
    let c2 = LokiClient::new(
        config(&server.uri())
            .allowed_labels(guarded.allowed())
            .build()
            .unwrap(),
    )
    .unwrap();
    c2.push(std::slice::from_ref(&stream), PushOptions::protobuf())
        .await
        .unwrap();
    let req = last_request(&server).await;
    assert_eq!(req.headers["content-type"], "application/x-protobuf");
    let raw = snap::raw::Decoder::new().decompress_vec(&req.body).unwrap();
    // PushRequest.streams[0]
    let (field, stream_bytes, rest) = read_len_field(&raw);
    assert_eq!(field, 1);
    assert!(rest.is_empty());
    let (f, labels, mut rest) = read_len_field(stream_bytes);
    assert_eq!(f, 1);
    assert_eq!(
        std::str::from_utf8(labels).unwrap(),
        r#"{env="test", service_name="svc"}"#
    );
    let mut entries = Vec::new();
    while !rest.is_empty() {
        let (f, e, r) = read_len_field(rest);
        assert_eq!(f, 2);
        entries.push(e.to_vec());
        rest = r;
    }
    assert_eq!(entries.len(), 3);
    // Entry 1: Timestamp{seconds, nanos}, line, one label pair.
    let (f, tsb, r) = read_len_field(&entries[0]);
    assert_eq!(f, 1);
    let (secs, nanos) = read_timestamp(tsb);
    assert_eq!((secs, nanos), (1_795_034_767, 64_713_072));
    let (f, line, r) = read_len_field(r);
    assert_eq!((f, line), (2, &b"proto line"[..]));
    let (f, pair, r) = read_len_field(r);
    assert_eq!(f, 3);
    assert!(r.is_empty());
    let (_, name, r2) = read_len_field(pair);
    let (_, value, _) = read_len_field(r2);
    assert_eq!((name, value), (&b"k"[..], &b"v"[..]));
    // Entry 2: zero timestamp is an empty message.
    let (_, tsb, _) = read_len_field(&entries[1]);
    assert!(tsb.is_empty());
    // Entry 3: negative time normalized to seconds -2 + 500 ms.
    let (_, tsb, _) = read_len_field(&entries[2]);
    assert_eq!(read_timestamp(tsb), (-2, 500_000_000));

    // the plain client applies the default guard: env is not allowlisted → metadata
    c.push(
        &[stream],
        PushOptions {
            format: PushFormat::Protobuf,
            gzip: true,
        },
    )
    .await
    .unwrap();
    let raw = snap::raw::Decoder::new()
        .decompress_vec(&last_request(&server).await.body)
        .unwrap();
    let (_, sb, _) = read_len_field(&raw);
    let (_, labels, _) = read_len_field(sb);
    assert_eq!(
        std::str::from_utf8(labels).unwrap(),
        r#"{service_name="svc"}"#
    );
}

fn read_varint(b: &[u8]) -> (u64, &[u8]) {
    let (mut v, mut shift, mut i) = (0u64, 0, 0);
    loop {
        let byte = b[i];
        v |= u64::from(byte & 0x7f) << shift;
        i += 1;
        if byte < 0x80 {
            return (v, &b[i..]);
        }
        shift += 7;
    }
}

fn read_len_field(b: &[u8]) -> (u64, &[u8], &[u8]) {
    let (tag, rest) = read_varint(b);
    assert_eq!(tag & 7, 2, "length-delimited");
    let (len, rest) = read_varint(rest);
    let (data, rest) = rest.split_at(len as usize);
    (tag >> 3, data, rest)
}

fn read_timestamp(mut b: &[u8]) -> (i64, i64) {
    let (mut secs, mut nanos) = (0i64, 0i64);
    while !b.is_empty() {
        let (tag, rest) = read_varint(b);
        let (v, rest) = read_varint(rest);
        match tag >> 3 {
            1 => secs = v as i64,
            2 => nanos = v as i64,
            _ => unreachable!(),
        }
        b = rest;
    }
    (secs, nanos)
}

#[tokio::test]
async fn push_disable_guard_and_empty() {
    let server = push_server("push.json").await;
    let c = LokiClient::new(
        config(&server.uri())
            .disable_label_guard(true)
            .build()
            .unwrap(),
    )
    .unwrap();
    c.push(&[], PushOptions::default()).await.unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
    c.push(
        &[Stream::new([("run", "r1")]).entry(Entry::new("x"))],
        PushOptions::default(),
    )
    .await
    .unwrap();
    let body: Value = serde_json::from_slice(&last_request(&server).await.body).unwrap();
    assert_eq!(body["streams"][0]["stream"], json!({"run": "r1"}));
}

#[tokio::test]
async fn push_validation_errors() {
    let server = push_server("push.json").await;
    let c = LokiClient::new(
        config(&server.uri())
            .disable_label_guard(true)
            .build()
            .unwrap(),
    )
    .unwrap();
    for stream in [
        Stream::new([("bad-label", "x")]),
        Stream::new([("__reserved", "x")]),
        Stream::new([("app", "")]),
        Stream::new([("app", "x")]).entry(Entry::new("l").metadata("", "v")),
    ] {
        let err = c.push(&[stream], PushOptions::default()).await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation, "{err}");
        assert!(err.message().starts_with("push: stream 0:"));
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn push_server_errors() {
    let server = MockServer::start().await;
    for (prefix, name, code) in [
        ("/old", "push.too-old", ErrorCode::Validation),
        ("/mt", "push.mt-no-tenant", ErrorCode::Authentication),
        ("/lbl", "push.invalid-label-name", ErrorCode::Validation),
    ] {
        Mock::given(path(format!("{prefix}/loki/api/v1/push")))
            .respond_with(fixture(name).response())
            .mount(&server)
            .await;
        let c = client(&format!("{}{prefix}", server.uri()));
        let err = c
            .push(
                &[Stream::new([("service_name", "s")]).entry(Entry::new("x"))],
                PushOptions::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), code, "{name}: {err}");
    }
    // 260: ingestion blocked by the administrator
    Mock::given(path("/blocked/loki/api/v1/push"))
        .respond_with(ResponseTemplate::new(260).insert_header("x-request-id", "rq-1"))
        .mount(&server)
        .await;
    Mock::given(path("/blocked2/loki/api/v1/push"))
        .respond_with(ResponseTemplate::new(260))
        .mount(&server)
        .await;
    for p in ["/blocked", "/blocked2"] {
        let c = client(&format!("{}{p}", server.uri()));
        let err = c
            .push(
                &[Stream::new([("service_name", "s")]).entry(Entry::new("x"))],
                PushOptions::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Unavailable);
        assert!(!err.is_retryable());
        assert_eq!(err.http_status(), Some(260));
    }
    // Ingester is shutting down → retried (idempotent push), then retryable UNAVAILABLE
    Mock::given(path("/wal/loki/api/v1/push"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_string("rpc error: code = Unknown desc = Ingester is shutting down"),
        )
        .expect(2)
        .mount(&server)
        .await;
    let c = LokiClient::new(
        config(&format!("{}/wal", server.uri()))
            .retry(support::fast_retry(1))
            .build()
            .unwrap(),
    )
    .unwrap();
    let err = c
        .push(
            &[Stream::new([("service_name", "s")]).entry(Entry::new("x"))],
            PushOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.is_retryable());
    // Loki's own ingestion 429
    Mock::given(path("/rate/loki/api/v1/push"))
        .respond_with(ResponseTemplate::new(429).set_body_string(
            "Ingestion rate limit exceeded for user fake (limit: 4194304 bytes/sec) while attempting to ingest '1' lines totaling '10' bytes, reduce log volume or contact your Loki administrator to see if the limit can be increased",
        ))
        .mount(&server)
        .await;
    let c = client(&format!("{}/rate", server.uri()));
    let err = c
        .push(
            &[Stream::new([("service_name", "s")]).entry(Entry::new("x"))],
            PushOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::RateLimited);
    assert!(err.message().contains("Ingestion rate limit exceeded"));
}

#[tokio::test]
async fn push_tenant_header() {
    let server = MockServer::start().await;
    Mock::given(path("/loki/api/v1/push"))
        .and(header("X-Scope-OrgID", "tenant-a"))
        .respond_with(fixture("push.mt-tenant").response())
        .mount(&server)
        .await;
    let c = LokiClient::new(config(&server.uri()).tenant_id("tenant-a").build().unwrap()).unwrap();
    c.push(
        &[Stream::new([("service_name", "s")]).entry(Entry::new("x"))],
        PushOptions::default(),
    )
    .await
    .unwrap();
}

#[test]
fn label_helpers() {
    assert!(validate_label("app_1", "x").is_ok());
    assert!(validate_label("_x", "x").is_ok());
    for (n, v) in [
        ("1app", "x"),
        ("a-b", "x"),
        ("", "x"),
        ("__name__", "x"),
        ("a", ""),
    ] {
        assert_eq!(
            validate_label(n, v).unwrap_err().code(),
            ErrorCode::Validation
        );
    }
    assert_eq!(normalize_label_name("service.name"), "service_name");
    assert_eq!(normalize_label_name("k8s.pod-name"), "k8s_pod_name");
    assert_eq!(normalize_label_name("1abc"), "_1abc");
    assert_eq!(normalize_label_name("ü"), "__");
    assert_eq!(normalize_label_name(""), "");
    let mut m = BTreeMap::new();
    m.insert("b".to_owned(), "x\"y".to_owned());
    m.insert("a".to_owned(), "1".to_owned());
    assert_eq!(label_set_string(&m), r#"{a="1", b="x\"y"}"#);
    assert_eq!(label_set_string(&BTreeMap::new()), "{}");
}

#[test]
fn cardinality_guard() {
    let g = CardinalityGuard::default();
    assert!(g.is_allowed("service_name") && g.is_allowed("pod"));
    assert!(!g.is_allowed("run"));
    assert_eq!(g.allowed().len(), 10);
    let attrs = sdk::attrs! {
        "service.name" => "svc",
        "level" => "",
        "user.id" => 7,
        "big" => u64::MAX,
        "ratio" => 1.5,
        "flag" => true,
        "list" => serde_json::json!([1, "a"]),
        "nothing" => serde_json::Value::Null,
        "" => "skipped",
        "%" => "odd",
    };
    let (labels, md) = g.split(&attrs);
    assert_eq!(
        labels,
        BTreeMap::from([("service_name".to_owned(), "svc".to_owned())])
    );
    assert_eq!(md["level"], "");
    assert_eq!(md["user_id"], "7");
    assert_eq!(md["big"], u64::MAX.to_string());
    assert_eq!(md["ratio"], "1.5");
    assert_eq!(md["flag"], "true");
    assert_eq!(md["list"], r#"[1,"a"]"#);
    assert_eq!(md["nothing"], "");
    assert_eq!(md["_"], "odd");
    assert!(!md.contains_key(""));

    let s = Stream::new([("service_name", "a"), ("run", "r")])
        .entry(Entry::new("x"))
        .entry(Entry::new("y").metadata("run", "mine"));
    let out = g.apply(&s);
    assert_eq!(out.labels.len(), 1);
    assert_eq!(out.entries[0].structured_metadata["run"], "r");
    assert_eq!(out.entries[1].structured_metadata["run"], "mine");
}
