//! Logging category: exporters, batching log clients, search with cursors, record mapping,
//! streams and labels.

mod support;

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use grow_grafana_loki as sdk;
use sdk::core::ErrorCode;
use sdk::logging::{
    BatchOptions, ExportOptions, LogClient, LogEntry, LogExporter, LogInput, LogQueryClient,
    LogRecord, SearchOptions, Severity, SortOrder, WaitOptions,
};
use sdk::{
    entry_to_record, records_to_streams, CardinalityGuard, LogClientOptions, OtlpExporter,
    PushExporter, PushFormat, StreamEntry,
};
use serde_json::{json, Value};
use support::{client, fixture};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn t(ns: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

fn body_json(req: &Request) -> Value {
    if req
        .headers
        .get("content-encoding")
        .map(|v| v.to_str().unwrap())
        == Some("gzip")
    {
        let mut s = String::new();
        flate2::read::GzDecoder::new(&req.body[..])
            .read_to_string(&mut s)
            .unwrap();
        serde_json::from_str(&s).unwrap()
    } else {
        serde_json::from_slice(&req.body).unwrap()
    }
}

fn record() -> LogRecord {
    let mut r = LogRecord::new(Severity::Error, "boom")
        .with_attribute("request.id", "r-1")
        .with_attribute("code", 503)
        .with_attribute("empty", "")
        .with_trace("4BF92F3577B34DA6A3CE929D0E0E4736", "00F067AA0BA902B7");
    r.timestamp = t(1_795_035_007_690_130_926);
    r.observed_timestamp = Some(t(1_795_035_007_690_130_927));
    r.trace_flags = Some(1);
    r.resource = sdk::attrs! {
        "service.name" => "svc",
        "deployment.environment" => "test",
        "service.version" => "1.0.0",
    };
    r
}

#[test]
fn records_to_streams_mapping() {
    let mut warn = LogRecord::new(Severity::Warn, "w");
    warn.severity_text = String::new();
    warn.resource = sdk::attrs! {"service.name" => "svc"};
    let mut leveled = LogRecord::new(Severity::Info, "own level").with_attribute("level", "custom");
    leveled.resource = sdk::attrs! {"service.name" => "svc"};
    let streams = records_to_streams(
        &[record(), warn.clone(), leveled],
        &CardinalityGuard::default(),
    );
    assert_eq!(streams.len(), 3);
    let err_stream = streams
        .iter()
        .find(|s| s.labels.get("level").map(String::as_str) == Some("error"))
        .unwrap();
    assert_eq!(
        err_stream.labels,
        BTreeMap::from([
            ("deployment_environment".to_owned(), "test".to_owned()),
            ("level".to_owned(), "error".to_owned()),
            ("service_name".to_owned(), "svc".to_owned()),
        ])
    );
    let md = &err_stream.entries[0].structured_metadata;
    assert_eq!(md["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(md["span_id"], "00f067aa0ba902b7");
    assert_eq!(md["trace_flags"], "1");
    assert_eq!(md["severity_text"], "ERROR");
    assert_eq!(md["severity_number"], "17");
    assert_eq!(md["observed_timestamp"], "1795035007690130927");
    assert_eq!(md["request_id"], "r-1");
    assert_eq!(md["code"], "503");
    assert_eq!(md["service_version"], "1.0.0");
    assert!(!md.contains_key("empty"));
    assert_eq!(err_stream.entries[0].line, "boom");
    let warn_stream = streams
        .iter()
        .find(|s| s.labels.get("level").map(String::as_str) == Some("warn"))
        .unwrap();
    assert_eq!(
        warn_stream.entries[0].structured_metadata["severity_text"],
        "WARN"
    );
    assert!(streams
        .iter()
        .any(|s| s.labels.get("level").map(String::as_str) == Some("custom")));
    // without "level" in the allowlist no level label is added
    let s = records_to_streams(&[warn], &CardinalityGuard::new(["service_name"]));
    assert_eq!(s[0].labels.len(), 1);
}

#[tokio::test]
async fn push_exporter_and_log_client() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/loki/api/v1/push"))
        .respond_with(fixture("push.json").response())
        .mount(&server)
        .await;
    let c = client(&server.uri());
    let exporter = PushExporter::new(&c, PushFormat::Json);
    assert!(format!("{exporter:?}").contains("PushExporter"));
    assert_eq!(
        exporter
            .export(&[], ExportOptions::default())
            .await
            .unwrap()
            .accepted,
        0
    );
    let r = exporter
        .export(&[record()], ExportOptions::new(false))
        .await
        .unwrap();
    assert_eq!((r.accepted, r.rejected), (1, 0));
    let body = body_json(&server.received_requests().await.unwrap()[0]);
    assert_eq!(body["streams"][0]["stream"]["level"], "error");

    let lc = c
        .new_log_client(
            LogClientOptions::default()
                .resource_attribute("service.name", "lc-svc")
                .batch(BatchOptions::default().flush_interval(Duration::from_secs(60))),
        )
        .unwrap();
    lc.send(LogEntry::Input(
        LogInput::new("hello")
            .severity(Severity::Warn)
            .attribute("password", "hunter2"),
    ));
    lc.flush(None).await.unwrap();
    assert_eq!(lc.stats().sent, 1);
    let reqs = server.received_requests().await.unwrap();
    let body = body_json(reqs.last().unwrap());
    assert_eq!(reqs.last().unwrap().headers["content-encoding"], "gzip");
    assert_eq!(body["streams"][0]["stream"]["service_name"], "lc-svc");
    assert_eq!(body["streams"][0]["values"][0][2]["password"], "[REDACTED]");
    // close of the LokiClient flushes queued records
    lc.send(LogEntry::from("on close"));
    let before = server.received_requests().await.unwrap().len();
    c.close().await;
    assert_eq!(server.received_requests().await.unwrap().len(), before + 1);
}

#[tokio::test]
async fn export_failures() {
    let server = MockServer::start().await;
    Mock::given(path("/bad/loki/api/v1/push"))
        .respond_with(fixture("push.too-old").response())
        .mount(&server)
        .await;
    Mock::given(path("/down/loki/api/v1/push"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    Mock::given(path("/bad/otlp/v1/logs"))
        .respond_with(fixture("otlpLogs.empty-json").response())
        .mount(&server)
        .await;
    Mock::given(path("/down/otlp/v1/logs"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let bad = client(&format!("{}/bad", server.uri()));
    let r = PushExporter::new(&bad, PushFormat::Protobuf)
        .export(&[record()], ExportOptions::default())
        .await
        .unwrap();
    assert_eq!((r.accepted, r.rejected), (0, 1));
    assert_eq!(r.error.unwrap().code(), ErrorCode::Validation);
    let r = OtlpExporter::new(&bad)
        .export(&[record()], ExportOptions::default())
        .await
        .unwrap();
    assert_eq!(r.rejected, 1);
    let down = client(&format!("{}/down", server.uri()));
    let err = PushExporter::new(&down, PushFormat::Json)
        .export(&[record()], ExportOptions::default())
        .await
        .unwrap_err();
    assert!(err.is_retryable());
    let err = OtlpExporter::new(&down)
        .export(&[record()], ExportOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn otlp_exporter_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/otlp/v1/logs"))
        .respond_with(fixture("otlpLogs.json").response())
        .mount(&server)
        .await;
    let c = client(&server.uri());
    let exp = OtlpExporter::new(&c);
    assert!(format!("{exp:?}").contains("OtlpExporter"));
    assert_eq!(
        exp.export(&[], ExportOptions::default())
            .await
            .unwrap()
            .accepted,
        0
    );
    let mut other = LogRecord::new(Severity::Info, "other");
    other.severity_text = String::new();
    other.attributes = sdk::attrs! {
        "b" => true, "f" => 1.5, "arr" => json!([1, "x"]), "map" => json!({"k": "v"}),
        "null" => Value::Null, "big" => u64::MAX,
    };
    let r = exp
        .export(
            &[record(), other.clone(), record()],
            ExportOptions::new(true),
        )
        .await
        .unwrap();
    assert_eq!(r.accepted, 3);
    let body = body_json(&server.received_requests().await.unwrap()[0]);
    let rls = body["resourceLogs"].as_array().unwrap();
    assert_eq!(rls.len(), 2, "grouped by resource");
    let first = &rls[0];
    assert_eq!(
        first["resource"]["attributes"][0],
        json!({"key": "deployment.environment", "value": {"stringValue": "test"}})
    );
    let scope = &first["scopeLogs"][0];
    assert_eq!(scope["scope"]["name"], "grow-codes-grafana-loki");
    let logs = scope["logRecords"].as_array().unwrap();
    assert_eq!(logs.len(), 2);
    let lr = &logs[0];
    assert_eq!(lr["timeUnixNano"], "1795035007690130926");
    assert_eq!(lr["observedTimeUnixNano"], "1795035007690130927");
    assert_eq!(lr["severityNumber"], 17);
    assert_eq!(lr["severityText"], "ERROR");
    assert_eq!(lr["body"]["stringValue"], "boom");
    assert_eq!(lr["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(lr["spanId"], "00f067aa0ba902b7");
    assert_eq!(lr["flags"], 1);
    assert!(lr["attributes"]
        .as_array()
        .unwrap()
        .contains(&json!({"key": "code", "value": {"intValue": "503"}})));
    let attrs = rls[1]["scopeLogs"][0]["logRecords"][0]["attributes"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        rls[1]["scopeLogs"][0]["logRecords"][0]["severityText"],
        "INFO"
    );
    for want in [
        json!({"key": "b", "value": {"boolValue": true}}),
        json!({"key": "f", "value": {"doubleValue": 1.5}}),
        json!({"key": "arr", "value": {"arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "x"}]}}}),
        json!({"key": "map", "value": {"kvlistValue": {"values": [{"key": "k", "value": {"stringValue": "v"}}]}}}),
        json!({"key": "big", "value": {"intValue": u64::MAX.to_string()}}),
    ] {
        assert!(attrs.contains(&want), "{want} in {attrs:?}");
    }
    assert!(!attrs.iter().any(|a| a["key"] == "null"));
    // arrays with nulls render them as empty strings
    let mut n = LogRecord::new(Severity::Info, "n");
    n.attributes = sdk::attrs! {"a" => json!([null])};
    exp.export(&[n], ExportOptions::new(false)).await.unwrap();
    let body = body_json(server.received_requests().await.unwrap().last().unwrap());
    assert_eq!(
        body["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"][0]["value"]
            ["arrayValue"]["values"][0],
        json!({"stringValue": ""})
    );

    let oc = c.new_otlp_log_client(LogClientOptions::default()).unwrap();
    oc.send(LogEntry::from("via client"));
    oc.flush(None).await.unwrap();
    assert_eq!(oc.stats().sent, 1);
    let pc = c
        .new_log_client_with_exporter(
            Arc::new(PushExporter::new(&c, PushFormat::Protobuf)),
            LogClientOptions::default(),
        )
        .unwrap();
    assert_eq!(pc.stats().sent, 0);
}

#[test]
fn entry_mapping() {
    let labels = BTreeMap::from([
        ("service_name".to_owned(), "svc".to_owned()),
        ("deployment_environment".to_owned(), "test".to_owned()),
        ("host_name".to_owned(), "h1".to_owned()),
        ("custom".to_owned(), "c".to_owned()),
        ("level".to_owned(), "warn".to_owned()),
    ]);
    let entry = |md: &[(&str, &str)], parsed: &[(&str, &str)]| StreamEntry {
        timestamp: t(5),
        line: "line".into(),
        structured_metadata: md
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        parsed: parsed
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let r = entry_to_record(
        &labels,
        &entry(
            &[
                ("trace_id", "ABC"),
                ("span_id", "DEF"),
                ("flags", "1"),
                ("observed_timestamp", "7"),
                ("severity_number", "17"),
                ("severity_text", "ERR"),
                ("detected_level", "error"),
                ("request_id", "r1"),
                ("status", "meta wins"),
            ],
            &[("status", "503"), ("msg", "m")],
        ),
    );
    assert_eq!(r.severity, Severity::Error);
    assert_eq!(r.severity_text, "ERR");
    assert_eq!(r.trace_id.as_deref(), Some("abc"));
    assert_eq!(r.span_id.as_deref(), Some("def"));
    assert_eq!(r.trace_flags, Some(1));
    assert_eq!(r.observed_timestamp, Some(t(7)));
    assert_eq!(r.resource["service.name"], "svc");
    assert_eq!(r.resource["deployment.environment"], "test");
    assert_eq!(r.resource["host.name"], "h1");
    assert_eq!(r.resource["custom"], "c");
    assert_eq!(r.attributes["status"], "meta wins");
    assert_eq!(r.attributes["msg"], "m");
    assert_eq!(r.attributes["request_id"], "r1");
    assert!(!r.attributes.contains_key("detected_level"));
    assert_eq!(r.body, "line");
    // fallbacks: detected_level, then the level label, then INFO
    let r = entry_to_record(
        &labels,
        &entry(
            &[("detected_level", "debug"), ("severity_number", "99")],
            &[],
        ),
    );
    assert_eq!(
        (r.severity, r.severity_text.as_str()),
        (Severity::Debug, "DEBUG")
    );
    let r = entry_to_record(&labels, &entry(&[("detected_level", "unknown")], &[]));
    assert_eq!(r.severity, Severity::Warn);
    let r = entry_to_record(
        &BTreeMap::new(),
        &entry(&[("trace_flags", "x"), ("observed_timestamp", "bad")], &[]),
    );
    assert_eq!(r.severity, Severity::Info);
    assert!(r.trace_flags.is_none() && r.observed_timestamp.is_none() && r.trace_id.is_none());
    let r = entry_to_record(&BTreeMap::new(), &entry(&[("severity_text", "5")], &[]));
    assert_eq!(
        (r.severity, r.severity_text.as_str()),
        (Severity::Info, "5")
    );
}

#[tokio::test]
async fn search_order_and_cursors() {
    let server = MockServer::start().await;
    Mock::given(path("/loki/api/v1/query_range"))
        .respond_with(fixture("queryRange.streams-forward").response())
        .mount(&server)
        .await;
    let c = client(&server.uri());
    let lq = c.log_query();
    assert!(format!("{lq:?}").contains("LogQuery"));
    let all = lq
        .search(
            r#"{run="c023892b83"}"#,
            SearchOptions::default().order(SortOrder::Asc),
        )
        .await
        .unwrap();
    assert!(all.records.len() >= 3);
    assert!(all.next_cursor.is_none());
    assert!(all
        .records
        .windows(2)
        .all(|w| w[0].timestamp <= w[1].timestamp));
    let p: Vec<(String, String)> = server.received_requests().await.unwrap()[0]
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(p.contains(&("direction".into(), "forward".into())));
    assert!(p.contains(&("limit".into(), "100".into())));

    let desc = lq
        .search(r#"{run="c023892b83"}"#, SearchOptions::default().limit(0))
        .await
        .unwrap();
    assert!(desc
        .records
        .windows(2)
        .all(|w| w[0].timestamp >= w[1].timestamp));

    // pages of one record: the cursor resumes after the last record
    let n = all.records.len();
    let mut seen = Vec::new();
    let mut cursor = None;
    for _ in 0..n {
        let mut opts = SearchOptions::default().limit(1).order(SortOrder::Asc);
        opts.cursor = cursor.take();
        let page = lq.search(r#"{run="c023892b83"}"#, opts).await.unwrap();
        seen.extend(page.records);
        cursor = page.next_cursor;
    }
    assert_eq!(seen.len(), n);
    let reqs = server.received_requests().await.unwrap();
    let last: Vec<(String, String)> = reqs
        .last()
        .unwrap()
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(last.iter().any(|(k, _)| k == "start") && last.iter().any(|(k, _)| k == "end"));

    // invalid cursors are VALIDATION; a descending cursor sets an exclusive end
    let err = lq
        .search("x", SearchOptions::default().cursor("%%"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let page = lq
        .search("x", SearchOptions::default().limit(1))
        .await
        .unwrap();
    let next = lq
        .search(
            "x",
            SearchOptions::default()
                .limit(1)
                .cursor(page.next_cursor.unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(next.records.len(), 1);
    let r = server.received_requests().await.unwrap();
    let q: Vec<(String, String)> = r
        .last()
        .unwrap()
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(q.contains(&("query".into(), r#"{service_name=~".+"} |= "x""#.into())));
    assert!(q.contains(&("limit".into(), "2".into())));
}

#[tokio::test]
async fn streams_labels_wait() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/loki/api/v1/series"))
        .respond_with(fixture("series.ok").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/labels"))
        .respond_with(fixture("labels.ok").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/query_range"))
        .respond_with(fixture("queryRange.otlp-record").response())
        .mount(&server)
        .await;
    let lq = client(&server.uri()).log_query();
    let streams = lq.list_streams().await.unwrap();
    assert!(streams.windows(2).all(|w| w[0] <= w[1]));
    assert!(streams
        .iter()
        .any(|s| s.contains(r#"service_name="sdk-verify""#)));
    let req = &server.received_requests().await.unwrap()[0];
    let body = String::from_utf8_lossy(&req.body);
    assert!(body.contains("since=3600s"));
    let labels = lq.list_labels().await.unwrap();
    assert!(labels.contains(&"deployment_environment".to_owned()));
    let r = lq
        .wait_for_log(
            "otlp",
            WaitOptions::default().timeout(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert_eq!(r.severity, Severity::Error);
    assert_eq!(r.resource["service.name"], "otlp-json-c023892b83");
    assert_eq!(r.attributes["request_id"], "otlp-c023892b83");
    assert_eq!(r.trace_flags, Some(1));
    let err = lq
        .wait_for_log(
            "x",
            WaitOptions::default()
                .timeout(Duration::from_millis(200))
                .interval(Duration::from_millis(50))
                .predicate(|_| false),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);
}
