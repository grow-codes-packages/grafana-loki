//! Integration suite against the real Loki (Docker): every public function is called at least
//! once with a round-trip assertion.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use grow_grafana_loki as sdk;
use grow_grafana_loki_integration::{configured, env, run_id};
use sdk::blocking::LokiClient as BlockingClient;
use sdk::core::{Hooks, RequestInfo, ResponseInfo};
use sdk::logging::{
    BatchOptions, LogClient, LogEntry, LogInput, LogQueryClient, LogRecord, SearchOptions,
    Severity, SortOrder, TailOptions, WaitOptions,
};
use sdk::{
    Entry, ErrorCode, IndexOptions, LabelsOptions, LiveTailOptions, LogClientOptions, LogQL,
    LokiClient as Client, LokiConfig as Config, PushExporter, PushFormat, PushOptions,
    QueryOptions, QueryRangeOptions, QueryResult, RetryPolicy, SeriesOptions, Stream, TailFrame,
    VolumeAggregation, VolumeOptions, RESULT_TYPE_MATRIX, RESULT_TYPE_VECTOR,
};

async fn must_range(c: &Client, q: &str, min: usize) -> QueryResult {
    let deadline = SystemTime::now() + Duration::from_secs(30);
    loop {
        let res = c
            .query_range(
                q,
                QueryRangeOptions {
                    since: Some(Duration::from_secs(900)),
                    limit: 1000,
                    direction: Some(sdk::Direction::Forward),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("queryRange {q}: {e}"));
        if res.entry_count() >= min || SystemTime::now() > deadline {
            assert!(
                res.entry_count() >= min,
                "queryRange {q}: {} entries, want >= {min}",
                res.entry_count()
            );
            return res;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn flush(c: &Client) {
    c.transport()
        .post("/flush")
        .send()
        .await
        .expect("POST /flush");
}

async fn wait_until(what: &str, f: impl Fn() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{what}: timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_push_and_query() {
    if !configured() {
        return;
    }
    let c = Client::from_env().expect("client");
    let run = run_id();
    let svc = format!("grow-it-rust-{run}");

    // Core: health, ready, ping, version, build info.
    let hs = c.health().await.unwrap();
    assert!(hs.healthy && hs.status == "ok", "{hs:?}");
    assert!(c.ready().await.unwrap());
    c.ping().await.unwrap();
    let want = env("GROW_GRAFANA_LOKI_EXPECTED_VERSION");
    let v = c.server_version().await.unwrap();
    assert!(want.is_empty() || v == want, "version {v} want {want}");
    let bi = c.build_info().await.unwrap();
    assert!(!bi.version.is_empty() && !bi.revision.is_empty());

    // Push: JSON (guard moves "run" to structured metadata), gzip and protobuf+snappy.
    let now = SystemTime::now();
    let ms = Duration::from_millis(1);
    let labels = [
        ("service_name", svc.as_str()),
        ("level", "info"),
        ("run", run.as_str()),
    ];
    let nasty = format!("quote\" back\\slash tab\t ü 😀 {run}");
    c.push(
        &[Stream::new(labels)
            .entry(
                Entry::new(format!(r#"{{"msg":"json line {run}","status":503}}"#))
                    .at(now)
                    .metadata("request_id", format!("req-{run}")),
            )
            .entry(Entry::new(format!(r#"{{"msg":"json ok {run}","status":200}}"#)).at(now + ms))
            .entry(Entry::new(nasty.clone()).at(now + 2 * ms))],
        PushOptions::default(),
    )
    .await
    .expect("push json");
    c.push(
        &[Stream::new(labels).entry(Entry::new(format!("gzip line {run}")).at(now + 3 * ms))],
        PushOptions::json(true),
    )
    .await
    .expect("push gzip");
    c.push(
        &[
            Stream::new([("service_name", svc.as_str()), ("level", "error")]).entry(
                Entry::new(format!("proto line {run}"))
                    .at(now + 4 * ms)
                    .metadata("run", run.clone())
                    .metadata("enc", "protobuf"),
            ),
        ],
        PushOptions::protobuf(),
    )
    .await
    .expect("push protobuf");
    let sel = LogQL::selector([("service_name", svc.as_str())]);
    let res = must_range(&c, &sel.to_string(), 5).await;
    let (mut got_proto, mut got_meta) = (false, false);
    for s in &res.streams {
        assert!(
            !s.labels.contains_key("run"),
            "run must not be an index label: {:?}",
            s.labels
        );
        for e in &s.entries {
            if e.line == format!("proto line {run}")
                && e.structured_metadata.get("enc").map(String::as_str) == Some("protobuf")
                && s.labels.get("level").map(String::as_str) == Some("error")
            {
                got_proto = true;
            }
            if e.structured_metadata.get("request_id") == Some(&format!("req-{run}"))
                && e.structured_metadata.get("run") == Some(&run)
            {
                got_meta = true;
            }
        }
    }
    assert!(
        got_proto && got_meta,
        "round trip proto={got_proto} meta={got_meta}"
    );

    // LogQL builder: escaping, json + where, structured metadata filter; format_query parses it.
    let q = sel
        .line_contains("json line")
        .json()
        .where_("status", ">=", 500)
        .build()
        .unwrap();
    let formatted = c.format_query(&q).await.unwrap();
    assert!(formatted.contains("status>=500"), "{formatted}");
    let r = must_range(&c, &q, 1).await;
    assert_eq!(r.entry_count(), 1);
    assert_eq!(r.streams[0].entries[0].parsed["status"], "503");
    let r = must_range(&c, &sel.line_contains(&nasty).to_string(), 1).await;
    assert_eq!(r.streams[0].entries[0].line, nasty);
    let r = must_range(
        &c,
        &sel.where_("run", "=", run.as_str())
            .where_("request_id", "=", format!("req-{run}"))
            .to_string(),
        1,
    )
    .await;
    assert_eq!(r.entry_count(), 1);
    let r = c
        .query_range(
            &sel.to_string(),
            QueryRangeOptions {
                since: Some(Duration::from_secs(900)),
                limit: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.entry_count(), 2);
    assert_eq!(
        c.format_query("{service_name=").await.unwrap_err().code(),
        ErrorCode::Validation
    );

    // Metric queries.
    let mq = format!("sum(count_over_time({sel}[10m]))");
    let inst = c.query(&mq, QueryOptions::default()).await.unwrap();
    assert_eq!(inst.result_type, RESULT_TYPE_VECTOR);
    assert_eq!(inst.vector.len(), 1);
    assert!(inst.vector[0].value.value >= 5.0);
    assert_eq!(
        c.query(&sel.to_string(), QueryOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let mat = c
        .query_range(
            &mq,
            QueryRangeOptions {
                since: Some(Duration::from_secs(600)),
                step: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (mat.result_type.as_str(), mat.matrix.len()),
        (RESULT_TYPE_MATRIX, 1)
    );

    // Labels, label values, series.
    let since = LabelsOptions {
        since: Some(Duration::from_secs(900)),
        ..Default::default()
    };
    let ls = c.labels(since.clone()).await.unwrap();
    assert!(ls.contains(&"service_name".to_owned()) && !ls.contains(&"request_id".to_owned()));
    let vals = c
        .label_values(
            "level",
            LabelsOptions {
                query: Some(sel.to_string()),
                ..since.clone()
            },
        )
        .await
        .unwrap();
    assert!(
        vals.contains(&"error".to_owned()) && vals.contains(&"info".to_owned()),
        "{vals:?}"
    );
    let series = c
        .series(
            &[sel.to_string()],
            SeriesOptions {
                since: Some(Duration::from_secs(900)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(series.len(), 2);
    assert_eq!(series[0]["service_name"], svc);

    // Volume (ingester data), then flush chunks for the index-only APIs.
    let window = VolumeOptions {
        start: Some(now - Duration::from_secs(600)),
        end: Some(SystemTime::now() + Duration::from_secs(60)),
        ..Default::default()
    };
    let vol = c
        .volume(
            &sel.to_string(),
            VolumeOptions {
                target_labels: vec!["level".into()],
                aggregate_by: Some(VolumeAggregation::Labels),
                ..window.clone()
            },
        )
        .await
        .unwrap();
    assert!(
        !vol.vector.is_empty() && vol.vector[0].value.value > 0.0,
        "volume {vol:?}"
    );
    let vr = c
        .volume_range(
            &sel.to_string(),
            VolumeOptions {
                step: Some(Duration::from_secs(60)),
                ..window.clone()
            },
        )
        .await
        .unwrap();
    assert!(
        vr.result_type == "matrix" && !vr.matrix.is_empty() && vr.vector.is_empty(),
        "volume range {vr:?}"
    );
    flush(&c).await;
    let mut st = sdk::IndexStats::default();
    for _ in 0..40 {
        st = c
            .index_stats(
                &sel.to_string(),
                IndexOptions {
                    start: window.start,
                    end: Some(SystemTime::now() + Duration::from_secs(60)),
                },
            )
            .await
            .unwrap();
        if st.entries >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(st.streams >= 1 && st.entries >= 5, "index stats {st:?}");

    // Errors.
    let err = c
        .push(
            &[Stream::new(labels)
                .entry(Entry::new("old").at(now - Duration::from_secs(30 * 86400)))],
            PushOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation, "{err}");
    let err = c
        .query_range("{service_name=", QueryRangeOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    c.close().await;
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail() {
    if !configured() {
        return;
    }
    let c = Client::from_env().unwrap();
    let run = run_id();
    let svc = format!("grow-it-tail-rust-{run}");
    let sel = LogQL::selector([("service_name", svc.as_str())]).to_string();

    let frames: Arc<Mutex<Vec<TailFrame>>> = Arc::default();
    let sink = frames.clone();
    let s = c
        .tail(
            &sel,
            move |f| sink.lock().unwrap().push(f),
            LiveTailOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.push(
        &[Stream::new([("service_name", svc.as_str())])
            .entry(Entry::new(format!("tail marker {run}")).metadata("k", "v"))],
        PushOptions::default(),
    )
    .await
    .unwrap();
    wait_until("tail frame", || !frames.lock().unwrap().is_empty()).await;
    {
        let f = frames.lock().unwrap()[0].clone();
        let e = &f.streams[0].entries[0];
        assert_eq!(e.line, format!("tail marker {run}"));
        assert_eq!(e.structured_metadata["k"], "v");
        assert_eq!(f.streams[0].labels["service_name"], svc);
    }
    s.stop().await;
    assert!(s.err().is_none(), "{:?}", s.err());

    // log_query().tail (category interface) with a free-text query.
    let recs = Arc::new(Mutex::new(Vec::new()));
    let sink = recs.clone();
    let h = c
        .log_query()
        .tail(
            &format!("category tail {run}"),
            Arc::new(move |r| sink.lock().unwrap().push(r)),
            TailOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.push(
        &[
            Stream::new([("service_name", svc.as_str()), ("level", "warn")])
                .entry(Entry::new(format!("category tail {run}"))),
        ],
        PushOptions::default(),
    )
    .await
    .unwrap();
    wait_until("tail record", || !recs.lock().unwrap().is_empty()).await;
    {
        let r = recs.lock().unwrap()[0].clone();
        assert_eq!(r.body, format!("category tail {run}"));
        assert_eq!(r.resource["service.name"], svc);
        assert_eq!(r.severity, Severity::Warn);
    }
    h.stop();

    // Loki closes a metric-query tail; the SDK rejects it before dialing.
    let err = c
        .tail(
            &format!("count_over_time({sel}[1m])"),
            |_| {},
            LiveTailOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let err = c
        .tail("{service_name=", |_| {}, LiveTailOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation, "{err}");
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logging_category() {
    if !configured() {
        return;
    }
    let c = Client::from_env().unwrap();
    let run = run_id();
    let svc = format!("grow-it-log-rust-{run}");
    let lq = c.log_query();
    let opts = || {
        LogClientOptions::default()
            .resource_attribute("service.name", svc.clone())
            .resource_attribute("deployment.environment", "it")
            .resource_attribute("service.version", "1.0.0")
    };

    // Push exporter (JSON): send, send_batch, logger facade, flush, search back.
    let lc = c.new_log_client(opts()).unwrap();
    lc.send(LogEntry::Input(
        LogInput::new(format!("send {run}"))
            .severity(Severity::Error)
            .attribute("run", run.clone())
            .attribute("n", 7)
            .attribute("password", "hunter2")
            .trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"),
    ));
    lc.send_batch(vec![
        LogEntry::from(format!("batch-0 {run}")),
        LogEntry::from(format!("batch-1 {run}")),
    ]);
    lc.logger(Some("it"), sdk::attrs! {"component" => "logger"})
        .child(sdk::attrs! {"child" => true})
        .warn(format!("logger {run}"), sdk::attrs! {"k" => "v"});
    lc.flush(None).await.unwrap();
    let st = lc.stats();
    assert_eq!((st.sent, st.failed), (4, 0), "{st:?}");
    let mut found = Vec::new();
    for _ in 0..60 {
        found = lq
            .search(&run, SearchOptions::default())
            .await
            .unwrap()
            .records;
        if found.len() >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(found.len(), 4);
    let sent = found
        .iter()
        .find(|r| r.body == format!("send {run}"))
        .expect("send record");
    assert_eq!(sent.severity, Severity::Error);
    assert_eq!(
        sent.trace_id.as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    assert_eq!(sent.span_id.as_deref(), Some("00f067aa0ba902b7"));
    assert_eq!(sent.attributes["n"], "7");
    assert_eq!(sent.attributes["password"], "[REDACTED]");
    assert_eq!(sent.resource["service.name"], svc.as_str());
    assert_eq!(sent.resource["deployment.environment"], "it");
    assert_eq!(sent.attributes["service_version"], "1.0.0");
    let logged = found
        .iter()
        .find(|r| r.body == format!("logger {run}"))
        .expect("logger record");
    assert_eq!(logged.severity, Severity::Warn);
    assert_eq!(logged.attributes["logger_name"], "it");

    // Search order + pagination with cursors, wait_for_log with a predicate.
    let sel = LogQL::selector([("service_name", svc.as_str())]).to_string();
    let page = lq
        .search(
            &sel,
            SearchOptions::default().limit(2).order(SortOrder::Asc),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert!(page.records[0].timestamp <= page.records[1].timestamp);
    let mut all = page.records.clone();
    let mut cursor = page.next_cursor.clone();
    while let Some(cur) = cursor.take() {
        let p = lq
            .search(
                &sel,
                SearchOptions::default()
                    .limit(2)
                    .order(SortOrder::Asc)
                    .cursor(cur),
            )
            .await
            .unwrap();
        all.extend(p.records);
        cursor = p.next_cursor;
    }
    assert_eq!(all.len(), 4);
    let r = lq
        .wait_for_log(
            &run,
            WaitOptions::default()
                .timeout(Duration::from_secs(10))
                .predicate(|r| r.body.starts_with("logger")),
        )
        .await
        .unwrap();
    assert_eq!(r.attributes["k"], "v");

    // OTLP exporter and protobuf push exporter.
    let oc = c.new_otlp_log_client(opts()).unwrap();
    oc.send(LogEntry::Input(
        LogInput::new(format!("otlp {run}"))
            .severity(Severity::Warn)
            .attribute("request.id", format!("r-{run}"))
            .attribute("code", 503)
            .trace("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"),
    ));
    let pc = c
        .new_log_client_with_exporter(
            Arc::new(PushExporter::new(&c, PushFormat::Protobuf)),
            opts(),
        )
        .unwrap();
    pc.send(LogEntry::from(format!("protobuf exporter {run}")));
    oc.flush(None).await.unwrap();
    pc.flush(None).await.unwrap();
    assert_eq!((oc.stats().sent, pc.stats().sent), (1, 1));
    let otlp = lq
        .wait_for_log(
            &format!(r#"{{service_name="{svc}"}} | trace_id="0af7651916cd43dd8448eb211c80319c""#),
            WaitOptions::default()
                .timeout(Duration::from_secs(30))
                .interval(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    assert_eq!(otlp.body, format!("otlp {run}"));
    assert_eq!(otlp.attributes["request_id"], format!("r-{run}").as_str());
    assert_eq!(otlp.severity, Severity::Warn);
    assert_eq!(otlp.span_id.as_deref(), Some("b7ad6b7169203331"));
    lq.wait_for_log(&format!("protobuf exporter {run}"), WaitOptions::default())
        .await
        .unwrap();
    // push_otlp: the direct form of the OTLP exporter
    let mut direct = LogRecord::new(Severity::Info, format!("push otlp {run}"));
    direct
        .resource
        .insert("service.name".into(), svc.as_str().into());
    c.push_otlp(&[direct], true).await.unwrap();
    lq.wait_for_log(&format!("push otlp {run}"), WaitOptions::default())
        .await
        .unwrap();

    // Streams and labels.
    let streams = lq.list_streams().await.unwrap();
    assert!(streams
        .iter()
        .any(|s| s.contains(&format!(r#"service_name="{svc}""#))));
    let labels = lq.list_labels().await.unwrap();
    assert!(labels.contains(&"deployment_environment".to_owned()));
    let err = lq
        .wait_for_log(
            &format!("never-{run}"),
            WaitOptions::default()
                .timeout(Duration::from_millis(700))
                .interval(Duration::from_millis(200)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);

    // Close flushes queued records.
    let slow = c
        .new_log_client(
            opts().batch(BatchOptions::default().flush_interval(Duration::from_secs(60))),
        )
        .unwrap();
    slow.send(LogEntry::from(format!("on close {run}")));
    c.close().await;
    c.close().await;
    let c2 = Client::from_env().unwrap();
    c2.log_query()
        .wait_for_log(&format!("on close {run}"), WaitOptions::default())
        .await
        .expect("close must flush");
    c2.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_tenant() {
    if !configured() {
        return;
    }
    let (mt, ta, tb) = (
        env("GROW_GRAFANA_LOKI_MULTI_TENANT_URL"),
        env("GROW_GRAFANA_LOKI_TENANT_A"),
        env("GROW_GRAFANA_LOKI_TENANT_B"),
    );
    if mt.is_empty() || ta.is_empty() || tb.is_empty() {
        eprintln!("multi-tenant Loki not configured");
        return;
    }
    let run = run_id();
    let c = Client::new(Config::builder().url(&mt).build().unwrap()).unwrap();
    assert!(c.health().await.unwrap().healthy);
    assert_eq!(
        c.ping().await.unwrap_err().code(),
        ErrorCode::Authentication
    );
    let svc = format!("mt-{run}");
    let stream = [Stream::new([("service_name", svc.as_str())])
        .entry(Entry::new(format!("tenant line {run}")))];
    assert_eq!(
        c.push(&stream, PushOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Authentication
    );
    let a = c.with_tenant(&ta).unwrap();
    let b = c.with_tenant(&tb).unwrap();
    a.push(&stream, PushOptions::default()).await.unwrap();
    let sel = format!(r#"{{service_name="{svc}"}}"#);
    must_range(&a, &sel, 1).await;
    let r = b
        .query_range(
            &sel,
            QueryRangeOptions {
                since: Some(Duration::from_secs(900)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.entry_count(), 0, "tenant B must not see tenant A data");
    let vals = b
        .label_values(
            "service_name",
            LabelsOptions {
                since: Some(Duration::from_secs(900)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!vals.contains(&svc));
    // Tenant header on the tail handshake.
    let frames: Arc<Mutex<Vec<TailFrame>>> = Arc::default();
    let sink = frames.clone();
    let s = a
        .tail(
            &sel,
            move |f| sink.lock().unwrap().push(f),
            LiveTailOptions {
                start: Some(SystemTime::now() - Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    wait_until("tenant tail", || !frames.lock().unwrap().is_empty()).await;
    s.stop().await;
    assert_eq!(
        c.tail(&sel, |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Authentication
    );
    assert_eq!(
        c.with_tenant(&format!("{ta}|{tb}")).unwrap_err().code(),
        ErrorCode::Validation
    );
    let err = Config::builder()
        .url(&mt)
        .tenant_id("bad/tenant")
        .build()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Config);
    let configured_tenant =
        Client::new(Config::builder().url(&mt).tenant_id(&ta).build().unwrap()).unwrap();
    assert_eq!(configured_tenant.tenant_id(), Some(ta.as_str()));
    configured_tenant.ping().await.unwrap();
    c.close().await;
}

#[derive(Default)]
struct Recorder {
    auth: Mutex<Vec<String>>,
    responses: AtomicU32,
}

impl Hooks for Recorder {
    fn on_request(&self, info: &RequestInfo) {
        if let Some(v) = info.headers.get("authorization") {
            self.auth.lock().unwrap().push(v.clone());
        }
    }
    fn on_response(&self, _info: &ResponseInfo) {
        self.responses.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_auth_and_faults() {
    if !configured() {
        return;
    }
    let gw = env("GROW_GRAFANA_LOKI_AUTH_PROXY_URL");
    if gw.is_empty() {
        eprintln!("auth gateway not configured");
        return;
    }
    let (user, pass, bearer) = (
        env("GROW_GRAFANA_LOKI_AUTH_PROXY_USERNAME"),
        env("GROW_GRAFANA_LOKI_AUTH_PROXY_PASSWORD"),
        env("GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN"),
    );
    let basic = Client::new(
        Config::builder()
            .url(&gw)
            .username(&user)
            .password(pass.as_str())
            .build()
            .unwrap(),
    )
    .unwrap();
    basic.ping().await.expect("basic");
    let run = run_id();
    let bc = Client::new(
        Config::builder()
            .url(&gw)
            .bearer_token(bearer.as_str())
            .build()
            .unwrap(),
    )
    .unwrap();
    bc.push(
        &[Stream::new([("service_name", format!("gw-{run}"))])
            .entry(Entry::new(format!("gateway {run}")))],
        PushOptions::json(true),
    )
    .await
    .expect("bearer push");
    must_range(&bc, &format!(r#"{{service_name="gw-{run}"}}"#), 1).await;
    // Tail through the gateway with bearer credentials.
    let frames: Arc<Mutex<Vec<TailFrame>>> = Arc::default();
    let sink = frames.clone();
    let s = bc
        .tail(
            &format!(r#"{{service_name="gw-{run}"}}"#),
            move |f| sink.lock().unwrap().push(f),
            LiveTailOptions {
                start: Some(SystemTime::now() - Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .expect("gateway tail");
    wait_until("gateway tail", || !frames.lock().unwrap().is_empty()).await;
    s.stop().await;

    let rec = Arc::new(Recorder::default());
    let wrong_pass = format!("wrong-{run}");
    let wrong = Client::new(
        Config::builder()
            .url(&gw)
            .username(&user)
            .password(wrong_pass.as_str())
            .hooks(rec.clone())
            .build()
            .unwrap(),
    )
    .unwrap();
    let err = wrong.ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(!err.message().contains(&wrong_pass));
    let seen = rec.auth.lock().unwrap().clone();
    assert!(
        !seen.is_empty() && seen.iter().all(|v| v == "[REDACTED]"),
        "{seen:?}"
    );
    let anon = Client::new(Config::builder().url(&gw).build().unwrap()).unwrap();
    assert_eq!(
        anon.tail(r#"{a="b"}"#, |_| {}, LiveTailOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Authentication
    );

    // Fault injection: 429 / 503 with Retry-After are retried, then surface typed errors.
    let retry = RetryPolicy {
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
        max_retry_after: Duration::from_secs(5),
        ..RetryPolicy::default()
    }
    .with_max_retries(1);
    for (fault, want) in [
        ("429", ErrorCode::RateLimited),
        ("503", ErrorCode::Unavailable),
        ("502", ErrorCode::Unavailable),
    ] {
        let rec = Arc::new(Recorder::default());
        let fc = Client::new(
            Config::builder()
                .url(&gw)
                .bearer_token(bearer.as_str())
                .retry(retry.clone())
                .header("X-Grow-Fault", fault)
                .hooks(rec.clone())
                .build()
                .unwrap(),
        )
        .unwrap();
        let err = fc.labels(LabelsOptions::default()).await.unwrap_err();
        assert_eq!(err.code(), want, "fault {fault}: {err}");
        assert_eq!(
            rec.responses.load(Ordering::SeqCst),
            2,
            "fault {fault}: attempts"
        );
        if fault != "502" {
            assert_eq!(
                err.retry_after(),
                Some(Duration::from_secs(1)),
                "fault {fault}"
            );
        }
    }
}

#[test]
fn blocking_round_trip() {
    if !configured() {
        return;
    }
    let c = BlockingClient::from_env().expect("client");
    assert!(c.health().unwrap().healthy);
    assert!(c.ready().unwrap());
    c.ping().unwrap();
    assert!(!c.server_version().unwrap().is_empty());
    assert!(!c.build_info().unwrap().revision.is_empty());
    let run = run_id();
    let svc = format!("grow-it-blocking-{run}");
    let sel = LogQL::selector([("service_name", svc.as_str())]).to_string();
    let frames: Arc<Mutex<Vec<TailFrame>>> = Arc::default();
    let sink = frames.clone();
    let tail = c
        .tail(
            &sel,
            move |f| sink.lock().unwrap().push(f),
            LiveTailOptions::default(),
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    c.push(
        &[
            Stream::new([("service_name", svc.as_str()), ("level", "info")])
                .entry(Entry::new(format!("blocking {run}")).metadata("k", "v")),
        ],
        PushOptions::protobuf(),
    )
    .unwrap();
    let mut res = c.query_range(&sel, QueryRangeOptions::default()).unwrap();
    for _ in 0..50 {
        if res.entry_count() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
        res = c.query_range(&sel, QueryRangeOptions::default()).unwrap();
    }
    assert_eq!(res.streams[0].entries[0].structured_metadata["k"], "v");
    for _ in 0..100 {
        if !frames.lock().unwrap().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        !frames.lock().unwrap().is_empty(),
        "blocking tail got nothing"
    );
    tail.stop();
    assert!(tail.is_done() && tail.err().is_none());
    let mq = format!("sum(count_over_time({sel}[5m]))");
    assert_eq!(
        c.query(&mq, QueryOptions::default()).unwrap().vector.len(),
        1
    );
    let since = LabelsOptions {
        since: Some(Duration::from_secs(900)),
        ..Default::default()
    };
    assert!(c
        .labels(since.clone())
        .unwrap()
        .contains(&"service_name".to_owned()));
    assert!(c
        .label_values("service_name", since)
        .unwrap()
        .contains(&svc));
    let series: Vec<BTreeMap<String, String>> =
        c.series(&[sel.as_str()], SeriesOptions::default()).unwrap();
    assert_eq!(series.len(), 1);
    let _ = c.index_stats(&sel, IndexOptions::default()).unwrap();
    // Volume is eventually consistent: a stream that query_range already returns can still be
    // missing from /index/volume for a moment (seen under load), so poll like the Python suite.
    let mut vol = c.volume(&sel, VolumeOptions::default()).unwrap();
    for _ in 0..50 {
        if !vol.vector.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
        vol = c.volume(&sel, VolumeOptions::default()).unwrap();
    }
    assert!(!vol.vector.is_empty(), "volume {vol:?}");
    let _ = c
        .volume_range(
            &sel,
            VolumeOptions {
                step: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(c.format_query(&sel).unwrap().contains(&svc));
    let lq = c.log_query();
    assert_eq!(
        lq.search(&sel, SearchOptions::default())
            .unwrap()
            .records
            .len(),
        1
    );
    assert!(lq
        .list_labels()
        .unwrap()
        .contains(&"service_name".to_owned()));
    assert!(!lq.list_streams().unwrap().is_empty());
    let lc = c
        .new_log_client(LogClientOptions::default().resource_attribute("service.name", svc.clone()))
        .unwrap();
    lc.send(LogEntry::from(format!("blocking log client {run}")));
    c.block_on(lc.flush(None)).unwrap();
    let r = lq
        .wait_for_log(
            &format!("blocking log client {run}"),
            WaitOptions::default(),
        )
        .unwrap();
    assert_eq!(r.resource["service.name"], svc.as_str());
    c.close();
    c.close();
    assert_eq!(c.ping().unwrap_err().code(), ErrorCode::Cancelled);
}
