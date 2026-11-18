// Taken over from sdk-kit 0.1.0 (header removed): the skeleton test targeted the demo service
// and its `token` option; this one drives every blocking method against Loki mocks.
//! Blocking client (feature `blocking`).
#![cfg(feature = "blocking")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use grow_grafana_loki as sdk;
use sdk::blocking::LokiClient as Client;
use sdk::core::ErrorCode;
use sdk::logging::{LogClient, LogEntry, SearchOptions, TailOptions, WaitOptions};
use sdk::{
    Entry, IndexOptions, LabelsOptions, LiveTailOptions, LogClientOptions, LokiConfig as Config,
    PushExporter, PushFormat, PushOptions, QueryOptions, QueryRangeOptions, SeriesOptions, Stream,
    VolumeOptions,
};
use support::{fixture, ws_server, WsScript};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn mock_server(rt: &tokio::runtime::Runtime) -> MockServer {
    rt.block_on(async {
        let server = MockServer::start().await;
        for (m, p, f) in [
            ("GET", "/ready", "ready.ok"),
            ("GET", "/loki/api/v1/labels", "labels.ok"),
            ("GET", "/loki/api/v1/status/buildinfo", "buildInfo.ok"),
            ("POST", "/loki/api/v1/push", "push.json"),
            ("GET", "/loki/api/v1/query", "query.metric-vector"),
            (
                "GET",
                "/loki/api/v1/query_range",
                "queryRange.streams-forward",
            ),
            (
                "GET",
                "/loki/api/v1/label/level/values",
                "labelValues.with-query",
            ),
            ("POST", "/loki/api/v1/series", "series.ok"),
            ("GET", "/loki/api/v1/index/stats", "indexStats.after-flush"),
            ("GET", "/loki/api/v1/index/volume", "volume.ok"),
            (
                "GET",
                "/loki/api/v1/index/volume_range",
                "volumeRange.before-flush",
            ),
            ("GET", "/loki/api/v1/format_query", "formatQuery.ok"),
        ] {
            Mock::given(method(m))
                .and(path(p))
                .respond_with(fixture(f).response())
                .mount(&server)
                .await;
        }
        server
    })
}

#[test]
fn blocking_client() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = mock_server(&rt);
    let cfg = Config::builder()
        .url(server.uri())
        .retry(support::fast_retry(0))
        .build()
        .unwrap();
    let c = Client::new(cfg).unwrap();
    assert!(c.health().unwrap().healthy);
    assert!(c.ready().unwrap());
    c.ping().unwrap();
    assert_eq!(c.server_version().unwrap(), "3.7.10");
    assert_eq!(c.build_info().unwrap().version, "3.7.10");
    assert!(c.config().tenant_id().is_none());
    let t = c.with_tenant("t1").unwrap();
    assert_eq!(t.tenant_id(), Some("t1"));
    t.ping().unwrap();
    drop(t);
    c.push(
        &[Stream::new([("service_name", "x")]).entry(Entry::new("line"))],
        PushOptions::default(),
    )
    .unwrap();
    assert_eq!(
        c.query(
            "sum(count_over_time({a=\"b\"}[1m]))",
            QueryOptions::default()
        )
        .unwrap()
        .vector
        .len(),
        3
    );
    assert!(
        c.query_range("{a=\"b\"}", QueryRangeOptions::default())
            .unwrap()
            .entry_count()
            > 0
    );
    assert!(!c.labels(LabelsOptions::default()).unwrap().is_empty());
    assert_eq!(
        c.label_values("level", LabelsOptions::default()).unwrap(),
        vec!["error", "info"]
    );
    assert!(!c
        .series(&["{a=\"b\"}"], SeriesOptions::default())
        .unwrap()
        .is_empty());
    assert!(
        c.index_stats("{a=\"b\"}", IndexOptions::default())
            .unwrap()
            .entries
            > 0
    );
    assert_eq!(
        c.volume("{a=\"b\"}", VolumeOptions::default())
            .unwrap()
            .vector
            .len(),
        1
    );
    assert_eq!(
        c.volume_range("{a=\"b\"}", VolumeOptions::default())
            .unwrap()
            .matrix
            .len(),
        1
    );
    assert!(c.format_query("{a=\"b\"}").unwrap().contains("status>=500"));

    let lq = c.log_query();
    assert!(!lq
        .search("{a=\"b\"}", SearchOptions::default())
        .unwrap()
        .records
        .is_empty());
    assert!(!lq.list_streams().unwrap().is_empty());
    assert!(!lq.list_labels().unwrap().is_empty());
    let rec = lq
        .wait_for_log(
            "{a=\"b\"}",
            WaitOptions::default().timeout(Duration::from_secs(5)),
        )
        .unwrap();
    assert!(!rec.body.is_empty());

    let lc = c.new_log_client(LogClientOptions::default()).unwrap();
    lc.send(LogEntry::from("blocking line"));
    c.block_on(lc.flush(None)).unwrap();
    assert_eq!(lc.stats().sent, 1);
    let oc = c.new_otlp_log_client(LogClientOptions::default()).unwrap();
    let pc = c
        .new_log_client_with_exporter(
            Arc::new(PushExporter::new(c.async_client(), PushFormat::Protobuf)),
            LogClientOptions::default(),
        )
        .unwrap();
    assert_eq!(oc.stats().sent + pc.stats().sent, 0);
    assert!(!c.async_client().is_closed());
    assert_eq!(c.block_on(async { 7 }), 7);
    c.close();
    c.close();
    assert_eq!(c.ping().unwrap_err().code(), ErrorCode::Cancelled);
    let _ = Client::from_env();
    let bad = Config::builder()
        .ca_cert_path("/nonexistent")
        .build()
        .unwrap();
    assert_eq!(Client::new(bad).unwrap_err().code(), ErrorCode::Config);
}

#[test]
fn blocking_tail() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let frame = fixture("tail.ok").frame();
    let (url, _) = rt.block_on(ws_server(WsScript::Frames(vec![frame.clone()])));
    let c = Client::new(Config::builder().url(url).build().unwrap()).unwrap();
    let got = Arc::new(Mutex::new(Vec::new()));
    let sink = got.clone();
    let s = c
        .tail(
            "{a=\"b\"}",
            move |f| sink.lock().unwrap().push(f),
            LiveTailOptions::default(),
        )
        .unwrap();
    for _ in 0..100 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(got.lock().unwrap().len(), 1);
    assert!(!s.is_done());
    s.stop();
    s.wait();
    assert!(s.is_done());
    assert!(s.err().is_none());
    assert_eq!(s.dropped_count(), 0);

    let (url, _) = rt.block_on(ws_server(WsScript::Frames(vec![frame])));
    let c = Client::new(Config::builder().url(url).build().unwrap()).unwrap();
    let got = Arc::new(Mutex::new(Vec::new()));
    let sink = got.clone();
    let h = c
        .log_query()
        .tail(
            "{a=\"b\"}",
            Arc::new(move |r| sink.lock().unwrap().push(r)),
            TailOptions::default(),
        )
        .unwrap();
    for _ in 0..100 {
        if !got.lock().unwrap().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(got.lock().unwrap()[0].body, "tail marker c023892b83");
    h.stop();
    c.close();
    drop(rt);
    let _ = ResponseTemplate::new(200);
}
