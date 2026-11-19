//! Query API against recorded Loki responses: instant/range queries, labels, series, index
//! statistics, volume (vector and matrix) and format_query.

mod support;

use std::time::{Duration, UNIX_EPOCH};

use grow_grafana_loki as sdk;
use sdk::core::ErrorCode;
use sdk::{
    parse_unix_nano, Direction, IndexOptions, LabelsOptions, QueryOptions, QueryRangeOptions,
    SeriesOptions, VolumeAggregation, VolumeOptions, RESULT_TYPE_MATRIX, RESULT_TYPE_SCALAR,
    RESULT_TYPE_STREAMS, RESULT_TYPE_VECTOR,
};
use serde_json::json;
use support::{client, fixture};
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn t(ns: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

async fn serve(server: &MockServer, p: &str, name: &str) {
    Mock::given(path(p))
        .respond_with(fixture(name).response())
        .mount(server)
        .await;
}

async fn last(server: &MockServer) -> Request {
    server.received_requests().await.unwrap().pop().unwrap()
}

fn params(req: &Request) -> Vec<(String, String)> {
    req.url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

#[tokio::test]
async fn instant_queries() {
    let server = MockServer::start().await;
    serve(&server, "/v/loki/api/v1/query", "query.metric-vector").await;
    serve(&server, "/s/loki/api/v1/query", "query.scalar-expr").await;
    serve(&server, "/e/loki/api/v1/query", "query.parse-error").await;
    Mock::given(path("/sc/loki/api/v1/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"status":"success","data":{"resultType":"scalar","result":[1795035067.5,"2.5"]}}),
        ))
        .mount(&server)
        .await;
    let c = client(&format!("{}/v", server.uri()));
    let r = c
        .query(
            "sum by (level) (count_over_time({run=\"x\"}[10m]))",
            QueryOptions {
                time: Some(t(1_795_035_067_710_000_000)),
                limit: 10,
                direction: Some(Direction::Forward),
            },
        )
        .await
        .unwrap();
    assert_eq!(r.result_type, RESULT_TYPE_VECTOR);
    assert_eq!(r.vector.len(), 3);
    assert_eq!(r.vector[2].metric["level"], "info");
    assert_eq!(r.vector[2].value.value, 8.0);
    assert_eq!(r.vector[2].value.timestamp, t(1_795_035_067_710_000_000));
    assert!(r.stats.is_some());
    let req = last(&server).await;
    assert_eq!(
        req.headers["x-loki-response-encoding-flags"],
        "categorize-labels"
    );
    let p = params(&req);
    assert!(p.contains(&("time".into(), "1795035067710000000".into())));
    assert!(p.contains(&("limit".into(), "10".into())));
    assert!(p.contains(&("direction".into(), "forward".into())));

    let s = client(&format!("{}/s", server.uri()));
    assert_eq!(
        s.query("vector(1)+1", QueryOptions::default())
            .await
            .unwrap()
            .vector[0]
            .value
            .value,
        2.0
    );
    let sc = client(&format!("{}/sc", server.uri()));
    let r = sc.query("2.5", QueryOptions::default()).await.unwrap();
    assert_eq!(r.result_type, RESULT_TYPE_SCALAR);
    assert_eq!(r.scalar.unwrap().value, 2.5);

    let e = client(&format!("{}/e", server.uri()));
    let err = e.query("sum(", QueryOptions::default()).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("parse error"));
    // client-side validation: no request is sent
    for q in ["", "  ", "{run=\"x\"}", " {a=\"b\"} |= \"x\""] {
        assert_eq!(
            c.query(q, QueryOptions::default())
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Validation
        );
    }
}

#[tokio::test]
async fn range_queries() {
    let server = MockServer::start().await;
    serve(
        &server,
        "/f/loki/api/v1/query_range",
        "queryRange.streams-forward",
    )
    .await;
    serve(
        &server,
        "/c/loki/api/v1/query_range",
        "queryRange.categorized-labels",
    )
    .await;
    serve(
        &server,
        "/m/loki/api/v1/query_range",
        "queryRange.metric-matrix",
    )
    .await;
    serve(&server, "/e/loki/api/v1/query_range", "queryRange.empty").await;
    serve(
        &server,
        "/x/loki/api/v1/query_range",
        "queryRange.end-before-start",
    )
    .await;
    serve(
        &server,
        "/mt/loki/api/v1/query_range",
        "queryRange.mt-no-tenant",
    )
    .await;
    serve(
        &server,
        "/pipe/loki/api/v1/query_range",
        "queryRange.mt-pipe-tenants",
    )
    .await;

    let f = client(&format!("{}/f", server.uri()));
    let r = f
        .query_range(
            "{run=\"x\"}",
            QueryRangeOptions {
                start: Some(t(1_000)),
                end: Some(t(2_000)),
                step: Some(Duration::from_secs(60)),
                interval: Some(Duration::from_millis(1500)),
                limit: 100,
                direction: Some(Direction::Forward),
                since: Some(Duration::from_secs(5)),
            },
        )
        .await
        .unwrap();
    assert_eq!(r.result_type, RESULT_TYPE_STREAMS);
    assert_eq!(r.streams.len(), 2);
    assert!(r.entry_count() >= 3);
    assert_eq!(r.streams[0].labels["level"], "error");
    let p = params(&last(&server).await);
    for kv in [
        ("start", "1000"),
        ("end", "2000"),
        ("step", "60s"),
        ("interval", "1500ms"),
        ("limit", "100"),
        ("direction", "forward"),
    ] {
        assert!(p.contains(&(kv.0.into(), kv.1.into())), "{kv:?} in {p:?}");
    }
    assert!(!p.iter().any(|(k, _)| k == "since"));

    let c = client(&format!("{}/c", server.uri()));
    let r = c
        .query_range(
            "{run=\"x\"} | json",
            QueryRangeOptions {
                since: Some(Duration::from_secs(900)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.encoding_flags, vec!["categorize-labels"]);
    let e = &r.streams[0].entries[1];
    assert_eq!(e.structured_metadata["request_id"], "req-c023892b83");
    assert_eq!(e.parsed["status"], "503");
    assert!(params(&last(&server).await).contains(&("since".into(), "900s".into())));

    let m = client(&format!("{}/m", server.uri()));
    let r = m
        .query_range(
            "sum(count_over_time({a=\"b\"}[1m]))",
            QueryRangeOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(r.result_type, RESULT_TYPE_MATRIX);
    assert_eq!(r.matrix.len(), 3);
    assert_eq!(r.matrix[2].values.len(), 2);
    assert_eq!(
        r.matrix[2].values[0].timestamp,
        t(1_795_034_820_000_000_000)
    );

    let e = client(&format!("{}/e", server.uri()));
    let r = e
        .query_range("{a=\"b\"}", QueryRangeOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (r.result_type.as_str(), r.entry_count()),
        (RESULT_TYPE_STREAMS, 0)
    );

    let x = client(&format!("{}/x", server.uri()));
    assert_eq!(
        x.query_range("{a=\"b\"}", QueryRangeOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let mt = client(&format!("{}/mt", server.uri()));
    assert_eq!(
        mt.query_range("{a=\"b\"}", QueryRangeOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Authentication
    );
    let pipe = client(&format!("{}/pipe", server.uri()));
    let err = pipe
        .query_range("{a=\"b\"}", QueryRangeOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);

    // client-side checks
    assert_eq!(
        f.query_range(" ", QueryRangeOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let err = f
        .query_range(
            "{a=\"b\"}",
            QueryRangeOptions {
                start: Some(t(5)),
                end: Some(t(5)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn labels_values_series() {
    let server = MockServer::start().await;
    serve(&server, "/loki/api/v1/labels", "labels.ok").await;
    serve(
        &server,
        "/loki/api/v1/label/level/values",
        "labelValues.with-query",
    )
    .await;
    serve(
        &server,
        "/loki/api/v1/label/does_not_exist/values",
        "labelValues.unknown-label",
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/loki/api/v1/series"))
        .and(header("content-type", "application/x-www-form-urlencoded"))
        .and(body_string_contains("match%5B%5D=%7Brun%3D%22x%22%7D"))
        .respond_with(fixture("series.ok").response())
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/loki/api/v1/series"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "success"})))
        .mount(&server)
        .await;
    let c = client(&server.uri());
    let labels = c
        .labels(LabelsOptions {
            start: Some(t(1)),
            end: Some(t(2)),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(labels.contains(&"service_name".to_owned()));
    let p = params(&last(&server).await);
    assert!(p.contains(&("start".into(), "1".into())) && p.contains(&("end".into(), "2".into())));
    let vals = c
        .label_values(
            "level",
            LabelsOptions {
                since: Some(Duration::from_secs(60)),
                query: Some("{run=\"x\"}".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(vals, vec!["error", "info"]);
    let p = params(&last(&server).await);
    assert!(p.contains(&("since".into(), "60s".into())));
    assert!(p.contains(&("query".into(), "{run=\"x\"}".into())));
    assert!(c
        .label_values("does_not_exist", LabelsOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        c.label_values("bad-name", LabelsOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    let series = c
        .series(
            &["{run=\"x\"}"],
            SeriesOptions {
                since: Some(Duration::from_secs(900)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(series[0]["service_name"], "sdk-verify");
    assert!(String::from_utf8_lossy(&last(&server).await.body).contains("since=900s"));
    let none = c
        .series(
            &["{other=\"y\"}".to_owned()],
            SeriesOptions {
                start: Some(t(1)),
                end: Some(t(9)),
                since: Some(Duration::from_secs(1)),
            },
        )
        .await
        .unwrap();
    assert!(none.is_empty());
    let body = String::from_utf8_lossy(&last(&server).await.body).into_owned();
    assert!(body.contains("start=1") && body.contains("end=9") && !body.contains("since"));
    let empty: [&str; 0] = [];
    assert_eq!(
        c.series(&empty, SeriesOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn index_stats_volume_format() {
    let server = MockServer::start().await;
    serve(
        &server,
        "/loki/api/v1/index/stats",
        "indexStats.after-flush",
    )
    .await;
    Mock::given(path("/loki/api/v1/index/volume"))
        .and(query_param("targetLabels", "level,env"))
        .and(query_param("aggregateBy", "labels"))
        .and(query_param("limit", "10"))
        .respond_with(fixture("volume.target-labels").response())
        .mount(&server)
        .await;
    serve(&server, "/loki/api/v1/index/volume", "volume.ok").await;
    serve(
        &server,
        "/loki/api/v1/index/volume_range",
        "volumeRange.before-flush",
    )
    .await;
    serve(&server, "/vec/loki/api/v1/index/volume_range", "volume.ok").await;
    serve(
        &server,
        "/empty/loki/api/v1/index/volume",
        "queryRange.empty",
    )
    .await;
    serve(&server, "/loki/api/v1/format_query", "formatQuery.ok").await;
    serve(
        &server,
        "/bad/loki/api/v1/format_query",
        "formatQuery.parse-error",
    )
    .await;
    serve(
        &server,
        "/bad/loki/api/v1/index/stats",
        "indexStats.invalid-query",
    )
    .await;
    let c = client(&server.uri());

    let st = c
        .index_stats(
            "{run=\"x\"}",
            IndexOptions {
                start: Some(t(1)),
                end: Some(t(2)),
            },
        )
        .await
        .unwrap();
    assert_eq!((st.streams, st.chunks, st.bytes, st.entries), (8, 8, 0, 13));
    assert_eq!(
        c.index_stats("", IndexOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    let v = c
        .volume(
            "{run=\"x\"}",
            VolumeOptions {
                target_labels: vec!["level".into(), "env".into()],
                aggregate_by: Some(VolumeAggregation::Labels),
                limit: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(v.vector[0].metric["level"], "");
    assert_eq!(v.vector[0].value.value, 721.0);
    let v = c
        .volume(
            "{run=\"x\"}",
            VolumeOptions {
                aggregate_by: Some(VolumeAggregation::Series),
                step: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(v.vector[0].value.value, 805.0);
    let p = params(&last(&server).await);
    assert!(p.contains(&("aggregateBy".into(), "series".into())));
    assert!(!p.iter().any(|(k, _)| k == "step"), "volume ignores step");

    let vr = c
        .volume_range(
            "{run=\"x\"}",
            VolumeOptions {
                start: Some(t(1)),
                end: Some(t(2)),
                step: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(vr.result_type, RESULT_TYPE_MATRIX);
    assert_eq!(
        vr.matrix[0]
            .values
            .iter()
            .map(|p| p.value)
            .collect::<Vec<_>>(),
        vec![544.0, 74.0, 187.0]
    );
    assert!(params(&last(&server).await).contains(&("step".into(), "60s".into())));
    // Loki answers volume_range with a vector when all data falls into one step: the SDK
    // converts it into a one-point-per-series matrix
    let vec = client(&format!("{}/vec", server.uri()));
    let vr = vec
        .volume_range("{run=\"x\"}", VolumeOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (vr.result_type.as_str(), vr.vector.len(), vr.matrix.len()),
        (RESULT_TYPE_MATRIX, 0, 1)
    );
    assert_eq!(vr.matrix[0].values.len(), 1);
    // volume disabled on the server: empty result is "no data", not an error
    let empty = client(&format!("{}/empty", server.uri()));
    assert!(empty
        .volume("{run=\"x\"}", VolumeOptions::default())
        .await
        .unwrap()
        .vector
        .is_empty());
    assert_eq!(
        c.volume(" ", VolumeOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        c.volume_range("", VolumeOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    assert_eq!(
        c.format_query("{app=\"x\"} |= \"err\" | json | status >= 500")
            .await
            .unwrap(),
        "{app=\"x\"} |= \"err\" | json | status>=500"
    );
    let bad = client(&format!("{}/bad", server.uri()));
    let err = bad.format_query("{app=").await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("parse error"), "{err}");
    assert_eq!(
        bad.index_stats("{a=", IndexOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn malformed_responses() {
    let server = MockServer::start().await;
    let cases = [
        json!({"data": {"resultType": "streams", "result": [{"stream": {}, "values": [["x", "l"]]}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": {}, "values": [[1, "l"]]}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": {}, "values": [["1", 2]]}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": {}, "values": [["1"]]}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": {}, "values": [["1", "l", 5]]}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": {"a": 1}, "values": []}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": [], "values": []}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": {}, "values": 3}]}}),
        json!({"data": {"resultType": "streams", "result": {}}}),
        json!({"data": {"resultType": "vector", "result": [{"metric": {}, "value": [1]}]}}),
        json!({"data": {"resultType": "vector", "result": [{"metric": {}, "value": [true, "1"]}]}}),
        json!({"data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, 2]}]}}),
        json!({"data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "x"]}]}}),
        json!({"data": {"resultType": "matrix", "result": [{"metric": {}, "values": 1}]}}),
        json!({"data": {"resultType": "matrix", "result": [{"metric": {}, "values": [[1]]}]}}),
        json!({"data": {"resultType": "bogus", "result": []}}),
        json!({"data": 5}),
    ];
    for (i, body) in cases.iter().enumerate() {
        Mock::given(path(format!("/{i}/loki/api/v1/query_range")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
            .mount(&server)
            .await;
        let c = client(&format!("{}/{i}", server.uri()));
        let err = c
            .query_range("{a=\"b\"}", QueryRangeOptions::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::Unavailable, "case {i}: {err}");
        assert!(
            err.message().contains("invalid response"),
            "case {i}: {err}"
        );
    }
    // tolerated shapes: no data, null result, string timestamps, missing values, request id
    for (i, body) in [
        json!({"status": "success"}),
        json!({"data": {"resultType": "streams", "result": null}}),
        json!({"data": {"resultType": "matrix", "result": [{"values": [["1.5", "2"]]}, {"metric": null}]}}),
        json!({"data": {"resultType": "streams", "result": [{"stream": null}]}}),
    ]
    .iter()
    .enumerate()
    {
        Mock::given(path(format!("/ok{i}/loki/api/v1/query_range")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
            .mount(&server)
            .await;
        let c = client(&format!("{}/ok{i}", server.uri()));
        c.query_range("{a=\"b\"}", QueryRangeOptions::default()).await.unwrap();
    }
    Mock::given(path("/rid/loki/api/v1/query_range"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-request-id", "rid-1")
                .set_body_json(json!({"data": 5})),
        )
        .mount(&server)
        .await;
    let err = client(&format!("{}/rid", server.uri()))
        .query_range("{a=\"b\"}", QueryRangeOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.request_id(), Some("rid-1"));
    Mock::given(path("/nojson/loki/api/v1/labels"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-request-id", "rid-2")
                .set_body_string("<html>"),
        )
        .mount(&server)
        .await;
    let err = client(&format!("{}/nojson", server.uri()))
        .labels(LabelsOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), err.request_id()),
        (ErrorCode::Unavailable, Some("rid-2"))
    );
}

#[test]
fn unix_nano_parsing() {
    assert_eq!(
        parse_unix_nano("1795034767064713072").unwrap(),
        t(1_795_034_767_064_713_072)
    );
    assert_eq!(
        parse_unix_nano("x").unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(Direction::default().as_str(), "backward");
    assert_eq!(VolumeAggregation::Series.as_str(), "series");
}
