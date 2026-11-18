// Taken over from sdk-kit 0.1.0 (header removed): the skeleton tests targeted the demo
// service and its `token` option; these test the Loki client surface and configuration.
//! `LokiConfig` and the core `LokiClient` functions against recorded Loki responses.

mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use grow_grafana_loki as sdk;
use sdk::core::{EnvReader, ErrorCode};
use sdk::{LokiClient as Client, LokiConfig as Config, DEFAULT_ALLOWED_LABELS};
use serde_json::json;
use support::{client, closed_port, config, fixture};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn config_from_env_precedence() {
    let env = EnvReader::from_map([
        ("GROW_GRAFANA_LOKI_URL", "http://grow:3100"),
        ("LOKI_URL", "http://vendor:3100"),
        ("LOKI_TENANT_ID", "team-a"),
        ("LOKI_BASIC_AUTH_USER", "platform-user"),
        ("LOKI_BASIC_AUTH_PASSWORD", "platform-pass"),
    ]);
    let c = Config::builder().build_with_env(&env).unwrap();
    assert_eq!(c.url().as_str(), "http://grow:3100/");
    assert_eq!(c.tenant_id(), Some("team-a"));
    assert_eq!(c.username(), Some("platform-user"));
    assert_eq!(c.password().unwrap().expose_secret(), "platform-pass");
    assert!(c.bearer_token().is_none());

    let env = EnvReader::from_map([
        ("LOKI_URL", "http://vendor:3100"),
        ("LOKI_USERNAME", "u1"),
        ("LOKI_BASIC_AUTH_USER", "u2"),
        ("GROW_GRAFANA_LOKI_TENANT_ID", "grow-tenant"),
        ("LOKI_TENANT_ID", "vendor-tenant"),
        ("GROW_GRAFANA_LOKI_PASSWORD", "p-grow"),
        ("LOKI_PASSWORD", "p-vendor"),
    ]);
    let c = Config::builder()
        .tenant_id("explicit")
        .build_with_env(&env)
        .unwrap();
    assert_eq!(c.url().as_str(), "http://vendor:3100/");
    assert_eq!(c.tenant_id(), Some("explicit"));
    assert_eq!(c.username(), Some("u1"));
    assert_eq!(c.password().unwrap().expose_secret(), "p-grow");

    let env = EnvReader::from_map([("LOKI_BEARER_TOKEN", "tok-1234")]);
    let c = Config::builder().build_with_env(&env).unwrap();
    assert_eq!(c.url().as_str(), "http://127.0.0.1:3100/");
    assert_eq!(c.bearer_token().unwrap().expose_secret(), "tok-1234");
    let debug = format!("{c:?}");
    assert!(debug.contains("[REDACTED]") && !debug.contains("tok-1234"));
    let _ = Config::from_env();
    let _ = Client::from_env();
}

#[test]
fn config_validation() {
    let err = |b: sdk::LokiConfigBuilder| b.build().unwrap_err();
    for b in [
        Config::builder().username("u"),
        Config::builder().password("p"),
        Config::builder()
            .username("u")
            .password("p")
            .bearer_token("t"),
        Config::builder().tenant_id(""),
        Config::builder().tenant_id("bad/tenant"),
        Config::builder().tenant_id("."),
        Config::builder().tenant_id(".."),
        Config::builder().tenant_id("x".repeat(151)),
        Config::builder().tenant_id("a|b"),
        Config::builder()
            .tenant_id("a|b/c")
            .multi_tenant_queries(true),
        Config::builder().allowed_labels(["bad-label"]),
        Config::builder().url("mailto:x"),
    ] {
        assert_eq!(err(b).code(), ErrorCode::Config);
    }
    let msg = err(Config::builder().tenant_id("bad/tenant"));
    assert!(msg.message().contains("unsupported character '/'"), "{msg}");
    let ok = Config::builder()
        .tenant_id("a|b")
        .multi_tenant_queries(true)
        .allowed_labels(["app", "env"])
        .disable_label_guard(true)
        .build()
        .unwrap();
    assert!(ok.multi_tenant_queries() && ok.disable_label_guard());
    assert_eq!(ok.allowed_labels(), vec!["app", "env"]);
    let d = Config::builder()
        .tenant_id("Team_1-(x)*'!.")
        .build()
        .unwrap();
    assert_eq!(d.allowed_labels().len(), DEFAULT_ALLOWED_LABELS.len());
    assert_eq!(d.common().url().as_str(), "http://127.0.0.1:3100/");
    let cfg = Config::builder()
        .ca_cert_path("/nonexistent/ca.pem")
        .build()
        .unwrap();
    assert_eq!(Client::new(cfg).unwrap_err().code(), ErrorCode::Config);
}

#[test]
fn config_builder_common_settings() {
    use std::time::Duration;
    let c = Config::builder()
        .connect_timeout(Duration::from_secs(1))
        .request_timeout(Duration::from_secs(2))
        .total_timeout(Duration::from_secs(3))
        .max_retries(5)
        .tls(sdk::core::TlsConfig::default())
        .insecure_skip_verify(false)
        .proxy("http://proxy:3128")
        .header("x-extra", "1")
        .user_agent_suffix("it")
        .hooks(Arc::new(NoHooks))
        .common(|c| c.max_retries(4))
        .retry(sdk::RetryPolicy::default())
        .build()
        .unwrap();
    assert_eq!(c.common().connect_timeout(), Duration::from_secs(1));
    assert_eq!(c.common().proxy(), Some("http://proxy:3128"));
}

struct NoHooks;
impl sdk::core::Hooks for NoHooks {}

#[tokio::test]
async fn health_ready_ping_version() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/ready"))
        .respond_with(fixture("ready.ok").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/labels"))
        .and(query_param("since", "5m"))
        .and(header("X-Scope-OrgID", "tenant-a"))
        .respond_with(fixture("ping.ok-tenant").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/labels"))
        .respond_with(fixture("ping.no-tenant").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/status/buildinfo"))
        .respond_with(fixture("buildInfo.ok").response())
        .mount(&server)
        .await;
    let c = client(&server.uri());
    let h = c.health().await.unwrap();
    assert!(h.healthy);
    assert_eq!(h.status, "ok");
    assert_eq!(h.details["httpStatus"], "200");
    assert_eq!(h.details["message"], "ready");
    assert!(c.ready().await.unwrap());
    let err = c.ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(err.message().contains("no org id"), "{err}");
    let t = c.with_tenant("tenant-a").unwrap();
    assert_eq!(t.tenant_id(), Some("tenant-a"));
    assert!(c.tenant_id().is_none());
    t.ping().await.unwrap();
    let info = c.build_info().await.unwrap();
    assert_eq!(info.version, "3.7.10");
    assert!(!info.revision.is_empty());
    assert_eq!(c.server_version().await.unwrap(), "3.7.10");
    assert!(format!("{c:?}").contains("LokiClient"));
    assert!(c.guard().is_allowed("service_name"));
}

#[tokio::test]
async fn health_and_ready_variants() {
    let server = MockServer::start().await;
    Mock::given(path("/a/ready"))
        .respond_with(
            ResponseTemplate::new(503)
                .set_body_string("Ingester not ready: waiting for 15s after being ready\n"),
        )
        .mount(&server)
        .await;
    Mock::given(path("/b/ready"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(path("/c/ready"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    Mock::given(path("/d/ready"))
        .respond_with(fixture("ping.gateway-bad-credentials").response())
        .mount(&server)
        .await;
    let a = client(&format!("{}/a", server.uri()));
    let h = a.health().await.unwrap();
    assert!(!h.healthy);
    assert!(h.status.starts_with("Ingester not ready"));
    assert_eq!(h.details["httpStatus"], "503");
    assert!(!a.ready().await.unwrap());
    let b = client(&format!("{}/b", server.uri()));
    assert_eq!(b.health().await.unwrap().status, "unhealthy");
    assert_eq!(b.ready().await.unwrap_err().code(), ErrorCode::Unavailable);
    let c = client(&format!("{}/c", server.uri()));
    // only HTTP 200 counts as ready (same as the other languages)
    assert_eq!(c.health().await.unwrap().status, "unhealthy");
    assert_eq!(c.ready().await.unwrap_err().code(), ErrorCode::Unavailable);
    let d = client(&format!("{}/d", server.uri()));
    assert_eq!(d.health().await.unwrap().status, "unauthorized");
    let err = d.ready().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(err.message().contains("unauthorized"));
}

#[tokio::test]
async fn health_unreachable_and_closed() {
    let c = client(&format!("http://127.0.0.1:{}", closed_port()));
    let h = c.health().await.unwrap();
    assert!(!h.healthy);
    assert_eq!(h.status, "unreachable");
    assert!(h.details.contains_key("error"));
    assert_eq!(c.ready().await.unwrap_err().code(), ErrorCode::Unavailable);
    c.close().await;
    assert_eq!(c.health().await.unwrap_err().code(), ErrorCode::Cancelled);
}

#[tokio::test]
async fn build_info_errors() {
    let server = MockServer::start().await;
    Mock::given(path("/a/loki/api/v1/status/buildinfo"))
        .respond_with(fixture("http.not-found").response())
        .mount(&server)
        .await;
    Mock::given(path("/b/loki/api/v1/status/buildinfo"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;
    Mock::given(path("/c/loki/api/v1/status/buildinfo"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;
    let a = client(&format!("{}/a", server.uri()));
    assert_eq!(
        a.server_version().await.unwrap_err().code(),
        ErrorCode::Unsupported
    );
    let b = client(&format!("{}/b", server.uri()));
    let err = b.build_info().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(!err.is_retryable());
    let c = client(&format!("{}/c", server.uri()));
    assert_eq!(
        c.build_info().await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
}

#[tokio::test]
async fn auth_headers_are_sent_and_redacted() {
    let server = MockServer::start().await;
    // base64("user:s3cret-pass")
    Mock::given(path("/loki/api/v1/labels"))
        .and(header("authorization", "Basic dXNlcjpzM2NyZXQtcGFzcw=="))
        .respond_with(fixture("ping.gateway-basic-ok").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/labels"))
        .and(header("authorization", "Bearer bearer-token-1"))
        .respond_with(fixture("ping.gateway-basic-ok").response())
        .mount(&server)
        .await;
    Mock::given(path("/loki/api/v1/labels"))
        .respond_with(fixture("ping.gateway-bad-credentials").response())
        .mount(&server)
        .await;
    let basic = Client::new(
        config(&server.uri())
            .username("user")
            .password("s3cret-pass")
            .build()
            .unwrap(),
    )
    .unwrap();
    basic.ping().await.unwrap();
    let bearer = Client::new(
        config(&server.uri())
            .bearer_token("bearer-token-1")
            .build()
            .unwrap(),
    )
    .unwrap();
    bearer.ping().await.unwrap();
    // the derived tenant client keeps the credentials
    bearer.with_tenant("t1").unwrap().ping().await.unwrap();
    let wrong = Client::new(
        config(&server.uri())
            .username("user")
            .password("wrong-pass-123")
            .build()
            .unwrap(),
    )
    .unwrap();
    let err = wrong.ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(!err.message().contains("wrong-pass-123"));
    assert!(!format!("{wrong:?}").contains("wrong-pass-123"));
}

#[tokio::test]
async fn with_tenant_validation_and_close() {
    let server = MockServer::start().await;
    Mock::given(path("/loki/api/v1/labels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "success"})))
        .mount(&server)
        .await;
    let c = client(&server.uri());
    for bad in ["", "a|b", "bad/tenant", ".."] {
        assert_eq!(
            c.with_tenant(bad).unwrap_err().code(),
            ErrorCode::Validation
        );
    }
    let multi = Client::new(
        config(&server.uri())
            .multi_tenant_queries(true)
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(multi.with_tenant("a|b").unwrap().tenant_id(), Some("a|b"));

    let derived = c.with_tenant("t1").unwrap();
    let closed = Arc::new(AtomicU32::new(0));
    let counter = closed.clone();
    derived.on_close(move || {
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
        })
    });
    derived.close().await;
    derived.close().await;
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    assert!(derived.is_closed());
    assert_eq!(
        derived.ping().await.unwrap_err().code(),
        ErrorCode::Cancelled
    );
    // the parent keeps working: derived clients do not own the connections
    assert!(!c.is_closed());
    c.ping().await.unwrap();
    let other = c.with_tenant("t2").unwrap();
    c.close().await;
    assert!(c.transport().is_closed());
    assert!(other.is_closed());
    assert_eq!(c.ping().await.unwrap_err().code(), ErrorCode::Cancelled);
}

#[tokio::test]
async fn ingester_shutting_down_is_retryable_unavailable() {
    let server = MockServer::start().await;
    Mock::given(path("/loki/api/v1/labels"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_string("rpc error: code = Unknown desc = Ingester is shutting down\n"),
        )
        .expect(2)
        .mount(&server)
        .await;
    let c = Client::new(
        config(&server.uri())
            .retry(support::fast_retry(1))
            .build()
            .unwrap(),
    )
    .unwrap();
    let err = c.ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.is_retryable());
    assert!(err.message().contains("WAL disk"), "{err}");
}

#[tokio::test]
async fn gateway_faults_map_to_typed_errors() {
    let server = MockServer::start().await;
    for (fault, code) in [
        ("429", ErrorCode::RateLimited),
        ("503", ErrorCode::Unavailable),
        ("502", ErrorCode::Unavailable),
    ] {
        let f = fixture(&format!("transport.gateway-fault-{fault}"));
        let mut resp = f.response();
        if fault != "502" {
            resp = resp.insert_header("retry-after", "1");
        }
        Mock::given(path(format!("/{fault}/loki/api/v1/labels")))
            .respond_with(resp)
            .mount(&server)
            .await;
        let c = client(&format!("{}/{fault}", server.uri()));
        let err = c.labels(Default::default()).await.unwrap_err();
        assert_eq!(err.code(), code, "{fault}: {err}");
        if fault != "502" {
            assert_eq!(err.retry_after(), Some(std::time::Duration::from_secs(1)));
        }
    }
}
