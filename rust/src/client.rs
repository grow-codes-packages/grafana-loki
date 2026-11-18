// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! `LokiClient`: entry point of the SDK (SPEC §2.4).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures::future::BoxFuture;
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::config::{tenant_problem, LokiConfig};
use crate::core::lifecycle::CloseGroup;
use crate::core::redact::truncate;
use crate::core::transport::{RequestBuilder, MAX_VENDOR_MESSAGE};
use crate::core::{Error, ErrorCode, HealthStatus, HttpTransport, Response, Result, Secret};
use crate::push::CardinalityGuard;
use crate::util::{first_line, map_loki_error, status_error};

/// Header carrying the tenant id (Loki multi-tenancy).
pub const HEADER_TENANT: &str = "X-Scope-OrgID";
/// Header asking Loki to categorize labels in query responses.
pub const HEADER_ENCODING_FLAGS: &str = "X-Loki-Response-Encoding-Flags";

const PATH_READY: &str = "/ready";
const PATH_BUILD_INFO: &str = "/loki/api/v1/status/buildinfo";

/// Async client for Grafana Loki. Cheap to clone (clones share one connection pool).
///
/// Service functions hang off the client directly: [`push`](Self::push),
/// [`query`](Self::query), [`query_range`](Self::query_range), [`labels`](Self::labels),
/// [`label_values`](Self::label_values), [`series`](Self::series), [`tail`](Self::tail),
/// [`index_stats`](Self::index_stats), [`volume`](Self::volume),
/// [`volume_range`](Self::volume_range), [`format_query`](Self::format_query),
/// [`ready`](Self::ready) and [`build_info`](Self::build_info). The logging category is reached
/// through [`new_log_client`](Self::new_log_client) /
/// [`new_otlp_log_client`](Self::new_otlp_log_client) (`LogClient`) and
/// [`log_query`](Self::log_query) (`LogQueryClient`). [`with_tenant`](Self::with_tenant)
/// returns a client for another tenant sharing the connection pool.
#[derive(Debug, Clone)]
pub struct LokiClient {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: LokiConfig,
    transport: Arc<HttpTransport>,
    owner: bool,
    on_close: CloseGroup,
    closed: AtomicBool,
    guard: CardinalityGuard,
    authorization: Option<Secret>,
}

/// Loki build information (`GET /loki/api/v1/status/buildinfo`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BuildInfo {
    /// Version, e.g. `3.7.10`.
    pub version: String,
    /// Git revision.
    pub revision: String,
    /// Git branch.
    pub branch: String,
    /// Build user.
    pub build_user: String,
    /// Build date.
    pub build_date: String,
    /// Go version (may be empty).
    pub go_version: String,
}

/// Decodes a JSON body; a malformed body is a non-retryable `UNAVAILABLE`.
pub(crate) fn decode_json<T: DeserializeOwned>(resp: &Response, op: &str) -> Result<T> {
    serde_json::from_slice(resp.bytes()).map_err(|_| {
        let e = Error::unavailable(format!("{op}: invalid JSON response"))
            .with_status(resp.status())
            .with_retryable(false);
        match resp.request_id() {
            Some(id) => e.with_request_id(id),
            None => e,
        }
    })
}

impl LokiClient {
    /// Creates a client from a configuration.
    pub fn new(config: LokiConfig) -> Result<Self> {
        config.validate()?;
        let transport = HttpTransport::new(config.common())?;
        let authorization = match (config.username(), config.password(), config.bearer_token()) {
            (Some(user), Some(pass), _) => {
                let token = base64::engine::general_purpose::STANDARD
                    .encode(format!("{user}:{}", pass.expose_secret()));
                transport.add_known_secret(pass);
                let token = Secret::new(token);
                transport.add_known_secret(&token);
                Some(Secret::new(format!("Basic {}", token.expose_secret())))
            }
            (_, _, Some(bearer)) => {
                transport.add_known_secret(bearer);
                Some(Secret::new(format!("Bearer {}", bearer.expose_secret())))
            }
            _ => None,
        };
        let guard = CardinalityGuard::new(config.allowed_labels());
        Ok(LokiClient {
            inner: Arc::new(Inner {
                config,
                transport: Arc::new(transport),
                owner: true,
                on_close: CloseGroup::new(),
                closed: AtomicBool::new(false),
                guard,
                authorization,
            }),
        })
    }

    /// Creates a client from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::new(LokiConfig::from_env()?)
    }

    /// The configuration in use.
    pub fn config(&self) -> &LokiConfig {
        &self.inner.config
    }

    /// The HTTP transport (for service modules and admin calls such as `POST /flush`).
    pub fn transport(&self) -> &HttpTransport {
        &self.inner.transport
    }

    /// The tenant id sent as `X-Scope-OrgID` (`None` for none).
    pub fn tenant_id(&self) -> Option<&str> {
        self.inner.config.tenant_id()
    }

    /// The cardinality guard used by `push` and the log exporter.
    pub fn guard(&self) -> &CardinalityGuard {
        &self.inner.guard
    }

    /// A client that sends `X-Scope-OrgID: id` on every request and on the tail handshake. It
    /// shares the connection pool and credentials; the original client is unchanged. A
    /// `VALIDATION` error reports an invalid id (see
    /// [`LokiConfigBuilder::tenant_id`](crate::LokiConfigBuilder::tenant_id)). Closing the
    /// derived client stops only its own log clients and tails; the shared connections close
    /// with the original client.
    pub fn with_tenant(&self, id: &str) -> Result<LokiClient> {
        if let Some(reason) = tenant_problem(id, self.inner.config.multi_tenant_queries()) {
            return Err(Error::validation(format!("withTenant: tenant id {reason}")));
        }
        Ok(LokiClient {
            inner: Arc::new(Inner {
                config: self.inner.config.with_tenant_id(id),
                transport: Arc::clone(&self.inner.transport),
                owner: false,
                on_close: CloseGroup::new(),
                closed: AtomicBool::new(false),
                guard: self.inner.guard.clone(),
                authorization: self.inner.authorization.clone(),
            }),
        })
    }

    /// Registers work that [`LokiClient::close`] runs (flush, stop tails, …).
    pub fn on_close<F>(&self, f: F)
    where
        F: FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    {
        self.inner.on_close.register(f);
    }

    /// A request with the auth and tenant headers.
    pub(crate) fn request(&self, method: Method, path: &str, op: &str) -> RequestBuilder<'_> {
        let mut req = self.inner.transport.request(method, path).operation(op);
        if let Some(auth) = &self.inner.authorization {
            req = req.secret_header("authorization", auth);
        }
        if let Some(t) = self.tenant_id() {
            req = req.header(HEADER_TENANT, t);
        }
        req
    }

    /// Sends a request built by [`request`](Self::request) with the Loki error mapping.
    pub(crate) async fn send(&self, req: RequestBuilder<'_>) -> Result<Response> {
        if self.is_closed() {
            return Err(Error::cancelled("client is closed"));
        }
        req.send().await.map_err(map_loki_error)
    }

    /// Authorization header value, when credentials are configured.
    pub(crate) fn authorization(&self) -> Option<&Secret> {
        self.inner.authorization.as_ref()
    }

    /// Readiness check (`GET /ready`). Healthy for HTTP 200; otherwise the status is Loki's
    /// text (for example `Ingester not ready: waiting for 15s after being ready`) or
    /// `unreachable` on network errors. Configured credentials are sent because gateways may
    /// protect `/ready`. Only `CANCELLED` and `CONFIG` are returned as errors.
    pub async fn health(&self) -> Result<HealthStatus> {
        let resp =
            match self.raw_ready("health").await {
                Ok(r) => r,
                Err(err) => {
                    return match err.code() {
                        ErrorCode::Cancelled | ErrorCode::Config => Err(err),
                        _ => Ok(HealthStatus::unhealthy("unreachable")
                            .with_detail("error", err.message())),
                    }
                }
            };
        let text = truncate(
            &self.transport().redact(&first_line(resp.bytes())),
            MAX_VENDOR_MESSAGE,
        );
        let mut hs = if resp.status() == 200 {
            HealthStatus::healthy("ok")
        } else if text.is_empty() {
            HealthStatus::unhealthy("unhealthy")
        } else {
            HealthStatus::unhealthy(text.clone())
        };
        hs = hs.with_detail("httpStatus", resp.status().to_string());
        if !text.is_empty() {
            hs = hs.with_detail("message", text);
        }
        Ok(hs)
    }

    async fn raw_ready(&self, op: &str) -> Result<Response> {
        let mut req = self.request(Method::GET, PATH_READY, op);
        for status in 400..600 {
            req = req.ok_status(status);
        }
        self.send(req).await
    }

    /// Whether Loki answers `GET /ready` with 200. A not-ready server (HTTP 503 during startup)
    /// returns `false` without an error; network errors and other statuses return an error.
    pub async fn ready(&self) -> Result<bool> {
        let resp = self.raw_ready("ready").await?;
        match resp.status() {
            200 => Ok(true),
            503 => Ok(false),
            s => Err(status_error(
                self.transport(),
                "ready",
                s,
                resp.bytes(),
                resp.request_id(),
            )),
        }
    }

    /// Authenticated round trip (`GET /loki/api/v1/labels?since=5m`); returns the latency.
    /// `AUTHENTICATION` on bad gateway credentials or a missing tenant on a multi-tenant Loki.
    pub async fn ping(&self) -> Result<Duration> {
        let started = Instant::now();
        let req = self
            .request(Method::GET, crate::query::PATH_LABELS, "ping")
            .query("since", "5m");
        self.send(req).await?;
        Ok(started.elapsed())
    }

    /// Server build information. `UNSUPPORTED` when the endpoint does not exist (HTTP 404).
    pub async fn build_info(&self) -> Result<BuildInfo> {
        let resp = self
            .send(self.request(Method::GET, PATH_BUILD_INFO, "buildInfo"))
            .await
            .map_err(|e| match e.code() {
                ErrorCode::NotFound => {
                    Error::unsupported("buildInfo: the server exposes no build information")
                }
                _ => e,
            })?;
        decode_json(&resp, "buildInfo")
    }

    /// The Loki version (`build_info().version`).
    pub async fn server_version(&self) -> Result<String> {
        Ok(self.build_info().await?.version)
    }

    /// Flushes and closes the log clients created by this client, stops its tails and (for a
    /// client from [`LokiClient::new`]) releases the connections. Later calls fail with
    /// `CANCELLED`. Idempotent.
    pub async fn close(&self) {
        self.inner.on_close.close().await;
        self.inner.closed.store(true, Ordering::SeqCst);
        if self.inner.owner {
            self.inner.transport.close();
        }
    }

    /// Whether [`LokiClient::close`] was called (on this client or, for a derived tenant
    /// client, on the original).
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst) || self.inner.transport.is_closed()
    }

    /// Whether this client owns the connection pool (false for [`with_tenant`](Self::with_tenant)).
    #[cfg_attr(not(feature = "blocking"), allow(dead_code))]
    pub(crate) fn is_owner(&self) -> bool {
        self.inner.owner
    }
}
