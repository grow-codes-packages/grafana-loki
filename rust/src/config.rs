// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! `LokiConfig`: common settings (SPEC §2.2) plus the Grafana Loki tenant, credentials and
//! cardinality guard.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::core::{
    CommonConfig, CommonConfigBuilder, EnvReader, Error, Hooks, Result, RetryPolicy, Secret,
    TlsConfig,
};

/// Endpoint used when neither the builder nor the environment sets one (HTTP API, OTLP and
/// WebSocket tail share this port).
pub const DEFAULT_URL: &str = "http://127.0.0.1:3100";

/// Loki's limit for a tenant id (`X-Scope-OrgID`), in bytes.
const MAX_TENANT_ID_LENGTH: usize = 150;

/// The cardinality guard's default label allowlist: only these keys become Loki index labels;
/// every other attribute is sent as structured metadata.
pub const DEFAULT_ALLOWED_LABELS: &[&str] = &[
    "service_name",
    "service_namespace",
    "deployment_environment",
    "host_name",
    "level",
    "job",
    "instance",
    "namespace",
    "pod",
    "container",
];

/// Vendor variables checked (in order) after `GROW_GRAFANA_LOKI_URL`.
const VENDOR_URL_VARS: &[&str] = &["LOKI_URL"];
/// Vendor variables checked after `GROW_GRAFANA_LOKI_TENANT_ID`.
const VENDOR_TENANT_VARS: &[&str] = &["LOKI_TENANT_ID"];
/// Vendor variables checked after `GROW_GRAFANA_LOKI_USERNAME`.
const VENDOR_USERNAME_VARS: &[&str] = &["LOKI_USERNAME", "LOKI_BASIC_AUTH_USER"];
/// Vendor variables checked after `GROW_GRAFANA_LOKI_PASSWORD`.
const VENDOR_PASSWORD_VARS: &[&str] = &["LOKI_PASSWORD", "LOKI_BASIC_AUTH_PASSWORD"];
/// Vendor variables checked after `GROW_GRAFANA_LOKI_BEARER_TOKEN`.
const VENDOR_BEARER_VARS: &[&str] = &["LOKI_BEARER_TOKEN"];

/// Immutable client configuration; build it with [`LokiConfig::builder`] or
/// [`LokiConfig::from_env`]. `Debug` output redacts every credential.
///
/// Loki itself has no authentication: the tenant id is sent as `X-Scope-OrgID` (needed when
/// Loki runs with `auth_enabled: true`), and Basic or Bearer credentials are for gateways and
/// hosted Loki (Grafana Cloud: Basic user = instance id, password = access token). The client
/// never interprets them.
///
/// Environment variables read by [`LokiConfig::from_env`] (explicit value >
/// `GROW_GRAFANA_LOKI_*` > vendor variables > default):
///
/// | Field | Variables |
/// | - | - |
/// | url | `GROW_GRAFANA_LOKI_URL`, `LOKI_URL` (default [`DEFAULT_URL`]) |
/// | tenant_id | `GROW_GRAFANA_LOKI_TENANT_ID`, `LOKI_TENANT_ID` |
/// | username | `GROW_GRAFANA_LOKI_USERNAME`, `LOKI_USERNAME`, `LOKI_BASIC_AUTH_USER` |
/// | password | `GROW_GRAFANA_LOKI_PASSWORD`, `LOKI_PASSWORD`, `LOKI_BASIC_AUTH_PASSWORD` |
/// | bearer_token | `GROW_GRAFANA_LOKI_BEARER_TOKEN`, `LOKI_BEARER_TOKEN` |
///
/// plus the common `GROW_GRAFANA_LOKI_CONNECT_TIMEOUT`, `REQUEST_TIMEOUT`, `TOTAL_TIMEOUT`,
/// `MAX_RETRIES`, `CA_CERT`, `CLIENT_CERT`, `CLIENT_KEY`, `INSECURE_SKIP_VERIFY` and `PROXY`.
#[derive(Clone)]
pub struct LokiConfig {
    common: CommonConfig,
    tenant_id: Option<String>,
    username: Option<String>,
    password: Option<Secret>,
    bearer_token: Option<Secret>,
    multi_tenant_queries: bool,
    allowed_labels: Option<Vec<String>>,
    disable_label_guard: bool,
}

impl fmt::Debug for LokiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiConfig")
            .field("common", &self.common)
            .field("tenant_id", &self.tenant_id)
            .field("username", &self.username)
            .field("password", &self.password)
            .field("bearer_token", &self.bearer_token)
            .field("multi_tenant_queries", &self.multi_tenant_queries)
            .field("allowed_labels", &self.allowed_labels)
            .field("disable_label_guard", &self.disable_label_guard)
            .finish()
    }
}

impl LokiConfig {
    /// A builder with nothing set.
    pub fn builder() -> LokiConfigBuilder {
        LokiConfigBuilder::default()
    }

    /// Configuration from the process environment
    /// (explicit > `GROW_GRAFANA_LOKI_*` > vendor variables > defaults).
    pub fn from_env() -> Result<Self> {
        Self::builder().build_from_env()
    }

    /// Settings shared by every Grow Codes SDK.
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    /// Endpoint base URL.
    pub fn url(&self) -> &url::Url {
        self.common.url()
    }

    /// Tenant id sent as `X-Scope-OrgID` (`None` sends no header).
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Basic authentication user name.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// Basic authentication password.
    pub fn password(&self) -> Option<&Secret> {
        self.password.as_ref()
    }

    /// Bearer token.
    pub fn bearer_token(&self) -> Option<&Secret> {
        self.bearer_token.as_ref()
    }

    /// Whether tenant ids joined with `|` (multi-tenant queries) are allowed.
    pub fn multi_tenant_queries(&self) -> bool {
        self.multi_tenant_queries
    }

    /// The effective cardinality guard allowlist (default [`DEFAULT_ALLOWED_LABELS`]).
    pub fn allowed_labels(&self) -> Vec<String> {
        match &self.allowed_labels {
            Some(l) => l.clone(),
            None => DEFAULT_ALLOWED_LABELS
                .iter()
                .map(|s| (*s).to_owned())
                .collect(),
        }
    }

    /// Whether `push` sends every stream label as an index label (the log exporter always
    /// applies the guard).
    pub fn disable_label_guard(&self) -> bool {
        self.disable_label_guard
    }

    /// A copy with another tenant id (not validated; see [`LokiConfig::validate`]).
    pub(crate) fn with_tenant_id(&self, tenant: &str) -> LokiConfig {
        let mut c = self.clone();
        c.tenant_id = Some(tenant.to_owned());
        c
    }

    /// Checks the service rules (common rules are checked when building): username and
    /// password together, Basic and Bearer mutually exclusive, valid tenant id and allowlist.
    /// Returns `CONFIG` errors.
    pub fn validate(&self) -> Result<()> {
        if self.username.is_some() != self.password.is_some() {
            return Err(Error::config(
                "username and password must be given together",
            ));
        }
        if self.username.is_some() && self.bearer_token.is_some() {
            return Err(Error::config(
                "basic credentials and bearer token are mutually exclusive",
            ));
        }
        if let Some(t) = &self.tenant_id {
            if let Some(reason) = tenant_problem(t, self.multi_tenant_queries) {
                return Err(Error::config(format!("tenantId {reason}")));
            }
        }
        if let Some(labels) = &self.allowed_labels {
            for l in labels {
                if !crate::push::valid_label_name(l) {
                    return Err(Error::config(format!(
                        "allowedLabels contains an invalid label name {l:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Returns why `id` is not a valid tenant id (`None` when valid).
pub(crate) fn tenant_problem(id: &str, multi: bool) -> Option<String> {
    if id.contains('|') {
        if !multi {
            return Some("must not contain '|' (multi-tenant queries are not enabled)".into());
        }
        return id.split('|').find_map(|part| tenant_problem(part, false));
    }
    if id.is_empty() {
        return Some("must not be empty".into());
    }
    if id.len() > MAX_TENANT_ID_LENGTH {
        return Some("is too long (max 150 bytes)".into());
    }
    if id == "." || id == ".." {
        return Some("must not be '.' or '..'".into());
    }
    id.chars()
        .find(|c| !(c.is_ascii_alphanumeric() || "!-_.*'()".contains(*c)))
        .map(|c| format!("contains unsupported character {c:?}"))
}

/// Builder for [`LokiConfig`]. Values set here win over the environment.
#[derive(Debug, Clone, Default)]
pub struct LokiConfigBuilder {
    common: CommonConfigBuilder,
    tenant_id: Option<String>,
    username: Option<String>,
    password: Option<Secret>,
    bearer_token: Option<Secret>,
    multi_tenant_queries: bool,
    allowed_labels: Option<Vec<String>>,
    disable_label_guard: bool,
}

impl LokiConfigBuilder {
    /// Endpoint base URL (default [`DEFAULT_URL`]).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.common = self.common.url(url);
        self
    }
    /// Tenant id sent as `X-Scope-OrgID` on every request and on the tail handshake. Rules:
    /// at most 150 bytes, not `.` or `..`, characters `[A-Za-z0-9!-_.*'()]` only; `|` only
    /// with [`multi_tenant_queries`](Self::multi_tenant_queries).
    pub fn tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }
    /// HTTP Basic user name (requires a password).
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }
    /// HTTP Basic password (requires a user name).
    pub fn password(mut self, password: impl Into<Secret>) -> Self {
        self.password = Some(password.into());
        self
    }
    /// Sends `Authorization: Bearer <token>` (not together with Basic).
    pub fn bearer_token(mut self, token: impl Into<Secret>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }
    /// Allows tenant ids joined with `|` (Loki's multi-tenant query syntax; the server must
    /// enable `multi_tenant_queries_enabled`).
    pub fn multi_tenant_queries(mut self, on: bool) -> Self {
        self.multi_tenant_queries = on;
        self
    }
    /// Cardinality guard allowlist used by `push` and the log exporter: labels outside it are
    /// moved to structured metadata (default [`DEFAULT_ALLOWED_LABELS`]).
    pub fn allowed_labels<I, S>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_labels = Some(labels.into_iter().map(Into::into).collect());
        self
    }
    /// Sends every stream label of `push` as an index label (the log exporter always applies
    /// the guard).
    pub fn disable_label_guard(mut self, disable: bool) -> Self {
        self.disable_label_guard = disable;
        self
    }
    /// Connect timeout (default 10 s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.connect_timeout(timeout);
        self
    }
    /// Per-attempt timeout (default 30 s).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.request_timeout(timeout);
        self
    }
    /// Whole-call timeout including retries (default 120 s).
    pub fn total_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.total_timeout(timeout);
        self
    }
    /// Retry policy.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.common = self.common.retry(retry);
        self
    }
    /// Maximum retries (default 3).
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.common = self.common.max_retries(max_retries);
        self
    }
    /// TLS settings.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.common = self.common.tls(tls);
        self
    }
    /// Extra trusted CA certificates (PEM file).
    pub fn ca_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.common = self.common.ca_cert_path(path);
        self
    }
    /// Disables TLS verification (tests only).
    pub fn insecure_skip_verify(mut self, skip: bool) -> Self {
        self.common = self.common.insecure_skip_verify(skip);
        self
    }
    /// Proxy URL (environment proxies are ignored).
    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.common = self.common.proxy(proxy);
        self
    }
    /// Extra request header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.common = self.common.header(name, value);
        self
    }
    /// Text appended to the User-Agent.
    pub fn user_agent_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.common = self.common.user_agent_suffix(suffix);
        self
    }
    /// Request hooks.
    pub fn hooks(mut self, hooks: Arc<dyn Hooks>) -> Self {
        self.common = self.common.hooks(hooks);
        self
    }
    /// Edits any other common setting.
    pub fn common(mut self, f: impl FnOnce(CommonConfigBuilder) -> CommonConfigBuilder) -> Self {
        self.common = f(self.common);
        self
    }

    /// Builds without reading the environment.
    pub fn build(self) -> Result<LokiConfig> {
        let config = LokiConfig {
            common: self.common.build(DEFAULT_URL)?,
            tenant_id: self.tenant_id,
            username: self.username,
            password: self.password,
            bearer_token: self.bearer_token,
            multi_tenant_queries: self.multi_tenant_queries,
            allowed_labels: self.allowed_labels,
            disable_label_guard: self.disable_label_guard,
        };
        config.validate()?;
        Ok(config)
    }

    /// Fills unset values from the process environment, then builds.
    pub fn build_from_env(self) -> Result<LokiConfig> {
        self.build_with_env(&EnvReader::new())
    }

    /// Fills unset values from `env`, then builds.
    pub fn build_with_env(mut self, env: &EnvReader) -> Result<LokiConfig> {
        self.common = self.common.apply_env(env, VENDOR_URL_VARS)?;
        if self.tenant_id.is_none() {
            self.tenant_id = env.string("TENANT_ID", VENDOR_TENANT_VARS);
        }
        if self.username.is_none() {
            self.username = env.string("USERNAME", VENDOR_USERNAME_VARS);
        }
        if self.password.is_none() {
            self.password = env.secret("PASSWORD", VENDOR_PASSWORD_VARS);
        }
        if self.bearer_token.is_none() {
            self.bearer_token = env.secret("BEARER_TOKEN", VENDOR_BEARER_VARS);
        }
        self.build()
    }
}
