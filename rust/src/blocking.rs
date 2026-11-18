// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! Blocking wrapper (feature `blocking`). Each client owns a small tokio runtime with one
//! worker thread, so background work (tails, batching log clients) keeps running between calls.
//! Clients from [`LokiClient::with_tenant`] share their parent's runtime.
//!
//! Do not use it from inside an async runtime (calls would panic); use the async client there.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::core::{Error, HealthStatus, Result};
use crate::logging::{
    BatchingLogClient, LogExporter, LogQueryClient, LogRecord, LogSearchResult, RecordCallback,
    SearchOptions, TailHandle, TailOptions, WaitOptions,
};
use crate::{
    BuildInfo, IndexOptions, IndexStats, LabelsOptions, LiveTailOptions, LogClientOptions,
    LokiClient as AsyncClient, LokiConfig, PushOptions, QueryOptions, QueryRangeOptions,
    QueryResult, SeriesOptions, Stream, TailFrame, VolumeOptions,
};

/// Blocking client for Grafana Loki with the same methods as the async client.
#[derive(Debug)]
pub struct LokiClient {
    inner: AsyncClient,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl LokiClient {
    /// Creates a client from a configuration.
    pub fn new(config: LokiConfig) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("grow-grafana-loki")
            .enable_all()
            .build()
            .map_err(|e| Error::config("cannot start the tokio runtime").with_source(e))?;
        let inner = {
            let _guard = runtime.enter();
            AsyncClient::new(config)?
        };
        Ok(LokiClient {
            inner,
            runtime: Arc::new(runtime),
        })
    }

    /// Creates a client from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::new(LokiConfig::from_env()?)
    }

    /// The wrapped async client.
    pub fn async_client(&self) -> &AsyncClient {
        &self.inner
    }

    /// Runs a future of the async API to completion on this client's runtime.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// See [`AsyncClient::config`].
    pub fn config(&self) -> &LokiConfig {
        self.inner.config()
    }

    /// See [`AsyncClient::tenant_id`].
    pub fn tenant_id(&self) -> Option<&str> {
        self.inner.tenant_id()
    }

    /// See [`AsyncClient::with_tenant`]; the derived client shares this client's runtime.
    pub fn with_tenant(&self, id: &str) -> Result<LokiClient> {
        Ok(LokiClient {
            inner: self.inner.with_tenant(id)?,
            runtime: Arc::clone(&self.runtime),
        })
    }

    /// See [`AsyncClient::health`].
    pub fn health(&self) -> Result<HealthStatus> {
        self.block_on(self.inner.health())
    }

    /// See [`AsyncClient::ready`].
    pub fn ready(&self) -> Result<bool> {
        self.block_on(self.inner.ready())
    }

    /// See [`AsyncClient::ping`].
    pub fn ping(&self) -> Result<Duration> {
        self.block_on(self.inner.ping())
    }

    /// See [`AsyncClient::server_version`].
    pub fn server_version(&self) -> Result<String> {
        self.block_on(self.inner.server_version())
    }

    /// See [`AsyncClient::build_info`].
    pub fn build_info(&self) -> Result<BuildInfo> {
        self.block_on(self.inner.build_info())
    }

    /// See [`AsyncClient::push`].
    pub fn push(&self, streams: &[Stream], opts: PushOptions) -> Result<()> {
        self.block_on(self.inner.push(streams, opts))
    }

    /// See [`AsyncClient::query`].
    pub fn query(&self, logql: &str, opts: QueryOptions) -> Result<QueryResult> {
        self.block_on(self.inner.query(logql, opts))
    }

    /// See [`AsyncClient::query_range`].
    pub fn query_range(&self, logql: &str, opts: QueryRangeOptions) -> Result<QueryResult> {
        self.block_on(self.inner.query_range(logql, opts))
    }

    /// See [`AsyncClient::labels`].
    pub fn labels(&self, opts: LabelsOptions) -> Result<Vec<String>> {
        self.block_on(self.inner.labels(opts))
    }

    /// See [`AsyncClient::label_values`].
    pub fn label_values(&self, name: &str, opts: LabelsOptions) -> Result<Vec<String>> {
        self.block_on(self.inner.label_values(name, opts))
    }

    /// See [`AsyncClient::series`].
    pub fn series<S: AsRef<str>>(
        &self,
        matchers: &[S],
        opts: SeriesOptions,
    ) -> Result<Vec<BTreeMap<String, String>>> {
        self.block_on(self.inner.series(matchers, opts))
    }

    /// See [`AsyncClient::index_stats`].
    pub fn index_stats(&self, query: &str, opts: IndexOptions) -> Result<IndexStats> {
        self.block_on(self.inner.index_stats(query, opts))
    }

    /// See [`AsyncClient::volume`].
    pub fn volume(&self, query: &str, opts: VolumeOptions) -> Result<QueryResult> {
        self.block_on(self.inner.volume(query, opts))
    }

    /// See [`AsyncClient::volume_range`].
    pub fn volume_range(&self, query: &str, opts: VolumeOptions) -> Result<QueryResult> {
        self.block_on(self.inner.volume_range(query, opts))
    }

    /// See [`AsyncClient::format_query`].
    pub fn format_query(&self, logql: &str) -> Result<String> {
        self.block_on(self.inner.format_query(logql))
    }

    /// See [`AsyncClient::tail`]. `on_entries` runs on the client's worker thread.
    pub fn tail<F>(&self, logql: &str, on_entries: F, opts: LiveTailOptions) -> Result<TailSession>
    where
        F: Fn(TailFrame) + Send + Sync + 'static,
    {
        let session = self.block_on(self.inner.tail(logql, on_entries, opts))?;
        Ok(TailSession {
            inner: session,
            runtime: Arc::clone(&self.runtime),
        })
    }

    /// See [`AsyncClient::log_query`].
    pub fn log_query(&self) -> LogQuery {
        LogQuery {
            inner: self.inner.log_query(),
            runtime: Arc::clone(&self.runtime),
        }
    }

    /// See [`AsyncClient::new_log_client`]. Flush with `client.block_on(lc.flush(None))`.
    pub fn new_log_client(&self, opts: LogClientOptions) -> Result<BatchingLogClient> {
        let _guard = self.runtime.enter();
        self.inner.new_log_client(opts)
    }

    /// See [`AsyncClient::new_otlp_log_client`].
    pub fn new_otlp_log_client(&self, opts: LogClientOptions) -> Result<BatchingLogClient> {
        let _guard = self.runtime.enter();
        self.inner.new_otlp_log_client(opts)
    }

    /// See [`AsyncClient::new_log_client_with_exporter`].
    pub fn new_log_client_with_exporter(
        &self,
        exporter: Arc<dyn LogExporter>,
        opts: LogClientOptions,
    ) -> Result<BatchingLogClient> {
        let _guard = self.runtime.enter();
        self.inner.new_log_client_with_exporter(exporter, opts)
    }

    /// See [`AsyncClient::close`]. Idempotent.
    pub fn close(&self) {
        self.block_on(self.inner.close());
    }
}

impl Drop for LokiClient {
    fn drop(&mut self) {
        if self.inner.is_owner() {
            self.inner.transport().close();
        }
    }
}

/// Blocking handle of a live tail (see [`crate::TailSession`]).
#[derive(Debug, Clone)]
pub struct TailSession {
    inner: crate::TailSession,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl TailSession {
    /// Closes the WebSocket and waits for the reader to finish. Idempotent.
    pub fn stop(&self) {
        self.runtime.block_on(self.inner.stop());
    }

    /// Blocks until the tail ended.
    pub fn wait(&self) {
        self.runtime.block_on(self.inner.wait());
    }

    /// Whether the tail has ended.
    pub fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Why the tail ended (see [`crate::TailSession::err`]).
    pub fn err(&self) -> Option<Error> {
        self.inner.err()
    }

    /// Number of entries Loki reported as dropped.
    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped_count()
    }
}

/// Blocking [`LogQueryClient`] (see [`crate::LogQuery`]).
#[derive(Debug, Clone)]
pub struct LogQuery {
    inner: crate::LogQuery,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl LogQuery {
    /// See [`LogQueryClient::search`].
    pub fn search(&self, query: &str, opts: SearchOptions) -> Result<LogSearchResult> {
        self.runtime.block_on(self.inner.search(query, opts))
    }

    /// See [`LogQueryClient::tail`]; stop it with [`TailHandle::stop`].
    pub fn tail(
        &self,
        query: &str,
        on_record: RecordCallback,
        opts: TailOptions,
    ) -> Result<TailHandle> {
        self.runtime
            .block_on(self.inner.tail(query, on_record, opts))
    }

    /// See [`LogQueryClient::list_streams`].
    pub fn list_streams(&self) -> Result<Vec<String>> {
        self.runtime.block_on(self.inner.list_streams())
    }

    /// See [`LogQueryClient::list_labels`].
    pub fn list_labels(&self) -> Result<Vec<String>> {
        self.runtime.block_on(self.inner.list_labels())
    }

    /// See [`LogQueryClient::wait_for_log`].
    pub fn wait_for_log(&self, query: &str, opts: WaitOptions) -> Result<LogRecord> {
        self.runtime.block_on(self.inner.wait_for_log(query, opts))
    }
}
