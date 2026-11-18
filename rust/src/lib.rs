// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! # grow-grafana-loki
//!
//! Rust SDK for [Grafana Loki](https://grow-codes-packages.github.io/grafana-loki/) by Grow Codes.
//!
//! ```no_run
//! # async fn demo() -> grow_grafana_loki::Result<()> {
//! use grow_grafana_loki::{Entry, LogQL, LokiClient, LokiConfig, PushOptions, QueryRangeOptions, Stream};
//!
//! let client = LokiClient::new(LokiConfig::from_env()?)?;
//! let health = client.health().await?;
//! println!("healthy: {}", health.healthy);
//!
//! let stream = Stream::new([("service_name", "checkout"), ("level", "info")])
//!     .entry(Entry::new("order placed").metadata("order_id", "o-1"));
//! client.push(&[stream], PushOptions::default()).await?;
//!
//! let query = LogQL::selector([("service_name", "checkout")]).line_contains("order").build()?;
//! let result = client.query_range(&query, QueryRangeOptions::default()).await?;
//! println!("{} entries", result.entry_count());
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! Modules: push (JSON or protobuf + Snappy, cardinality guard), query (`query`,
//! `query_range`, `labels`, `label_values`, `series`, `index_stats`, `volume`,
//! `volume_range`, `format_query`), live `tail` over WebSocket, the [`LogQL`] builder, and the
//! logging category ([`LokiClient::new_log_client`], [`LokiClient::log_query`]).
//!
//! Features: `blocking` (a blocking client), `testing` (in-memory fakes and the conformance
//! runner), `tracing-layer` and `log-bridge` (logging adapters, on by default).
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod core;

mod client;
mod config;
mod exporter;
mod logql;
mod logquery;
mod push;
mod query;
mod tail;
mod util;

#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;

#[cfg(feature = "testing")]
#[cfg_attr(docsrs, doc(cfg(feature = "testing")))]
pub mod testing;

pub mod logging;

pub use crate::core::{Error, ErrorCode, HealthStatus, Result, RetryPolicy, Secret};

pub use crate::config::{DEFAULT_ALLOWED_LABELS, DEFAULT_URL};

pub use crate::client::{BuildInfo, LokiClient, HEADER_ENCODING_FLAGS, HEADER_TENANT};

pub use crate::config::LokiConfig;

pub use crate::config::LokiConfigBuilder;

pub use crate::exporter::{
    records_to_streams, LogClientOptions, OtlpExporter, PushExporter, METADATA_OBSERVED_TIMESTAMP,
    METADATA_SEVERITY_NUMBER, METADATA_SEVERITY_TEXT, METADATA_SPAN_ID, METADATA_TRACE_FLAGS,
    METADATA_TRACE_ID,
};
pub use crate::logql::{quote, LogQL, LogQLValue, Matcher};
pub use crate::logquery::{entry_to_record, LogQuery};
pub use crate::push::{
    label_set_string, normalize_label_name, validate_label, CardinalityGuard, Entry, PushFormat,
    PushOptions, Stream,
};
pub use crate::query::{
    parse_unix_nano, Direction, IndexOptions, IndexStats, LabelsOptions, Point, QueryOptions,
    QueryRangeOptions, QueryResult, Sample, SampleSeries, SeriesOptions, StreamEntry, StreamResult,
    VolumeAggregation, VolumeOptions, RESULT_TYPE_MATRIX, RESULT_TYPE_SCALAR, RESULT_TYPE_STREAMS,
    RESULT_TYPE_VECTOR,
};
pub use crate::tail::{DroppedEntry, LiveTailOptions, TailFrame, TailSession, MAX_TAIL_DELAY};

/// Service-prefixed alias of [`Error`].
pub type LokiError = Error;
