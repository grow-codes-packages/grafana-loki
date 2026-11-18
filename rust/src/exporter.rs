//! Logging category exporters: [`PushExporter`] (Loki push API) and [`OtlpExporter`]
//! (`/otlp/v1/logs`), plus the log clients the [`LokiClient`] creates.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value};

use crate::client::LokiClient;
use crate::core::transport::SDK_VERSION;
use crate::core::{Error, Result};
use crate::logging::{
    AttrValue, Attributes, BatchOptions, BatchingLogClient, ExportOptions, ExportResult, LogClient,
    LogExporter, LogRecord, RedactionProcessor, Severity,
};
use crate::push::{label_set_string, CardinalityGuard, Entry, PushFormat, PushOptions, Stream};
use crate::util::format_nanos;

/// OTLP logs path.
pub(crate) const PATH_OTLP_LOGS: &str = "/otlp/v1/logs";

/// Structured metadata key for [`LogRecord::trace_id`] (the name Loki derives from OTLP).
pub const METADATA_TRACE_ID: &str = "trace_id";
/// Structured metadata key for [`LogRecord::span_id`].
pub const METADATA_SPAN_ID: &str = "span_id";
/// Structured metadata key for [`LogRecord::trace_flags`].
pub const METADATA_TRACE_FLAGS: &str = "trace_flags";
/// Structured metadata key for [`LogRecord::severity_text`].
pub const METADATA_SEVERITY_TEXT: &str = "severity_text";
/// Structured metadata key for the OpenTelemetry severity number.
pub const METADATA_SEVERITY_NUMBER: &str = "severity_number";
/// Structured metadata key for [`LogRecord::observed_timestamp`] (Unix nanoseconds).
pub const METADATA_OBSERVED_TIMESTAMP: &str = "observed_timestamp";

/// The `LogExporter` of the logging category: groups records into Loki streams with the
/// cardinality guard and posts them to `/loki/api/v1/push`.
///
/// Mapping: resource and attribute keys are normalized (`service.name` → `service_name`);
/// allowlisted keys become index labels, plus `level` (lower-case severity name) when allowed;
/// everything else, and `trace_id`, `span_id`, `trace_flags`, `severity_text`,
/// `severity_number` and `observed_timestamp`, becomes structured metadata. The record body is
/// the log line.
#[derive(Debug, Clone)]
pub struct PushExporter {
    client: LokiClient,
    format: PushFormat,
}

impl PushExporter {
    /// An exporter for `client` using `format`.
    pub fn new(client: &LokiClient, format: PushFormat) -> Self {
        PushExporter {
            client: client.clone(),
            format,
        }
    }
}

/// Converts records into Loki streams using `guard`. Streams are ordered by their label set.
pub fn records_to_streams(records: &[LogRecord], guard: &CardinalityGuard) -> Vec<Stream> {
    let mut by_key: BTreeMap<String, Stream> = BTreeMap::new();
    for r in records {
        let (labels, md) = record_labels(r, guard);
        let key = label_set_string(&labels);
        by_key
            .entry(key)
            .or_insert_with(|| Stream {
                labels,
                entries: Vec::new(),
            })
            .entries
            .push(Entry {
                timestamp: Some(r.timestamp),
                line: r.body.clone(),
                structured_metadata: md,
            });
    }
    by_key.into_values().collect()
}

fn record_labels(
    r: &LogRecord,
    guard: &CardinalityGuard,
) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
    let (mut labels, mut metadata) = guard.split(&r.resource);
    let (al, am) = guard.split(&r.attributes);
    labels.extend(al);
    metadata.extend(am);
    let sev = r.severity;
    if !labels.contains_key("level") && guard.is_allowed("level") {
        labels.insert("level".into(), sev.text().to_lowercase());
    }
    let sev_text = if r.severity_text.is_empty() {
        sev.text().to_owned()
    } else {
        r.severity_text.clone()
    };
    metadata.insert(METADATA_SEVERITY_TEXT.into(), sev_text);
    metadata.insert(METADATA_SEVERITY_NUMBER.into(), sev.number().to_string());
    if let Some(t) = &r.trace_id {
        metadata.insert(METADATA_TRACE_ID.into(), t.to_lowercase());
    }
    if let Some(s) = &r.span_id {
        metadata.insert(METADATA_SPAN_ID.into(), s.to_lowercase());
    }
    if let Some(f) = r.trace_flags {
        metadata.insert(METADATA_TRACE_FLAGS.into(), f.to_string());
    }
    if let Some(o) = r.observed_timestamp {
        metadata.insert(METADATA_OBSERVED_TIMESTAMP.into(), format_nanos(o));
    }
    // Loki drops empty metadata values anyway.
    metadata.retain(|_, v| !v.is_empty());
    (labels, metadata)
}

/// A non-retryable error rejects the whole batch; a retryable one is returned for the batch
/// processor to retry.
fn export_failure(n: usize, err: Error) -> Result<ExportResult> {
    if err.is_retryable() {
        Err(err)
    } else {
        Ok(ExportResult::partial(0, n, Some(err)))
    }
}

#[async_trait]
impl LogExporter for PushExporter {
    async fn export(&self, records: &[LogRecord], opts: ExportOptions) -> Result<ExportResult> {
        if records.is_empty() {
            return Ok(ExportResult::default());
        }
        let streams = records_to_streams(records, self.client.guard());
        let push = PushOptions {
            format: self.format,
            gzip: opts.gzip,
        };
        // Labels are already guarded; a second pass could move "level" away.
        match self.client.push_inner(&streams, push, false).await {
            Ok(()) => Ok(ExportResult::accepted(records.len())),
            Err(e) => export_failure(records.len(), e),
        }
    }
}

/// A `LogExporter` that posts OTLP/JSON logs to Loki's native OTLP endpoint
/// (`/otlp/v1/logs`). Loki promotes `service.name`, `deployment.environment` and other default
/// resource attributes to labels and stores everything else as structured metadata.
#[derive(Debug, Clone)]
pub struct OtlpExporter {
    client: LokiClient,
    scope: String,
}

impl OtlpExporter {
    /// An OTLP exporter for `client`.
    pub fn new(client: &LokiClient) -> Self {
        OtlpExporter {
            client: client.clone(),
            scope: "grow-codes-grafana-loki".into(),
        }
    }
}

#[async_trait]
impl LogExporter for OtlpExporter {
    async fn export(&self, records: &[LogRecord], opts: ExportOptions) -> Result<ExportResult> {
        if records.is_empty() {
            return Ok(ExportResult::default());
        }
        let body = otlp_request(records, &self.scope).to_string().into_bytes();
        let req = self
            .client
            .request(Method::POST, PATH_OTLP_LOGS, "otlpLogs")
            .idempotent(true);
        let req = if opts.gzip {
            req.header("content-encoding", "gzip")
                .body(crate::push::gzip_bytes(&body), "application/json")
        } else {
            req.body(body, "application/json")
        };
        match self.client.send(req).await {
            Ok(_) => Ok(ExportResult::accepted(records.len())),
            Err(e) => export_failure(records.len(), e),
        }
    }
}

fn otlp_attrs(m: &Attributes) -> Vec<Value> {
    m.iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| json!({"key": k, "value": otlp_any(v)}))
        .collect()
}

/// Renders an attribute value as an OTLP/JSON `AnyValue`.
fn otlp_any(v: &AttrValue) -> Value {
    match v {
        AttrValue::Bool(b) => json!({ "boolValue": b }),
        AttrValue::Number(n) if n.is_i64() || n.is_u64() => json!({ "intValue": n.to_string() }),
        AttrValue::Number(n) => json!({ "doubleValue": n }),
        AttrValue::Array(a) => {
            let values: Vec<Value> = a.iter().map(otlp_any).collect();
            json!({ "arrayValue": { "values": values } })
        }
        AttrValue::Object(m) => {
            let attrs: Attributes = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            json!({ "kvlistValue": { "values": otlp_attrs(&attrs) } })
        }
        AttrValue::String(s) => json!({ "stringValue": s }),
        AttrValue::Null => json!({ "stringValue": "" }),
    }
}

pub(crate) fn otlp_request(records: &[LogRecord], scope: &str) -> Value {
    let mut groups: Vec<(String, &Attributes, Vec<Value>)> = Vec::new();
    for r in records {
        let key = serde_json::to_string(&r.resource).unwrap_or_default();
        let idx = match groups.iter().position(|(k, _, _)| *k == key) {
            Some(i) => i,
            None => {
                groups.push((key, &r.resource, Vec::new()));
                groups.len() - 1
            }
        };
        let text = if r.severity_text.is_empty() {
            r.severity.text().to_owned()
        } else {
            r.severity_text.clone()
        };
        let mut lr = json!({
            "timeUnixNano": format_nanos(r.timestamp),
            "severityNumber": r.severity.number(),
            "severityText": text,
            "body": { "stringValue": r.body },
            "attributes": otlp_attrs(&r.attributes),
        });
        if let Some(o) = r.observed_timestamp {
            lr["observedTimeUnixNano"] = json!(format_nanos(o));
        }
        if let Some(t) = &r.trace_id {
            lr["traceId"] = json!(t.to_lowercase());
        }
        if let Some(s) = &r.span_id {
            lr["spanId"] = json!(s.to_lowercase());
        }
        if let Some(f) = r.trace_flags {
            lr["flags"] = json!(f);
        }
        groups[idx].2.push(lr);
    }
    let rls: Vec<Value> = groups
        .into_iter()
        .map(|(_, resource, logs)| {
            json!({
                "resource": { "attributes": otlp_attrs(resource) },
                "scopeLogs": [{ "scope": { "name": scope, "version": SDK_VERSION }, "logRecords": logs }],
            })
        })
        .collect();
    json!({ "resourceLogs": rls })
}

/// Options of the batching log clients made by [`LokiClient::new_log_client`] and friends.
#[derive(Debug, Clone)]
pub struct LogClientOptions {
    /// Batch settings.
    pub batch: BatchOptions,
    /// Resource attributes merged into every record (record keys win): `service.name`,
    /// `service.version`, `deployment.environment`, `host.name`.
    pub resource: Attributes,
    /// Redaction applied before records leave the process (default
    /// [`RedactionProcessor::default`]); `None` disables it.
    pub redaction: Option<RedactionProcessor>,
    /// Minimum severity of loggers made by [`LogClient::logger`] (default `Info`).
    pub min_severity: Severity,
}

impl Default for LogClientOptions {
    fn default() -> Self {
        LogClientOptions {
            batch: BatchOptions::default(),
            resource: Attributes::new(),
            redaction: Some(RedactionProcessor::default()),
            min_severity: Severity::Info,
        }
    }
}

impl LogClientOptions {
    /// Adds one resource attribute.
    pub fn resource_attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<AttrValue>,
    ) -> Self {
        self.resource.insert(key.into(), value.into());
        self
    }

    /// Sets the batch options.
    pub fn batch(mut self, batch: BatchOptions) -> Self {
        self.batch = batch;
        self
    }
}

impl LokiClient {
    /// A batching `LogClient` that pushes to Loki through a JSON [`PushExporter`]. Closing the
    /// `LokiClient` flushes and closes it. Must be called inside a tokio runtime.
    pub fn new_log_client(&self, opts: LogClientOptions) -> Result<BatchingLogClient> {
        self.new_log_client_with_exporter(Arc::new(PushExporter::new(self, PushFormat::Json)), opts)
    }

    /// A batching `LogClient` that exports through Loki's OTLP endpoint (`/otlp/v1/logs`).
    /// Closing the `LokiClient` flushes and closes it.
    pub fn new_otlp_log_client(&self, opts: LogClientOptions) -> Result<BatchingLogClient> {
        self.new_log_client_with_exporter(Arc::new(OtlpExporter::new(self)), opts)
    }

    /// A batching `LogClient` for any exporter (for example
    /// `PushExporter::new(&client, PushFormat::Protobuf)`). Closing the `LokiClient` flushes
    /// and closes it.
    pub fn new_log_client_with_exporter(
        &self,
        exporter: Arc<dyn LogExporter>,
        opts: LogClientOptions,
    ) -> Result<BatchingLogClient> {
        let client = BatchingLogClient::builder(exporter)
            .batch_options(opts.batch)
            .resource(opts.resource)
            .redaction(opts.redaction)
            .min_severity(opts.min_severity)
            .build()?;
        let closing = client.clone();
        self.on_close(move || {
            Box::pin(async move {
                let _ = closing.close().await;
            })
        });
        Ok(client)
    }
}
