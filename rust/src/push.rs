//! Push API: streams, label validation, the cardinality guard and the JSON / protobuf+Snappy
//! wire formats of `POST /loki/api/v1/push`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::time::SystemTime;

use crate::client::LokiClient;
use crate::core::{Error, Result};
use crate::logging::{AttrValue, Attributes};
use crate::util::{format_nanos, go_quote, to_unix_nanos};

/// Push path.
pub(crate) const PATH_PUSH: &str = "/loki/api/v1/push";

/// Loki's push status when ingestion is administratively blocked (`blocked_ingestion` limits).
const STATUS_INGESTION_BLOCKED: u16 = 260;

/// One Loki log stream (`LokiStream`): a label set and its entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stream {
    /// The stream's index labels. Names must match `[a-zA-Z_][a-zA-Z0-9_]*` and must not start
    /// with `__`; values must be non-empty. Labels outside the cardinality guard allowlist are
    /// moved to every entry's structured metadata (see
    /// [`LokiConfigBuilder::allowed_labels`](crate::LokiConfigBuilder::allowed_labels)).
    pub labels: BTreeMap<String, String>,
    /// The log lines of the stream (any order).
    pub entries: Vec<Entry>,
}

impl Stream {
    /// A stream with the given labels and no entries.
    pub fn new<I, K, V>(labels: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Stream {
            labels: labels
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            entries: Vec::new(),
        }
    }

    /// Adds an entry.
    pub fn entry(mut self, entry: Entry) -> Self {
        self.entries.push(entry);
        self
    }
}

/// One log line of a [`Stream`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entry {
    /// Timestamp of the line (`None` = now). Sent as Unix nanoseconds.
    pub timestamp: Option<SystemTime>,
    /// The log line.
    pub line: String,
    /// Per-entry structured metadata (string → string; not indexed).
    pub structured_metadata: BTreeMap<String, String>,
}

impl Entry {
    /// An entry with the given line, timestamped at push time.
    pub fn new(line: impl Into<String>) -> Self {
        Entry {
            line: line.into(),
            ..Default::default()
        }
    }

    /// Sets the timestamp.
    pub fn at(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Adds one structured metadata pair.
    pub fn metadata(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.structured_metadata.insert(name.into(), value.into());
        self
    }
}

/// Wire format of [`LokiClient::push`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PushFormat {
    /// `application/json` (the default).
    #[default]
    Json,
    /// A Snappy-compressed `logproto.PushRequest` (`application/x-protobuf`), encoded from the
    /// Apache-2.0 `pkg/push/push.proto` field numbers.
    Protobuf,
}

/// Options of [`LokiClient::push`]. The default posts uncompressed JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PushOptions {
    /// Wire format (default [`PushFormat::Json`]).
    pub format: PushFormat,
    /// Compresses a JSON body (`Content-Encoding: gzip`). Protobuf bodies are always
    /// Snappy-compressed and ignore it.
    pub gzip: bool,
}

impl PushOptions {
    /// JSON push, optionally gzip-compressed.
    pub fn json(gzip: bool) -> Self {
        PushOptions {
            format: PushFormat::Json,
            gzip,
        }
    }

    /// Protobuf + Snappy push.
    pub fn protobuf() -> Self {
        PushOptions {
            format: PushFormat::Protobuf,
            gzip: false,
        }
    }
}

impl LokiClient {
    /// Sends streams to Loki (`POST /loki/api/v1/push`). Labels are validated client side and
    /// the cardinality guard moves labels outside the allowlist to structured metadata (unless
    /// `disable_label_guard`). The whole request fails or succeeds; Loki rejects entries older
    /// than its `reject_old_samples_max_age` or too far in the future with `VALIDATION`.
    /// Retrying is safe: Loki drops exact duplicates, so the transport replays the identical
    /// payload on 429/5xx. An empty slice sends nothing.
    pub async fn push(&self, streams: &[Stream], opts: PushOptions) -> Result<()> {
        self.push_inner(streams, opts, !self.config().disable_label_guard())
            .await
    }

    pub(crate) async fn push_inner(
        &self,
        streams: &[Stream],
        opts: PushOptions,
        guard: bool,
    ) -> Result<()> {
        if streams.is_empty() {
            return Ok(());
        }
        let now = SystemTime::now();
        let mut prepared = Vec::with_capacity(streams.len());
        for (i, s) in streams.iter().enumerate() {
            let mut s = if guard {
                self.guard().apply(s)
            } else {
                s.clone()
            };
            validate_stream(&s)
                .map_err(|reason| Error::validation(format!("push: stream {i}: {reason}")))?;
            for e in &mut s.entries {
                e.timestamp.get_or_insert(now);
            }
            prepared.push(s);
        }
        let req = self
            .request(reqwest::Method::POST, PATH_PUSH, "push")
            .idempotent(true);
        let req = match opts.format {
            PushFormat::Json => {
                let body = encode_push_json(&prepared);
                if opts.gzip {
                    req.header("content-encoding", "gzip")
                        .body(gzip_bytes(&body), "application/json")
                } else {
                    req.body(body, "application/json")
                }
            }
            PushFormat::Protobuf => {
                let raw = encode_push_protobuf(&prepared);
                let body = snap::raw::Encoder::new().compress_vec(&raw).map_err(|e| {
                    Error::validation("push: snappy compression failed").with_source(e)
                })?;
                req.body(body, "application/x-protobuf")
            }
        };
        let resp = self.send(req).await?;
        if resp.status() == STATUS_INGESTION_BLOCKED {
            let err = Error::unavailable(
                "push: ingestion is blocked by the Loki administrator (HTTP 260)",
            )
            .with_status(STATUS_INGESTION_BLOCKED)
            .with_retryable(false);
            return Err(match resp.request_id() {
                Some(id) => err.with_request_id(id),
                None => err,
            });
        }
        Ok(())
    }
}

/// Checks label names and values and metadata keys.
fn validate_stream(s: &Stream) -> std::result::Result<(), String> {
    for (name, value) in &s.labels {
        validate_label(name, value).map_err(|e| e.message().to_owned())?;
    }
    for (j, e) in s.entries.iter().enumerate() {
        if e.structured_metadata.keys().any(String::is_empty) {
            return Err(format!(
                "entry {j}: structured metadata name must not be empty"
            ));
        }
    }
    Ok(())
}

/// Checks one Loki index label: the name must match `[a-zA-Z_][a-zA-Z0-9_]*` and not start
/// with `__` (reserved), the value must be non-empty. Returns a `VALIDATION` error.
pub fn validate_label(name: &str, value: &str) -> Result<()> {
    if !valid_label_name(name) {
        return Err(Error::validation(format!(
            "label: invalid label name {name:?} (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )));
    }
    if name.starts_with("__") {
        return Err(Error::validation(format!(
            "label: label name {name:?} is reserved (starts with __)"
        )));
    }
    if value.is_empty() {
        return Err(Error::validation(format!(
            "label: label {name:?} has an empty value"
        )));
    }
    Ok(())
}

/// Whether `name` matches `[a-zA-Z_][a-zA-Z0-9_]*`.
pub(crate) fn valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

/// Turns an attribute key into a Loki label / structured metadata name the way Loki does for
/// OTLP attributes: every character outside `[a-zA-Z0-9_]` becomes `_` (`service.name` →
/// `service_name`) and a leading digit gets a `_` prefix. An empty key stays empty.
pub fn normalize_label_name(key: &str) -> String {
    let mut out: String = key
        .bytes()
        .map(|b| {
            if b == b'_' || b.is_ascii_alphanumeric() {
                b as char
            } else {
                '_'
            }
        })
        .collect();
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// Keeps Loki's index small: only allowlisted keys become labels, everything else is sent as
/// structured metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardinalityGuard {
    allowed: BTreeSet<String>,
}

impl Default for CardinalityGuard {
    fn default() -> Self {
        CardinalityGuard::new(crate::config::DEFAULT_ALLOWED_LABELS.iter().copied())
    }
}

impl CardinalityGuard {
    /// A guard for the allowlist (see also [`CardinalityGuard::default`] for
    /// [`DEFAULT_ALLOWED_LABELS`](crate::DEFAULT_ALLOWED_LABELS)).
    pub fn new<I, S>(allowed: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CardinalityGuard {
            allowed: allowed.into_iter().map(Into::into).collect(),
        }
    }

    /// The allowlist, sorted.
    pub fn allowed(&self) -> Vec<String> {
        self.allowed.iter().cloned().collect()
    }

    /// Whether `name` may be an index label.
    pub fn is_allowed(&self, name: &str) -> bool {
        self.allowed.contains(name)
    }

    /// A copy of `s` whose labels outside the allowlist are moved to every entry's structured
    /// metadata (an entry's own metadata wins).
    pub fn apply(&self, s: &Stream) -> Stream {
        let (labels, moved): (BTreeMap<_, _>, BTreeMap<_, _>) = s
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .partition(|(k, _)| self.allowed.contains(k));
        let entries = s
            .entries
            .iter()
            .map(|e| {
                let mut e = e.clone();
                for (k, v) in &moved {
                    e.structured_metadata
                        .entry(k.clone())
                        .or_insert_with(|| v.clone());
                }
                e
            })
            .collect();
        Stream { labels, entries }
    }

    /// Divides attributes into index labels (allowlisted, non-empty values) and structured
    /// metadata. Keys are normalized with [`normalize_label_name`]; non-string values are
    /// rendered as text (numbers and booleans) or JSON (arrays and maps).
    pub fn split(
        &self,
        attrs: &Attributes,
    ) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
        let mut labels = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        for (k, v) in attrs {
            let name = normalize_label_name(k);
            if name.is_empty() {
                continue;
            }
            let v = attr_string(v);
            if self.allowed.contains(&name) && !v.is_empty() && !name.starts_with("__") {
                labels.insert(name, v);
            } else {
                metadata.insert(name, v);
            }
        }
        (labels, metadata)
    }
}

/// Renders an attribute value as a structured metadata string.
pub(crate) fn attr_string(v: &AttrValue) -> String {
    match v {
        AttrValue::Null => String::new(),
        AttrValue::String(s) => s.clone(),
        AttrValue::Bool(b) => b.to_string(),
        AttrValue::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
            (Some(i), _, _) => i.to_string(),
            (_, Some(u), _) => u.to_string(),
            (_, _, Some(f)) => f.to_string(),
            _ => n.to_string(),
        },
        other => other.to_string(),
    }
}

/// Renders labels in Prometheus label-set syntax with sorted names and Go-quoted values:
/// `{a="b", c="d"}`.
pub fn label_set_string(labels: &BTreeMap<String, String>) -> String {
    let parts: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}={}", go_quote(v)))
        .collect();
    format!("{{{}}}", parts.join(", "))
}

pub(crate) fn encode_push_json(streams: &[Stream]) -> Vec<u8> {
    let streams: Vec<serde_json::Value> = streams
        .iter()
        .map(|s| {
            let values: Vec<serde_json::Value> = s
                .entries
                .iter()
                .map(|e| {
                    let ts = e.timestamp.map(format_nanos).unwrap_or_default();
                    if e.structured_metadata.is_empty() {
                        serde_json::json!([ts, e.line])
                    } else {
                        serde_json::json!([ts, e.line, e.structured_metadata])
                    }
                })
                .collect();
            serde_json::json!({"stream": s.labels, "values": values})
        })
        .collect();
    serde_json::json!({ "streams": streams })
        .to_string()
        .into_bytes()
}

pub(crate) fn gzip_bytes(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    // Writing into a Vec cannot fail.
    let _ = enc.write_all(data);
    enc.finish().unwrap_or_default()
}

// Protobuf encoding of logproto.PushRequest, hand-written from the field numbers of Loki's
// Apache-2.0 pkg/push/push.proto (v3.7.10):
//
//   PushRequest      { repeated StreamAdapter streams = 1; }
//   StreamAdapter    { string labels = 1; repeated EntryAdapter entries = 2; }
//   EntryAdapter     { google.protobuf.Timestamp timestamp = 1; string line = 2;
//                      repeated LabelPairAdapter structuredMetadata = 3; }
//   LabelPairAdapter { string name = 1; string value = 2; }
//   Timestamp        { int64 seconds = 1; int32 nanos = 2; }
pub(crate) fn encode_push_protobuf(streams: &[Stream]) -> Vec<u8> {
    let mut out = Vec::new();
    for s in streams {
        let mut sb = Vec::new();
        pb_bytes(&mut sb, 1, label_set_string(&s.labels).as_bytes());
        for e in &s.entries {
            pb_bytes(&mut sb, 2, &encode_entry_protobuf(e));
        }
        pb_bytes(&mut out, 1, &sb);
    }
    out
}

fn encode_entry_protobuf(e: &Entry) -> Vec<u8> {
    let ns = e.timestamp.map(to_unix_nanos).unwrap_or(0);
    let (mut secs, mut nanos) = (ns / 1_000_000_000, ns % 1_000_000_000);
    if nanos < 0 {
        secs -= 1;
        nanos += 1_000_000_000;
    }
    let mut ts = Vec::new();
    if secs != 0 {
        pb_tag(&mut ts, 1, 0);
        pb_varint(&mut ts, secs as u64);
    }
    if nanos != 0 {
        pb_tag(&mut ts, 2, 0);
        pb_varint(&mut ts, nanos as u64);
    }
    let mut b = Vec::new();
    pb_bytes(&mut b, 1, &ts);
    pb_bytes(&mut b, 2, e.line.as_bytes());
    for (k, v) in &e.structured_metadata {
        let mut pair = Vec::new();
        pb_bytes(&mut pair, 1, k.as_bytes());
        pb_bytes(&mut pair, 2, v.as_bytes());
        pb_bytes(&mut b, 3, &pair);
    }
    b
}

fn pb_tag(b: &mut Vec<u8>, field: u64, wire: u64) {
    pb_varint(b, (field << 3) | wire);
}

fn pb_varint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push((v as u8) | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

fn pb_bytes(b: &mut Vec<u8>, field: u64, data: &[u8]) {
    pb_tag(b, field, 2);
    pb_varint(b, data.len() as u64);
    b.extend_from_slice(data);
}
