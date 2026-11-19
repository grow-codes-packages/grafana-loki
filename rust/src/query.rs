//! Query API: instant and range queries, labels, series, index statistics, volume and
//! `format_query`, plus the decoders of Loki's result shapes.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use reqwest::Method;
use serde::Deserialize;
use serde_json::Value;

use crate::client::{decode_json, LokiClient, HEADER_ENCODING_FLAGS};
use crate::core::{Error, Result};
use crate::util::{format_duration, format_nanos, from_unix_nanos};

pub(crate) const PATH_QUERY: &str = "/loki/api/v1/query";
pub(crate) const PATH_RANGE: &str = "/loki/api/v1/query_range";
pub(crate) const PATH_LABELS: &str = "/loki/api/v1/labels";
pub(crate) const PATH_SERIES: &str = "/loki/api/v1/series";
pub(crate) const PATH_STATS: &str = "/loki/api/v1/index/stats";
pub(crate) const PATH_VOLUME: &str = "/loki/api/v1/index/volume";
pub(crate) const PATH_VOLUME_RANGE: &str = "/loki/api/v1/index/volume_range";
pub(crate) const PATH_FORMAT: &str = "/loki/api/v1/format_query";

/// Result type of a log query.
pub const RESULT_TYPE_STREAMS: &str = "streams";
/// Result type of an instant metric query.
pub const RESULT_TYPE_VECTOR: &str = "vector";
/// Result type of a range metric query.
pub const RESULT_TYPE_MATRIX: &str = "matrix";
/// Result type of a scalar expression.
pub const RESULT_TYPE_SCALAR: &str = "scalar";

/// Order of log entries in query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Direction {
    /// Newest entries first (Loki's default).
    #[default]
    Backward,
    /// Oldest entries first.
    Forward,
}

impl Direction {
    /// Wire value (`backward` / `forward`).
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Backward => "backward",
            Direction::Forward => "forward",
        }
    }
}

/// Options of [`LokiClient::query`] (instant metric queries).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryOptions {
    /// Evaluation time (`None` = now).
    pub time: Option<SystemTime>,
    /// Maximum number of entries (kept for parity; metric queries ignore it; 0 = default).
    pub limit: usize,
    /// Kept for parity; metric queries ignore it.
    pub direction: Option<Direction>,
}

/// Options of [`LokiClient::query_range`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryRangeOptions {
    /// Inclusive start; `None` = `end - since`, or Loki's default (one hour before end).
    pub start: Option<SystemTime>,
    /// Exclusive end; `None` = now.
    pub end: Option<SystemTime>,
    /// Relative start used when `start` is `None`.
    pub since: Option<Duration>,
    /// Resolution of metric queries.
    pub step: Option<Duration>,
    /// Samples stream results (only every `interval` an entry).
    pub interval: Option<Duration>,
    /// Maximum number of entries across all streams (0 = Loki default 100; max 5000 by default).
    pub limit: usize,
    /// Direction of entries (default backward).
    pub direction: Option<Direction>,
}

/// Options of [`LokiClient::labels`] and [`LokiClient::label_values`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelsOptions {
    /// Start of the time range (`None` = Loki's default, the last 6 hours).
    pub start: Option<SystemTime>,
    /// End of the time range.
    pub end: Option<SystemTime>,
    /// Relative start used when `start` is `None`.
    pub since: Option<Duration>,
    /// Optional stream selector that scopes the result.
    pub query: Option<String>,
}

/// Options of [`LokiClient::series`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeriesOptions {
    /// Start of the time range.
    pub start: Option<SystemTime>,
    /// End of the time range.
    pub end: Option<SystemTime>,
    /// Relative start used when `start` is `None`.
    pub since: Option<Duration>,
}

/// Options of [`LokiClient::index_stats`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexOptions {
    /// Start of the time range.
    pub start: Option<SystemTime>,
    /// End of the time range.
    pub end: Option<SystemTime>,
}

/// How [`LokiClient::volume`] aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeAggregation {
    /// By series of the matched labels (Loki's default).
    Series,
    /// By label name (with `target_labels`).
    Labels,
}

impl VolumeAggregation {
    /// Wire value (`series` / `labels`).
    pub fn as_str(self) -> &'static str {
        match self {
            VolumeAggregation::Series => "series",
            VolumeAggregation::Labels => "labels",
        }
    }
}

/// Options of [`LokiClient::volume`] and [`LokiClient::volume_range`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VolumeOptions {
    /// Start of the time range.
    pub start: Option<SystemTime>,
    /// End of the time range.
    pub end: Option<SystemTime>,
    /// Resolution of `volume_range`.
    pub step: Option<Duration>,
    /// Maximum number of series (0 = default).
    pub limit: usize,
    /// Aggregates by these label names.
    pub target_labels: Vec<String>,
    /// Aggregation (default series).
    pub aggregate_by: Option<VolumeAggregation>,
}

/// The data of a query, query_range, volume or volume_range response.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryResult {
    /// `streams`, `vector`, `matrix` or `scalar` (empty when Loki sent no data).
    pub result_type: String,
    /// Set for log queries.
    pub streams: Vec<StreamResult>,
    /// Set for instant metric queries.
    pub vector: Vec<Sample>,
    /// Set for range metric queries.
    pub matrix: Vec<SampleSeries>,
    /// Set for scalar results.
    pub scalar: Option<Point>,
    /// Response encoding flags (for example `categorize-labels`).
    pub encoding_flags: Vec<String>,
    /// Loki's raw query statistics (may be absent).
    pub stats: Option<Value>,
}

impl QueryResult {
    /// Number of log entries across all streams.
    pub fn entry_count(&self) -> usize {
        self.streams.iter().map(|s| s.entries.len()).sum()
    }
}

/// One stream of a log query result or tail frame.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamResult {
    /// Stream labels. With categorized labels (the SDK default) they are index labels only;
    /// otherwise Loki merges metadata and parsed labels in.
    pub labels: BTreeMap<String, String>,
    /// The entries of the stream.
    pub entries: Vec<StreamEntry>,
}

/// One log line returned by Loki.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    /// Timestamp of the line.
    pub timestamp: SystemTime,
    /// The log line.
    pub line: String,
    /// Structured metadata (categorized responses only).
    pub structured_metadata: BTreeMap<String, String>,
    /// Labels extracted by parsers such as `| json` (categorized responses only).
    pub parsed: BTreeMap<String, String>,
}

/// One metric sample value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    /// Sample time.
    pub timestamp: SystemTime,
    /// Sample value.
    pub value: f64,
}

/// One series of an instant vector.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Series labels.
    pub metric: BTreeMap<String, String>,
    /// The value.
    pub value: Point,
}

/// One series of a range matrix.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleSeries {
    /// Series labels.
    pub metric: BTreeMap<String, String>,
    /// The values over time.
    pub values: Vec<Point>,
}

/// Response of [`LokiClient::index_stats`] (from the TSDB index only: zero until the ingester
/// flushed its chunks; `bytes` is tracked per chunk in KB).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct IndexStats {
    /// Number of streams.
    #[serde(default)]
    pub streams: i64,
    /// Number of chunks.
    #[serde(default)]
    pub chunks: i64,
    /// Bytes (KB rounding per chunk).
    #[serde(default)]
    pub bytes: i64,
    /// Number of entries.
    #[serde(default)]
    pub entries: i64,
}

/// Whether `q` is a log query (starts with a stream selector).
pub(crate) fn is_log_query(q: &str) -> bool {
    q.trim_start().starts_with('{')
}

type Params = Vec<(String, String)>;

fn set_time(q: &mut Params, key: &str, t: Option<SystemTime>) {
    if let Some(t) = t {
        q.push((key.into(), format_nanos(t)));
    }
}

fn set_duration(q: &mut Params, key: &str, d: Option<Duration>) {
    if let Some(d) = d.filter(|d| !d.is_zero()) {
        q.push((key.into(), format_duration(d)));
    }
}

fn set_limit(q: &mut Params, n: usize) {
    if n > 0 {
        q.push(("limit".into(), n.to_string()));
    }
}

fn set_direction(q: &mut Params, d: Option<Direction>) {
    if let Some(d) = d {
        q.push(("direction".into(), d.as_str().into()));
    }
}

fn require_query(op: &str, q: &str) -> Result<()> {
    if q.trim().is_empty() {
        return Err(Error::validation(format!("{op}: query must not be empty")));
    }
    Ok(())
}

impl LokiClient {
    /// Runs an instant query (`GET /loki/api/v1/query`). Loki answers log queries only as range
    /// queries, so a log query (`{...}`) is rejected with `VALIDATION` before any request: use
    /// [`query_range`](Self::query_range) or `log_query().search`.
    pub async fn query(&self, logql: &str, opts: QueryOptions) -> Result<QueryResult> {
        require_query("query", logql)?;
        if is_log_query(logql) {
            return Err(Error::validation(
                "query: log queries are not supported as instant queries; use queryRange",
            ));
        }
        let mut q: Params = vec![("query".into(), logql.into())];
        set_time(&mut q, "time", opts.time);
        set_limit(&mut q, opts.limit);
        set_direction(&mut q, opts.direction);
        self.get_result("query", PATH_QUERY, q).await
    }

    /// Runs a range query (`GET /loki/api/v1/query_range`). Log queries return `streams`
    /// (each stream's entries ordered by direction; streams are not ordered globally), metric
    /// queries return `matrix`. Labels are categorized: stream labels are index labels,
    /// structured metadata and parsed labels come separately per entry.
    pub async fn query_range(&self, logql: &str, opts: QueryRangeOptions) -> Result<QueryResult> {
        require_query("queryRange", logql)?;
        if let (Some(s), Some(e)) = (opts.start, opts.end) {
            if e <= s {
                return Err(Error::validation("queryRange: end must be after start"));
            }
        }
        let mut q: Params = vec![("query".into(), logql.into())];
        set_time(&mut q, "start", opts.start);
        set_time(&mut q, "end", opts.end);
        if opts.start.is_none() {
            set_duration(&mut q, "since", opts.since);
        }
        set_duration(&mut q, "step", opts.step);
        set_duration(&mut q, "interval", opts.interval);
        set_limit(&mut q, opts.limit);
        set_direction(&mut q, opts.direction);
        self.get_result("queryRange", PATH_RANGE, q).await
    }

    /// Lists label names (`GET /loki/api/v1/labels`). Structured metadata keys are not labels.
    /// An empty result is an empty list.
    pub async fn labels(&self, opts: LabelsOptions) -> Result<Vec<String>> {
        self.get_strings("labels", PATH_LABELS, labels_query(&opts))
            .await
    }

    /// Lists the values of label `name` (`GET /loki/api/v1/label/{name}/values`). Loki answers
    /// unknown and invalid names with an empty list, so the name is validated client side.
    pub async fn label_values(&self, name: &str, opts: LabelsOptions) -> Result<Vec<String>> {
        if !crate::push::valid_label_name(name) {
            return Err(Error::validation(format!(
                "labelValues: invalid label name {name:?}"
            )));
        }
        let path = format!("/loki/api/v1/label/{name}/values");
        self.get_strings(&format!("labelValues {name}"), &path, labels_query(&opts))
            .await
    }

    /// Lists the label sets of the streams matching any of the selectors (`POST
    /// /loki/api/v1/series` with `match[]`). At least one matcher is required (Loki would
    /// otherwise return every series).
    pub async fn series<S: AsRef<str>>(
        &self,
        matchers: &[S],
        opts: SeriesOptions,
    ) -> Result<Vec<BTreeMap<String, String>>> {
        if matchers.is_empty() {
            return Err(Error::validation(
                "series: at least one matcher is required",
            ));
        }
        let mut q: Params = matchers
            .iter()
            .map(|m| ("match[]".to_owned(), m.as_ref().to_owned()))
            .collect();
        set_time(&mut q, "start", opts.start);
        set_time(&mut q, "end", opts.end);
        if opts.start.is_none() {
            set_duration(&mut q, "since", opts.since);
        }
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&q)
            .finish();
        let req = self
            .request(Method::POST, PATH_SERIES, "series")
            .idempotent(true)
            .body(body, "application/x-www-form-urlencoded");
        let resp = self.send(req).await?;
        #[derive(Deserialize)]
        struct Env {
            #[serde(default)]
            data: Option<Vec<BTreeMap<String, String>>>,
        }
        let env: Env = decode_json(&resp, "series")?;
        Ok(env.data.unwrap_or_default())
    }

    /// Index statistics for a selector (`GET /loki/api/v1/index/stats`). Values come from the
    /// TSDB index and stay zero until the ingester flushed the chunks.
    pub async fn index_stats(&self, query: &str, opts: IndexOptions) -> Result<IndexStats> {
        require_query("indexStats", query)?;
        let mut q: Params = vec![("query".into(), query.into())];
        set_time(&mut q, "start", opts.start);
        set_time(&mut q, "end", opts.end);
        let resp = self.send(self.get(PATH_STATS, "indexStats", &q)).await?;
        decode_json(&resp, "indexStats")
    }

    /// Ingested bytes per series or label (`GET /loki/api/v1/index/volume`) as a vector. Needs
    /// `limits_config.volume_enabled` on the server; otherwise Loki returns an empty vector
    /// ("no data", not an error).
    pub async fn volume(&self, query: &str, opts: VolumeOptions) -> Result<QueryResult> {
        let q = volume_query("volume", query, &opts, false)?;
        self.get_result("volume", PATH_VOLUME, q).await
    }

    /// [`volume`](Self::volume) over time (`GET /loki/api/v1/index/volume_range`). The result
    /// is always a matrix (`result_type` `"matrix"`, data in `matrix`): Loki answers with a
    /// vector when every series has a single point, and the SDK converts that vector into a
    /// one-point-per-series matrix.
    pub async fn volume_range(&self, query: &str, opts: VolumeOptions) -> Result<QueryResult> {
        let q = volume_query("volumeRange", query, &opts, true)?;
        let mut res = self.get_result("volumeRange", PATH_VOLUME_RANGE, q).await?;
        if res.result_type == RESULT_TYPE_VECTOR {
            res.matrix = std::mem::take(&mut res.vector)
                .into_iter()
                .map(|s| SampleSeries {
                    metric: s.metric,
                    values: vec![s.value],
                })
                .collect();
            res.result_type = RESULT_TYPE_MATRIX.to_owned();
        }
        Ok(res)
    }

    /// Asks Loki to parse and pretty-print a LogQL query (`GET /loki/api/v1/format_query`).
    /// `VALIDATION` when the query does not parse.
    pub async fn format_query(&self, logql: &str) -> Result<String> {
        let q: Params = vec![("query".into(), logql.into())];
        let resp = self.send(self.get(PATH_FORMAT, "formatQuery", &q)).await?;
        #[derive(Deserialize)]
        struct Env {
            #[serde(default)]
            data: String,
        }
        Ok(decode_json::<Env>(&resp, "formatQuery")?.data)
    }

    async fn get_strings(&self, op: &str, path: &str, q: Params) -> Result<Vec<String>> {
        let resp = self.send(self.get(path, op, &q)).await?;
        #[derive(Deserialize)]
        struct Env {
            #[serde(default)]
            data: Option<Vec<String>>,
        }
        Ok(decode_json::<Env>(&resp, op)?.data.unwrap_or_default())
    }

    async fn get_result(&self, op: &str, path: &str, q: Params) -> Result<QueryResult> {
        let req = self
            .get(path, op, &q)
            .header(HEADER_ENCODING_FLAGS, "categorize-labels");
        let resp = self.send(req).await?;
        #[derive(Deserialize)]
        struct Env {
            #[serde(default)]
            data: Value,
        }
        let env: Env = decode_json(&resp, op)?;
        decode_query_data(&env.data).map_err(|reason| {
            let e = Error::unavailable(format!("{op}: invalid response: {reason}"))
                .with_status(resp.status())
                .with_retryable(false);
            match resp.request_id() {
                Some(id) => e.with_request_id(id),
                None => e,
            }
        })
    }

    fn get<'a>(
        &'a self,
        path: &str,
        op: &str,
        q: &Params,
    ) -> crate::core::transport::RequestBuilder<'a> {
        let mut req = self.request(Method::GET, path, op);
        for (k, v) in q {
            req = req.query(k, v);
        }
        req
    }
}

fn labels_query(opts: &LabelsOptions) -> Params {
    let mut q = Params::new();
    set_time(&mut q, "start", opts.start);
    set_time(&mut q, "end", opts.end);
    if opts.start.is_none() {
        set_duration(&mut q, "since", opts.since);
    }
    if let Some(query) = opts.query.as_deref().filter(|s| !s.is_empty()) {
        q.push(("query".into(), query.into()));
    }
    q
}

fn volume_query(op: &str, query: &str, opts: &VolumeOptions, with_step: bool) -> Result<Params> {
    require_query(op, query)?;
    let mut q: Params = vec![("query".into(), query.into())];
    set_time(&mut q, "start", opts.start);
    set_time(&mut q, "end", opts.end);
    if with_step {
        set_duration(&mut q, "step", opts.step);
    }
    set_limit(&mut q, opts.limit);
    if !opts.target_labels.is_empty() {
        q.push(("targetLabels".into(), opts.target_labels.join(",")));
    }
    if let Some(a) = opts.aggregate_by {
        q.push(("aggregateBy".into(), a.as_str().into()));
    }
    Ok(q)
}

type Decode<T> = std::result::Result<T, String>;

/// Decodes the `data` object of a query response.
pub(crate) fn decode_query_data(data: &Value) -> Decode<QueryResult> {
    let mut out = QueryResult::default();
    let Some(obj) = data.as_object() else {
        return if data.is_null() {
            Ok(out)
        } else {
            Err("data is not an object".into())
        };
    };
    out.result_type = obj
        .get("resultType")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    out.encoding_flags = string_list(obj.get("encodingFlags"));
    out.stats = obj.get("stats").filter(|v| !v.is_null()).cloned();
    let result = match obj.get("result") {
        None | Some(Value::Null) => return Ok(out),
        Some(r) => r,
    };
    match out.result_type.as_str() {
        RESULT_TYPE_STREAMS => out.streams = decode_streams(result)?,
        RESULT_TYPE_VECTOR => {
            for item in array(result, "vector")? {
                out.vector.push(Sample {
                    metric: string_map(item.get("metric"))?,
                    value: decode_point(item.get("value").unwrap_or(&Value::Null))?,
                });
            }
        }
        RESULT_TYPE_MATRIX => {
            for item in array(result, "matrix")? {
                let values = match item.get("values") {
                    None | Some(Value::Null) => Vec::new(),
                    Some(v) => array(v, "matrix values")?
                        .iter()
                        .map(decode_point)
                        .collect::<Decode<_>>()?,
                };
                out.matrix.push(SampleSeries {
                    metric: string_map(item.get("metric"))?,
                    values,
                });
            }
        }
        RESULT_TYPE_SCALAR => out.scalar = Some(decode_point(result)?),
        other => return Err(format!("unknown result type {other:?}")),
    }
    Ok(out)
}

fn array<'a>(v: &'a Value, what: &str) -> Decode<&'a Vec<Value>> {
    v.as_array()
        .ok_or_else(|| format!("{what} is not an array"))
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn string_map(v: Option<&Value>) -> Decode<BTreeMap<String, String>> {
    match v {
        None | Some(Value::Null) => Ok(BTreeMap::new()),
        Some(Value::Object(m)) => m
            .iter()
            .map(|(k, v)| match v {
                Value::String(s) => Ok((k.clone(), s.clone())),
                other => Err(format!("label {k:?} has a non-string value {other}")),
            })
            .collect(),
        Some(other) => Err(format!("labels are not an object: {other}")),
    }
}

/// Decodes streams with 2-element (`[ts, line]`) or categorized 3-element
/// (`[ts, line, {structuredMetadata, parsed}]`) values.
pub(crate) fn decode_streams(v: &Value) -> Decode<Vec<StreamResult>> {
    array(v, "streams")?
        .iter()
        .map(|item| {
            let entries = match item.get("values") {
                None | Some(Value::Null) => Vec::new(),
                Some(vals) => array(vals, "stream values")?
                    .iter()
                    .map(decode_stream_entry)
                    .collect::<Decode<_>>()?,
            };
            Ok(StreamResult {
                labels: string_map(item.get("stream"))?,
                entries,
            })
        })
        .collect()
}

fn decode_stream_entry(v: &Value) -> Decode<StreamEntry> {
    let parts = v
        .as_array()
        .filter(|a| a.len() >= 2)
        .ok_or_else(|| format!("stream value {v} must have 2 or 3 elements"))?;
    let ts = parts[0]
        .as_str()
        .ok_or_else(|| "stream timestamp is not a string".to_owned())?;
    let timestamp = parse_unix_nano(ts).map_err(|e| e.message().to_owned())?;
    let line = parts[1]
        .as_str()
        .ok_or_else(|| "stream line is not a string".to_owned())?
        .to_owned();
    let (mut structured_metadata, mut parsed) = (BTreeMap::new(), BTreeMap::new());
    if let Some(cat) = parts.get(2) {
        if !cat.is_object() {
            return Err("stream categorized labels are not an object".into());
        }
        structured_metadata = string_map(cat.get("structuredMetadata"))?;
        parsed = string_map(cat.get("parsed"))?;
    }
    Ok(StreamEntry {
        timestamp,
        line,
        structured_metadata,
        parsed,
    })
}

/// Decodes `[<float seconds>, "<value>"]`.
fn decode_point(v: &Value) -> Decode<Point> {
    let pair = v.as_array().filter(|a| a.len() == 2).ok_or_else(|| {
        format!(
            "invalid sample {}",
            crate::core::redact::truncate(&v.to_string(), 60)
        )
    })?;
    let ts_text = match &pair[0] {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => return Err(format!("invalid sample timestamp {other}")),
    };
    let timestamp = parse_unix_seconds(&ts_text)?;
    let s = pair[1]
        .as_str()
        .ok_or_else(|| "sample value is not a string".to_owned())?;
    let value = s
        .parse::<f64>()
        .map_err(|_| format!("sample value {s:?} is not a number"))?;
    Ok(Point { timestamp, value })
}

/// Parses a decimal Unix nanosecond timestamp (Loki's wire form). `VALIDATION` when invalid.
pub fn parse_unix_nano(s: &str) -> Result<SystemTime> {
    s.parse::<i64>()
        .map(from_unix_nanos)
        .map_err(|_| Error::validation(format!("invalid nanosecond timestamp {s:?}")))
}

/// Parses `<seconds>[.<fraction>]` without float rounding (exponent forms fall back to f64).
fn parse_unix_seconds(s: &str) -> Decode<SystemTime> {
    let bad = || format!("invalid timestamp {s:?}");
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    match whole.parse::<i64>() {
        Ok(sec) if frac.bytes().all(|b| b.is_ascii_digit()) => {
            let mut digits: String = frac.chars().take(9).collect();
            while digits.len() < 9 {
                digits.push('0');
            }
            let ns: i64 = digits.parse().map_err(|_| bad())?;
            Ok(from_unix_nanos(
                sec.saturating_mul(1_000_000_000).saturating_add(ns),
            ))
        }
        _ => {
            let f: f64 = s.parse().map_err(|_| bad())?;
            if !f.is_finite() {
                return Err(bad());
            }
            Ok(from_unix_nanos((f * 1e9) as i64))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_parsing() {
        assert_eq!(
            parse_unix_seconds("1795033788.144").unwrap(),
            from_unix_nanos(1_795_033_788_144_000_000)
        );
        assert_eq!(
            parse_unix_seconds("1.0000000019").unwrap(),
            from_unix_nanos(1_000_000_001)
        );
        assert_eq!(
            parse_unix_seconds("1e3").unwrap(),
            from_unix_nanos(1_000_000_000_000)
        );
        assert!(parse_unix_seconds("NaN").is_err());
        assert!(parse_unix_seconds("x").is_err());
        assert!(parse_unix_seconds("1.x").is_err());
    }
}
