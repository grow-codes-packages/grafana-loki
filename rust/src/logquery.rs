//! `LogQueryClient` of the logging category on top of Loki's query API.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use base64::Engine as _;

use crate::client::LokiClient;
use crate::core::{Error, Result};
use crate::exporter::{
    METADATA_OBSERVED_TIMESTAMP, METADATA_SEVERITY_NUMBER, METADATA_SEVERITY_TEXT,
    METADATA_SPAN_ID, METADATA_TRACE_FLAGS, METADATA_TRACE_ID,
};
use crate::logging::{
    AttrValue, Attributes, LogQueryClient, LogRecord, LogSearchResult, RecordCallback,
    SearchOptions, Severity, SortOrder, TailHandle, TailOptions,
};
use crate::logql::quote;
use crate::push::label_set_string;
use crate::query::{
    is_log_query, parse_unix_nano, Direction, LabelsOptions, QueryRangeOptions, SeriesOptions,
    StreamEntry, StreamResult,
};
use crate::tail::LiveTailOptions;
use crate::util::{from_unix_nanos, to_unix_nanos};

/// Loki's default `max_entries_limit_per_query`.
const MAX_ENTRIES_PER_QUERY: usize = 5000;

/// Matches every stream that has a `service_name` (Loki adds one to every stream,
/// `unknown_service` by default).
const STREAMS_SELECTOR: &str = r#"{service_name=~".+"}"#;

/// Loki label names mapped back to OpenTelemetry resource keys.
const RESOURCE_LABEL_KEYS: &[(&str, &str)] = &[
    ("service_name", "service.name"),
    ("service_namespace", "service.namespace"),
    ("service_version", "service.version"),
    ("service_instance_id", "service.instance.id"),
    ("deployment_environment", "deployment.environment"),
    ("host_name", "host.name"),
];

/// The `LogQueryClient` of this SDK. Queries are raw LogQL; a query that does not start with a
/// stream selector is free text and becomes `{service_name=~".+"} |= "<text>"`.
#[derive(Debug, Clone)]
pub struct LogQuery {
    client: LokiClient,
}

impl LokiClient {
    /// The `LogQueryClient` of this client (same tenant and credentials).
    pub fn log_query(&self) -> LogQuery {
        LogQuery {
            client: self.clone(),
        }
    }
}

/// Turns a free-text query into LogQL.
fn to_logql(query: &str) -> String {
    let q = query.trim();
    if q.is_empty() {
        STREAMS_SELECTOR.to_owned()
    } else if is_log_query(q) {
        q.to_owned()
    } else {
        format!("{STREAMS_SELECTOR} |= {}", quote(q))
    }
}

/// Decoded search cursor: `<last timestamp ns>:<entries at that timestamp already returned>`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SearchCursor {
    ts: i64,
    skip: usize,
}

fn encode_cursor(c: SearchCursor) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{}:{}", c.ts, c.skip))
}

fn decode_cursor(s: &str) -> Result<SearchCursor> {
    let bad = || Error::validation("logs.search: invalid cursor");
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|_| bad())?;
    let text = String::from_utf8(raw).map_err(|_| bad())?;
    let (ts, skip) = text.split_once(':').ok_or_else(bad)?;
    Ok(SearchCursor {
        ts: ts.parse().map_err(|_| bad())?,
        skip: skip.parse().map_err(|_| bad())?,
    })
}

struct SortedEntry<'a> {
    labels: &'a BTreeMap<String, String>,
    key: String,
    entry: &'a StreamEntry,
}

/// Flattens streams and sorts the entries by timestamp (asc or desc), then by label set and
/// line for a stable order.
fn merge_streams(streams: &[StreamResult], asc: bool) -> Vec<SortedEntry<'_>> {
    let mut out: Vec<SortedEntry<'_>> = streams
        .iter()
        .flat_map(|s| {
            let key = label_set_string(&s.labels);
            s.entries.iter().map(move |e| SortedEntry {
                labels: &s.labels,
                key: key.clone(),
                entry: e,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        let (ta, tb) = (a.entry.timestamp, b.entry.timestamp);
        let by_time = if asc { ta.cmp(&tb) } else { tb.cmp(&ta) };
        by_time
            .then_with(|| a.key.cmp(&b.key))
            .then_with(|| a.entry.line.cmp(&b.entry.line))
    });
    out
}

/// Normalizes one Loki entry into a `LogRecord`: body = line, resource = stream labels
/// (`service_name` → `service.name` and other known OpenTelemetry keys mapped back),
/// attributes = structured metadata + parsed labels without the fields mapped below, severity
/// from `severity_number`, `severity_text`, `detected_level` or the `level` label, trace id /
/// span id / flags from `trace_id` / `span_id` / `trace_flags` (or `flags`).
pub fn entry_to_record(labels: &BTreeMap<String, String>, e: &StreamEntry) -> LogRecord {
    let mut resource = Attributes::new();
    for (k, v) in labels {
        let key = RESOURCE_LABEL_KEYS
            .iter()
            .find(|(l, _)| l == k)
            .map(|(_, r)| (*r).to_owned())
            .unwrap_or_else(|| k.clone());
        resource.insert(key, AttrValue::String(v.clone()));
    }
    let mut meta: BTreeMap<String, String> = e.parsed.clone();
    meta.extend(
        e.structured_metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    let mut take = |keys: &[&str]| -> String {
        let mut out = String::new();
        for k in keys {
            if let Some(v) = meta.remove(*k) {
                if out.is_empty() {
                    out = v;
                }
            }
        }
        out
    };
    let trace_id = take(&[METADATA_TRACE_ID]).to_lowercase();
    let span_id = take(&[METADATA_SPAN_ID]).to_lowercase();
    let trace_flags = take(&[METADATA_TRACE_FLAGS, "flags"]).parse::<u8>().ok();
    let observed = take(&[METADATA_OBSERVED_TIMESTAMP]);
    let sev_num = take(&[METADATA_SEVERITY_NUMBER]);
    let sev_text = take(&[METADATA_SEVERITY_TEXT]);
    let detected = take(&["detected_level"]);
    let severity = match sev_num.parse::<u8>() {
        Ok(n @ 1..=24) => Severity::from_number(n),
        _ => [
            sev_text.as_str(),
            detected.as_str(),
            labels.get("level").map_or("", String::as_str),
        ]
        .iter()
        .filter(|t| !t.trim().is_empty() && t.trim().parse::<u8>().is_err())
        .find_map(|t| t.parse::<Severity>().ok())
        .unwrap_or(Severity::Info),
    };
    let severity_text = if sev_text.is_empty() {
        severity.text().to_owned()
    } else {
        sev_text
    };
    LogRecord {
        timestamp: e.timestamp,
        observed_timestamp: parse_unix_nano(&observed)
            .ok()
            .filter(|_| !observed.is_empty()),
        severity,
        severity_text,
        body: e.line.clone(),
        attributes: meta
            .into_iter()
            .map(|(k, v)| (k, AttrValue::String(v)))
            .collect(),
        resource,
        trace_id: (!trace_id.is_empty()).then_some(trace_id),
        span_id: (!span_id.is_empty()).then_some(span_id),
        trace_flags,
    }
}

#[async_trait]
impl LogQueryClient for LogQuery {
    /// Runs a range query and returns normalized records in the requested order (default newest
    /// first) merged across streams. Pagination uses an opaque cursor `<last ns>:<count at
    /// that ns>`, so entries sharing a nanosecond are neither repeated nor skipped. `total` is
    /// not available.
    async fn search(&self, query: &str, opts: SearchOptions) -> Result<LogSearchResult> {
        let limit = if opts.limit == 0 { 100 } else { opts.limit };
        let asc = opts.order == SortOrder::Asc;
        let mut ro = QueryRangeOptions {
            start: opts.start,
            end: opts.end,
            direction: Some(if asc {
                Direction::Forward
            } else {
                Direction::Backward
            }),
            ..Default::default()
        };
        let cursor = match &opts.cursor {
            Some(c) => {
                let cur = decode_cursor(c)?;
                let boundary = from_unix_nanos(cur.ts);
                if asc {
                    ro.start = Some(boundary); // inclusive
                    if ro.end.is_none() {
                        ro.end = Some(SystemTime::now() + Duration::from_secs(60));
                    }
                } else {
                    ro.end = Some(boundary + Duration::from_nanos(1)); // end is exclusive
                    if ro.start.is_none() {
                        ro.start = Some(boundary - Duration::from_secs(3600));
                    }
                }
                Some(cur)
            }
            None => None,
        };
        let skip = cursor.map_or(0, |c| c.skip);
        let fetch = (limit + skip).min(MAX_ENTRIES_PER_QUERY);
        ro.limit = fetch;
        let direction = ro.direction;
        let logql = to_logql(query);
        let mut streams = self.client.query_range(&logql, ro).await?.streams;
        let (truncated, boundary) = {
            let merged = merge_streams(&streams, asc);
            (
                merged.len() >= fetch,
                merged.last().map(|e| e.entry.timestamp),
            )
        };
        if let Some(boundary) = boundary.filter(|_| truncated) {
            // Loki cut the result inside the boundary nanosecond in its own order: fetch that
            // nanosecond completely (start=T, end=T+1) so the page and the cursor's skip count
            // are deterministic.
            let exact = self
                .client
                .query_range(
                    &logql,
                    QueryRangeOptions {
                        start: Some(boundary),
                        end: Some(boundary + Duration::from_nanos(1)),
                        limit: MAX_ENTRIES_PER_QUERY,
                        direction,
                        ..Default::default()
                    },
                )
                .await?;
            for s in &mut streams {
                s.entries.retain(|e| e.timestamp != boundary);
            }
            streams.extend(exact.streams);
        }
        let mut entries = merge_streams(&streams, asc);
        if let Some(cur) = cursor {
            let skipped = entries
                .iter()
                .take(cur.skip)
                .take_while(|e| to_unix_nanos(e.entry.timestamp) == cur.ts)
                .count();
            entries.drain(..skipped);
        }
        let full = truncated || entries.len() > limit;
        entries.truncate(limit);
        let records = entries
            .iter()
            .map(|e| entry_to_record(e.labels, e.entry))
            .collect();
        let mut next_cursor = None;
        if full {
            if let Some(last) = entries.last() {
                let last_ts = to_unix_nanos(last.entry.timestamp);
                let mut n = entries
                    .iter()
                    .rev()
                    .take_while(|e| to_unix_nanos(e.entry.timestamp) == last_ts)
                    .count();
                if let Some(cur) = cursor.filter(|c| c.ts == last_ts) {
                    n += cur.skip;
                }
                next_cursor = Some(encode_cursor(SearchCursor {
                    ts: last_ts,
                    skip: n,
                }));
            }
        }
        Ok(LogSearchResult {
            records,
            next_cursor,
            total: None,
        })
    }

    /// Follows new records matching `query` over the WebSocket tail (see
    /// [`LokiClient::tail`]). `start` defaults to now (live only).
    async fn tail(
        &self,
        query: &str,
        on_record: RecordCallback,
        opts: TailOptions,
    ) -> Result<TailHandle> {
        let session = self
            .client
            .tail(
                &to_logql(query),
                move |frame| {
                    for st in &frame.streams {
                        for e in &st.entries {
                            on_record(entry_to_record(&st.labels, e));
                        }
                    }
                },
                LiveTailOptions {
                    start: opts.start,
                    ..Default::default()
                },
            )
            .await?;
        let handle = TailHandle::new();
        let h = handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = h.stopped() => session.stop().await,
                _ = session.wait() => {}
            }
        });
        Ok(handle)
    }

    /// Label sets of the streams seen in the last hour (series over `{service_name=~".+"}`),
    /// rendered as `{a="b", c="d"}` and sorted.
    async fn list_streams(&self) -> Result<Vec<String>> {
        let series = self
            .client
            .series(
                &[STREAMS_SELECTOR],
                SeriesOptions {
                    since: Some(Duration::from_secs(3600)),
                    ..Default::default()
                },
            )
            .await?;
        let mut out: Vec<String> = series.iter().map(label_set_string).collect();
        out.sort();
        Ok(out)
    }

    /// Label names (`labels` with Loki's default window), sorted.
    async fn list_labels(&self) -> Result<Vec<String>> {
        let mut labels = self.client.labels(LabelsOptions::default()).await?;
        labels.sort();
        Ok(labels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors() {
        let c = SearchCursor { ts: 42, skip: 3 };
        assert_eq!(decode_cursor(&encode_cursor(c)).unwrap(), c);
        assert!(decode_cursor("!!").is_err());
        let enc = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
        assert!(decode_cursor(&enc("nocolon")).is_err());
        assert!(decode_cursor(&enc("x:1")).is_err());
        assert!(decode_cursor(&enc("1:-1")).is_err());
        assert!(decode_cursor(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0xff, 0xfe])
        )
        .is_err());
        assert_eq!(to_logql("  {a=\"b\"} "), "{a=\"b\"}");
        assert_eq!(to_logql("hi"), r#"{service_name=~".+"} |= "hi""#);
    }
}
