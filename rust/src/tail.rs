//! Live tail over Loki's WebSocket endpoint (`GET /loki/api/v1/tail`).
//!
//! The handshake is an HTTP/1.1 upgrade made with the same TLS, proxy and timeout settings as
//! the HTTP transport; frames are read with `tokio-tungstenite`.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;

use crate::client::{LokiClient, HEADER_ENCODING_FLAGS, HEADER_TENANT};
use crate::core::redact::truncate;
use crate::core::transport::MAX_VENDOR_MESSAGE;
use crate::core::{CommonConfig, Error, ErrorCode, Result};
use crate::query::{decode_streams, is_log_query, parse_unix_nano, StreamResult};
use crate::util::{format_nanos, status_error};

/// Tail path.
pub(crate) const PATH_TAIL: &str = "/loki/api/v1/tail";

/// Loki's maximum tail `delay_for`.
pub const MAX_TAIL_DELAY: Duration = Duration::from_secs(5);

/// Bound of one WebSocket tail frame.
const MAX_TAIL_FRAME: usize = 64 << 20;

/// Options of [`LokiClient::tail`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveTailOptions {
    /// Replays entries from `start` before streaming live ones. `None` means "live only"
    /// (start = now); without it Loki would replay the last hour.
    pub start: Option<SystemTime>,
    /// Bounds the replayed entries (0 = Loki default 100).
    pub limit: usize,
    /// Delays delivery to let late entries arrive (0 to 5 s, whole seconds).
    pub delay_for: Duration,
}

/// One message of a live tail.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TailFrame {
    /// The new entries, categorized like `query_range` results.
    pub streams: Vec<StreamResult>,
    /// Entries Loki dropped because the tailer fell behind.
    pub dropped_entries: Vec<DroppedEntry>,
}

/// Identifies an entry dropped by a slow tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedEntry {
    /// Stream labels.
    pub labels: BTreeMap<String, String>,
    /// Timestamp of the dropped entry.
    pub timestamp: SystemTime,
}

/// A running live tail. Cheap to clone; clones control the same tail.
#[derive(Clone)]
pub struct TailSession {
    inner: Arc<TailInner>,
}

struct TailInner {
    cancel: CancellationToken,
    done: watch::Receiver<bool>,
    err: Mutex<Option<Error>>,
    dropped: AtomicU64,
}

impl std::fmt::Debug for TailSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailSession")
            .field("done", &self.is_done())
            .field("dropped", &self.dropped_count())
            .finish()
    }
}

impl TailSession {
    /// Asks the tail to stop (close code 1000) without waiting. Idempotent.
    pub fn cancel(&self) {
        self.inner.cancel.cancel();
    }

    /// Closes the WebSocket (close code 1000) and waits for the reader to finish. Idempotent.
    pub async fn stop(&self) {
        self.cancel();
        self.wait().await;
    }

    /// Resolves when the tail ended (stop, server close or error).
    pub async fn wait(&self) {
        let mut rx = self.inner.done.clone();
        let _ = rx.wait_for(|d| *d).await;
    }

    /// Whether the tail has ended.
    pub fn is_done(&self) -> bool {
        *self.inner.done.borrow()
    }

    /// Why the tail ended: `None` after stop or a normal close, `VALIDATION` when Loki closed
    /// it for a bad query (for example a metric query), `UNAVAILABLE` on connection errors.
    pub fn err(&self) -> Option<Error> {
        self.inner
            .err
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Number of entries Loki reported as dropped.
    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    fn set_err(&self, err: Error) {
        let mut slot = self.inner.err.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(err);
        }
    }
}

/// Decodes one tail frame (`{"streams":[...],"dropped_entries":[...]}`).
pub(crate) fn decode_tail_frame(data: &[u8]) -> std::result::Result<TailFrame, String> {
    let v: Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
    let mut frame = TailFrame::default();
    if let Some(s) = v.get("streams").filter(|s| !s.is_null()) {
        frame.streams = decode_streams(s)?;
    }
    if let Some(d) = v.get("dropped_entries").and_then(Value::as_array) {
        for item in d {
            let labels = item
                .get("labels")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                        .collect()
                })
                .unwrap_or_default();
            let ts = item
                .get("timestamp")
                .and_then(Value::as_str)
                .ok_or_else(|| "dropped entry timestamp is missing".to_owned())?;
            let timestamp = parse_unix_nano(ts).map_err(|e| e.message().to_owned())?;
            frame
                .dropped_entries
                .push(DroppedEntry { labels, timestamp });
        }
    }
    Ok(frame)
}

/// Whether a close reason is Loki rejecting the query.
fn is_tail_rejection(reason: &str) -> bool {
    let r = reason.to_lowercase();
    r.contains("only log selector") || r.contains("parse error") || r.contains("not supported")
}

/// HTTP/1.1-only client with the transport's TLS, proxy and connect settings.
fn ws_http_client(config: &CommonConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .connect_timeout(config.connect_timeout())
        .use_rustls_tls();
    let tls = config.tls();
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let mut ca_pem = Vec::new();
    if let Some(path) = &tls.ca_cert_path {
        ca_pem.extend(read_file(path, "CA certificate")?);
        ca_pem.push(b'\n');
    }
    if let Some(pem) = &tls.ca_cert_pem {
        ca_pem.extend_from_slice(pem.as_bytes());
    }
    if !ca_pem.is_empty() {
        let certs = reqwest::Certificate::from_pem_bundle(&ca_pem)
            .map_err(|e| Error::config("CA certificate is not valid PEM").with_source(e))?;
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
        let mut pem = read_file(cert, "client certificate")?;
        pem.push(b'\n');
        pem.extend(read_file(key, "client key")?);
        let identity = reqwest::Identity::from_pem(&pem).map_err(|e| {
            Error::config("client certificate or key is not valid PEM").with_source(e)
        })?;
        builder = builder.identity(identity);
    }
    if let Some(proxy) = config.proxy() {
        let proxy = reqwest::Proxy::all(proxy)
            .map_err(|e| Error::config("proxy is not a valid URL").with_source(e.without_url()))?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(|e| Error::config("cannot build the HTTP client").with_source(e.without_url()))
}

fn read_file(path: &std::path::Path, what: &str) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        Error::config(format!("{what} file {} cannot be read", path.display())).with_source(e)
    })
}

impl LokiClient {
    /// Streams new entries matching a log query over a WebSocket (`GET /loki/api/v1/tail`).
    /// `on_entries` is called from one task for each frame. The handshake carries the auth,
    /// tenant and categorize-labels headers; handshake failures are returned as errors (401 →
    /// `AUTHENTICATION`, 400 → `VALIDATION`). Metric queries and `delay_for` above 5 s are
    /// rejected client side. The tail runs until [`TailSession::stop`], the client is closed or
    /// the server closes it. Must be called inside a tokio runtime.
    pub async fn tail<F>(
        &self,
        logql: &str,
        on_entries: F,
        opts: LiveTailOptions,
    ) -> Result<TailSession>
    where
        F: Fn(TailFrame) + Send + Sync + 'static,
    {
        const OP: &str = "tail";
        if !is_log_query(logql) {
            return Err(Error::validation(
                "tail: only log queries (starting with a {...} selector) can be tailed",
            ));
        }
        if opts.delay_for > MAX_TAIL_DELAY {
            return Err(Error::validation("tail: delayFor must be between 0 and 5s"));
        }
        if self.is_closed() {
            return Err(Error::cancelled("tail: client is closed"));
        }
        let config = self.config().common();
        let mut url = config.url().clone();
        let base = url.path().trim_end_matches('/').to_owned();
        url.set_path(&format!("{base}{PATH_TAIL}"));
        {
            let mut q = url.query_pairs_mut();
            q.clear();
            q.append_pair("query", logql);
            q.append_pair(
                "start",
                &format_nanos(opts.start.unwrap_or_else(SystemTime::now)),
            );
            if opts.limit > 0 {
                q.append_pair("limit", &opts.limit.to_string());
            }
            if !opts.delay_for.is_zero() {
                q.append_pair("delay_for", &opts.delay_for.as_secs().to_string());
            }
        }
        let http = ws_http_client(config)?;
        let key = base64::engine::general_purpose::STANDARD.encode(rand::random::<[u8; 16]>());
        let mut req = http
            .get(url)
            .header("user-agent", self.transport().user_agent());
        for (k, v) in config.headers() {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(auth) = self.authorization() {
            req = req.header("authorization", auth.expose_secret());
        }
        if let Some(t) = self.tenant_id() {
            req = req.header(HEADER_TENANT, t);
        }
        req = req
            .header(HEADER_ENCODING_FLAGS, "categorize-labels")
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", &key);
        let handshake = config.connect_timeout() + config.request_timeout();
        let resp = match tokio::time::timeout(handshake, req.send()).await {
            Err(_) => {
                return Err(Error::timeout(format!(
                    "{OP}: handshake timeout of {} ms exceeded",
                    handshake.as_millis()
                )))
            }
            Ok(Err(e)) => {
                return Err(Error::unavailable(format!(
                    "{OP}: websocket handshake failed: {}",
                    self.transport().redact(&e.without_url().to_string())
                )))
            }
            Ok(Ok(r)) => r,
        };
        let status = resp.status().as_u16();
        if status != 101 {
            let request_id = resp
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let body = resp.bytes().await.unwrap_or_default();
            return Err(status_error(
                self.transport(),
                OP,
                status,
                &body,
                request_id.as_deref(),
            ));
        }
        let expected = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        let accept = resp
            .headers()
            .get("sec-websocket-accept")
            .and_then(|v| v.to_str().ok());
        if accept != Some(expected.as_str()) {
            return Err(Error::unavailable(format!(
                "{OP}: websocket handshake failed: invalid Sec-WebSocket-Accept"
            )));
        }
        let upgraded = resp.upgrade().await.map_err(|e| {
            Error::unavailable(format!(
                "{OP}: websocket upgrade failed: {}",
                truncate(&e.without_url().to_string(), MAX_VENDOR_MESSAGE)
            ))
        })?;
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_TAIL_FRAME);
        ws_config.max_frame_size = Some(MAX_TAIL_FRAME);
        let ws = WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(ws_config)).await;

        let (done_tx, done_rx) = watch::channel(false);
        let session = TailSession {
            inner: Arc::new(TailInner {
                cancel: CancellationToken::new(),
                done: done_rx,
                err: Mutex::new(None),
                dropped: AtomicU64::new(0),
            }),
        };
        let closing = session.clone();
        self.on_close(move || Box::pin(async move { closing.stop().await }));
        let reader = session.clone();
        tokio::spawn(async move {
            read_loop(ws, &reader, on_entries).await;
            let _ = done_tx.send(true);
        });
        Ok(session)
    }
}

async fn read_loop<S, F>(mut ws: WebSocketStream<S>, session: &TailSession, on_entries: F)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Fn(TailFrame),
{
    loop {
        let msg = tokio::select! {
            biased;
            _ = session.inner.cancel.cancelled() => {
                let _ = ws.close(Some(CloseFrame { code: CloseCode::Normal, reason: "".into() })).await;
                return;
            }
            msg = ws.next() => msg,
        };
        let data: Vec<u8> = match msg {
            None => return,
            Some(Ok(Message::Text(t))) => t.as_bytes().to_vec(),
            Some(Ok(Message::Binary(b))) => b.to_vec(),
            Some(Ok(Message::Close(frame))) => {
                if let Some(f) = frame {
                    let reason = f.reason.trim().to_owned();
                    if is_tail_rejection(&reason) {
                        session.set_err(Error::validation(format!(
                            "tail: closed by Loki: {}",
                            truncate(&reason, MAX_VENDOR_MESSAGE)
                        )));
                    } else if !matches!(f.code, CloseCode::Normal | CloseCode::Away) {
                        session.set_err(Error::unavailable(format!(
                            "tail: closed by Loki (code {}): {}",
                            u16::from(f.code),
                            truncate(&reason, MAX_VENDOR_MESSAGE)
                        )));
                    }
                }
                return;
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => {
                if !session.inner.cancel.is_cancelled() {
                    session.set_err(Error::new(
                        ErrorCode::Unavailable,
                        format!("tail: connection lost: {e}"),
                    ));
                }
                return;
            }
        };
        match decode_tail_frame(&data) {
            Ok(frame) => {
                if !frame.dropped_entries.is_empty() {
                    session
                        .inner
                        .dropped
                        .fetch_add(frame.dropped_entries.len() as u64, Ordering::Relaxed);
                }
                let _ = catch_unwind(AssertUnwindSafe(|| on_entries(frame)));
            }
            Err(reason) => {
                session.set_err(Error::unavailable(format!("tail: invalid frame: {reason}")));
                let _ = ws
                    .close(Some(CloseFrame {
                        code: CloseCode::Unsupported,
                        reason: "invalid frame".into(),
                    }))
                    .await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_reasons() {
        assert!(is_tail_rejection("only log selector is supported"));
        assert!(is_tail_rejection("Parse error at line 1"));
        assert!(!is_tail_rejection("bye"));
    }
}
