//! Shared helpers for the Loki service tests: recorded fixtures, mock clients and a tiny
//! WebSocket server for tail tests.
#![allow(dead_code, clippy::result_large_err)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use grow_grafana_loki as sdk;
use sdk::core::{RandomSource, RetryPolicy};
use sdk::{LokiClient, LokiConfig, LokiConfigBuilder};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use wiremock::ResponseTemplate;

/// A recorded response from the real Loki 3.7.10 container.
pub struct Fixture {
    pub json: Value,
}

impl Fixture {
    pub fn status(&self) -> u16 {
        self.json["response"]["status"].as_u64().unwrap() as u16
    }

    /// The response body bytes.
    pub fn body(&self) -> Vec<u8> {
        match &self.json["response"]["body"] {
            Value::Null => Vec::new(),
            Value::String(s) => s.clone().into_bytes(),
            Value::Object(m) if m.contains_key("base64") => {
                base64::engine::general_purpose::STANDARD
                    .decode(m["base64"].as_str().unwrap())
                    .unwrap()
            }
            other => other.to_string().into_bytes(),
        }
    }

    /// The WebSocket frame of a tail fixture.
    pub fn frame(&self) -> String {
        self.json["response"]["frame"].to_string()
    }

    /// A wiremock response replaying the fixture.
    pub fn response(&self) -> ResponseTemplate {
        let ct = self.json["response"]["contentType"]
            .as_str()
            .unwrap_or("text/plain")
            .to_owned();
        ResponseTemplate::new(self.status()).set_body_raw(self.body(), &ct)
    }
}

pub fn fixture(name: &str) -> Fixture {
    let path = format!(
        "{}/tests/fixtures/loki/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    Fixture {
        json: serde_json::from_str(&text).unwrap(),
    }
}

/// Retry policy with deterministic jitter and short backoffs.
pub fn fast_retry(max_retries: u32) -> RetryPolicy {
    RetryPolicy {
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
        ..RetryPolicy::default()
    }
    .with_random(RandomSource::fixed(0.0))
    .with_max_retries(max_retries)
}

pub fn config(url: &str) -> LokiConfigBuilder {
    LokiConfig::builder().url(url).retry(fast_retry(0))
}

pub fn client(url: &str) -> LokiClient {
    LokiClient::new(config(url).build().unwrap()).unwrap()
}

/// What the WebSocket test server saw in the handshake.
#[derive(Debug, Default, Clone)]
pub struct Handshake {
    pub uri: String,
    pub headers: Vec<(String, String)>,
}

impl Handshake {
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    }
}

/// Server behaviour after the upgrade.
#[derive(Clone)]
pub enum WsScript {
    /// Send these text frames, then wait for the client to close.
    Frames(Vec<String>),
    /// Send these frames, then close with code and reason.
    FramesThenClose(Vec<String>, u16, String),
    /// Send these frames as binary messages, then wait.
    Binary(Vec<String>),
    /// Send a ping and these frames, then drop the TCP connection without closing.
    FramesThenDrop(Vec<String>),
}

/// Starts a one-connection WebSocket server; returns its http URL and the captured handshake.
pub async fn ws_server(script: WsScript) -> (String, Arc<Mutex<Handshake>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Handshake::default()));
    let captured = seen.clone();
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let callback = |req: &Request, resp: Response| {
            let mut h = captured.lock().unwrap();
            h.uri = req.uri().to_string();
            h.headers = req
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_owned()))
                .collect();
            Ok(resp)
        };
        let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
            return;
        };
        let send_all = |frames: Vec<String>, binary: bool| {
            frames
                .into_iter()
                .map(move |f| {
                    if binary {
                        Message::Binary(f.into_bytes().into())
                    } else {
                        Message::Text(f.into())
                    }
                })
                .collect::<Vec<_>>()
        };
        match script {
            WsScript::Frames(frames) => {
                for m in send_all(frames, false) {
                    let _ = ws.send(m).await;
                }
                while let Some(Ok(m)) = ws.next().await {
                    if m.is_close() {
                        break;
                    }
                }
            }
            WsScript::Binary(frames) => {
                for m in send_all(frames, true) {
                    let _ = ws.send(m).await;
                }
                while let Some(Ok(m)) = ws.next().await {
                    if m.is_close() {
                        break;
                    }
                }
            }
            WsScript::FramesThenClose(frames, code, reason) => {
                for m in send_all(frames, false) {
                    let _ = ws.send(m).await;
                }
                let _ = ws
                    .close(Some(CloseFrame {
                        code: CloseCode::from(code),
                        reason: reason.into(),
                    }))
                    .await;
                while let Some(Ok(_)) = ws.next().await {}
            }
            WsScript::FramesThenDrop(frames) => {
                let _ = ws.send(Message::Ping(vec![1u8].into())).await;
                for m in send_all(frames, false) {
                    let _ = ws.send(m).await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(ws);
            }
        }
    });
    (format!("http://{addr}"), seen)
}

/// A raw TCP server that answers every connection with `response` (HTTP text), then keeps the
/// socket open for `hold`.
pub async fn raw_server(response: &'static str, hold: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 4096];
                let _ = tcp.read(&mut buf).await;
                if !response.is_empty() {
                    let _ = tcp.write_all(response.as_bytes()).await;
                }
                tokio::time::sleep(hold).await;
            });
        }
    });
    format!("http://{addr}")
}

/// A closed local port.
pub fn closed_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}
