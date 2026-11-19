# grow-grafana-loki (Rust)

Rust SDK for Grafana Loki by [Grow Codes](https://grow.codes). Async on tokio, with an optional
blocking client. Tested against `grafana/loki:3.7.10`.

```toml
[dependencies]
grow-grafana-loki = "0.1.0"
# blocking client:   features = ["blocking"]
# test fakes/runner: features = ["testing"] (dev-dependency)
```

```rust,no_run
use std::time::Duration;
use grow_grafana_loki::{Entry, LogQL, LokiClient, LokiConfig, PushOptions, QueryRangeOptions, Stream};

#[tokio::main]
async fn main() -> grow_grafana_loki::Result<()> {
    // explicit > GROW_GRAFANA_LOKI_* > LOKI_URL, LOKI_TENANT_ID, ... > defaults
    let client = LokiClient::new(LokiConfig::from_env()?)?;
    println!("{:?}", client.health().await?);

    // Labels outside the allowlist ("app") become structured metadata (cardinality guard).
    let stream = Stream::new([("service_name", "checkout"), ("level", "error"), ("app", "web")])
        .entry(Entry::new(r#"{"msg":"payment failed","status":502}"#).metadata("request_id", "r-1"));
    client.push(&[stream], PushOptions::default()).await?; // or PushOptions::protobuf()

    let q = LogQL::selector([("service_name", "checkout")])
        .line_contains("payment")
        .json()
        .where_("status", ">=", 500)
        .build()?;
    let res = client
        .query_range(&q, QueryRangeOptions { since: Some(Duration::from_secs(300)), ..Default::default() })
        .await?;
    println!("{} entries", res.entry_count());
    client.close().await;
    Ok(())
}
```

- Push (JSON, gzip, protobuf + Snappy), `query`, `query_range`, `labels`, `label_values`,
  `series`, `tail` (WebSocket), `index_stats`, `volume`, `volume_range`, `format_query`, `ready`,
  `build_info`, `with_tenant` (`X-Scope-OrgID`).
- Logging category: `new_log_client` / `new_otlp_log_client` (batching `LogClient`, `Logger`,
  redaction, `tracing` layer and `log` bridge) and `log_query()` (`search` with cursors, `tail`,
  `list_streams`, `list_labels`, `wait_for_log`).
- Errors: one `Error` enum with `code()`, `http_status()`, `request_id()`, `is_retryable()`,
  `retry_after()`; messages never contain credentials.
- Retries with full-jitter backoff and `Retry-After`, connect/request/total timeouts,
  cancellation (drop the future or pass a `CancellationToken`), request hooks, `tracing` debug
  events.
- MSRV 1.88. Licence Apache-2.0. Protobuf push messages are hand-encoded from the field numbers
  of Loki's Apache-2.0 `pkg/push/push.proto`. Documentation:
  https://grow-codes-packages.github.io/grafana-loki/

Development: `make deps build lint test coverage license pack docs` (see `Makefile`).
