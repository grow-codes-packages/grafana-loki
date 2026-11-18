//! Grafana Loki Rust SDK end to end: configuration from the environment, health, push, LogQL
//! builder, range and metric queries, a live tail, the batching log client with the Logger
//! facade and search.
//!
//! ```sh
//! GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100 cargo run
//! ```
//!
//! Optional: `GROW_GRAFANA_LOKI_TENANT_ID` (X-Scope-OrgID), `GROW_GRAFANA_LOKI_USERNAME` /
//! `GROW_GRAFANA_LOKI_PASSWORD` or `GROW_GRAFANA_LOKI_BEARER_TOKEN` for gateways and Grafana
//! Cloud.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grow_grafana_loki::logging::{LogClient, LogQueryClient, SearchOptions, WaitOptions};
use grow_grafana_loki::{
    attrs, Entry, ErrorCode, LiveTailOptions, LogClientOptions, LogQL, LokiClient, LokiConfig,
    PushOptions, QueryOptions, QueryRangeOptions, Result, Stream,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    // CONFIG errors name the variable, never its value; Debug output redacts credentials.
    let config = LokiConfig::from_env()?;
    println!("config: {config:?}");
    let client = LokiClient::new(config)?;

    let health = client.health().await?;
    println!(
        "health: healthy={} status={}",
        health.healthy, health.status
    );
    println!("loki version: {}", client.server_version().await?);

    // Push one stream. "app" is not in the label allowlist, so the cardinality guard sends it
    // as structured metadata.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let service = format!("sdk-example-rust-{nanos}");
    let stream = Stream::new([
        ("service_name", service.as_str()),
        ("level", "error"),
        ("app", "checkout"),
    ])
    .entry(Entry::new(r#"{"msg":"payment failed","status":502}"#).metadata("request_id", "req-1"))
    .entry(Entry::new(r#"{"msg":"payment ok","status":200}"#));
    match client.push(&[stream], PushOptions::json(true)).await {
        Err(e) if e.code() == ErrorCode::Authentication => {
            eprintln!("check the tenant id or gateway credentials");
            return Err(e);
        }
        other => other?,
    }

    // Build LogQL safely and run a range query.
    let selector = LogQL::selector([("service_name", service.as_str())]);
    let query = selector
        .line_contains("payment")
        .json()
        .where_("status", ">=", 500)
        .build()?;
    let res = client
        .query_range(
            &query,
            QueryRangeOptions {
                since: Some(Duration::from_secs(300)),
                limit: 10,
                ..Default::default()
            },
        )
        .await?;
    for s in &res.streams {
        for e in &s.entries {
            println!(
                "{} metadata={:?} parsed={:?}",
                e.line, e.structured_metadata, e.parsed
            );
        }
    }

    // Metric query.
    let vec = client
        .query(
            &format!("sum(count_over_time({selector}[5m]))"),
            QueryOptions::default(),
        )
        .await?;
    if let Some(sample) = vec.vector.first() {
        println!("lines in the last 5m: {}", sample.value.value);
    }

    // Live tail: print what arrives while the example runs.
    let tail = client
        .tail(
            &selector.to_string(),
            |frame| {
                for s in &frame.streams {
                    for e in &s.entries {
                        println!("tail: {}", e.line);
                    }
                }
            },
            LiveTailOptions::default(),
        )
        .await?;

    // Batching log client + Logger facade (records are redacted before export).
    let logs = client.new_log_client(
        LogClientOptions::default()
            .resource_attribute("service.name", service.clone())
            .resource_attribute("deployment.environment", "example"),
    )?;
    let logger = logs.logger(Some("checkout"), attrs! {"region" => "eu"});
    logger.info(
        "order placed",
        attrs! {"order_id" => 42, "password" => "never-sent"},
    );
    logs.flush(None).await?;

    // Category search: normalized LogRecords, newest first.
    let found = client
        .log_query()
        .wait_for_log(
            "order placed",
            WaitOptions::default().timeout(Duration::from_secs(10)),
        )
        .await?;
    println!(
        "found: {} severity={} attributes={:?}",
        found.body, found.severity, found.attributes
    );
    let page = client
        .log_query()
        .search(&selector.to_string(), SearchOptions::default().limit(5))
        .await?;
    println!("search returned {} records", page.records.len());

    tokio::time::sleep(Duration::from_secs(1)).await;
    tail.stop().await;
    client.close().await; // flushes log clients, stops tails
    Ok(())
}
