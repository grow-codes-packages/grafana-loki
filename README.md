# Grow Codes SDK for Grafana Loki

[![CI](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml/badge.svg)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/grow-codes-packages/grafana-loki?sort=semver)](https://github.com/grow-codes-packages/grafana-loki/releases)
[![Docs](https://img.shields.io/badge/docs-grow--codes--packages.github.io-blue)](https://grow-codes-packages.github.io/grafana-loki/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Grow Codes](https://img.shields.io/badge/Grow%20Codes-grow.codes-2ea44f)](https://grow.codes)

| Language | Package | Coverage |
| - | - | - |
| TypeScript | [![npm](https://img.shields.io/npm/v/@grow-codes/grafana-loki)](https://www.npmjs.com/package/@grow-codes/grafana-loki) | [![TypeScript coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-typescript.json)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml) |
| Rust | [![crates.io](https://img.shields.io/crates/v/grow-grafana-loki)](https://crates.io/crates/grow-grafana-loki) [![docs.rs](https://img.shields.io/docsrs/grow-grafana-loki)](https://docs.rs/grow-grafana-loki) | [![Rust coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-rust.json)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml) |
| Go | [![Go Reference](https://pkg.go.dev/badge/github.com/grow-codes-packages/grafana-loki/go.svg)](https://pkg.go.dev/github.com/grow-codes-packages/grafana-loki/go) | [![Go coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-go.json)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml) |
| Python | [![PyPI](https://img.shields.io/pypi/v/grow-grafana-loki)](https://pypi.org/project/grow-grafana-loki/) | [![Python coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-python.json)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml) |
| Java | [![Maven Central](https://img.shields.io/maven-central/v/codes.grow.packages/grafana-loki)](https://central.sonatype.com/artifact/codes.grow.packages/grafana-loki) | [![Java coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-java.json)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml) |

Open-source client libraries for [Grafana Loki](#trademarks-and-disclaimer) in **TypeScript, Rust, Go,
Python and Java** — one API, five idiomatic packages, tested against a real Grafana Loki in Docker.

- **Typed client** with timeouts, retries with jitter, `Retry-After`, cancellation and one error
  model (`LokiError` with canonical codes) in every language.
- **Secrets stay secret**: credentials are redacted in `toString`, logs, hooks and error messages.
- **Loki API**: push (JSON, gzip, protobuf + Snappy, OTLP) with structured metadata and a
  cardinality guard, LogQL queries, labels, series, index stats and volume, a WebSocket live tail,
  a LogQL builder with escaping, status and multi-tenancy (`X-Scope-OrgID`).
- **Logging**: batched, compressed log shipping with back-pressure, redaction, a logger facade and
  adapters for the common logging frameworks of each language; log search with a cursor that never
  repeats or skips entries.

## Contents

- [Supported service version](#supported-service-version)
- [Install](#install)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Function overview](#function-overview)
- [Testing with Docker](#testing-with-docker)
- [Compatibility](#compatibility)
- [Contributing](#contributing)
- [Security](#security)
- [Licence](#licence)
- [Trademarks and disclaimer](#trademarks-and-disclaimer)
- [About Grow Codes](#about-grow-codes)

## Supported service version

Tested in CI against **`grafana/loki:3.7.10`** (the version pinned in the Grow Codes stack catalog),
single-tenant and multi-tenant (`auth_enabled: true`), plus Basic/Bearer authentication through a
gateway. Other Loki 3.x versions with the same HTTP API usually work, and hosted Loki (Grafana
Cloud) uses the same paths; see [Compatibility](#compatibility) and
[docs/compatibility.md](docs/compatibility.md).

## Install

| Language | Package | Install |
| - | - | - |
| TypeScript (Node.js ≥ 22.12) | `@grow-codes/grafana-loki` | `npm install @grow-codes/grafana-loki` |
| Rust (MSRV 1.85) | `grow-grafana-loki` | `cargo add grow-grafana-loki` |
| Go (1.26+) | `github.com/grow-codes-packages/grafana-loki/go` | `go get github.com/grow-codes-packages/grafana-loki/go` |
| Python (≥ 3.10) | `grow-grafana-loki` | `pip install grow-grafana-loki` |
| Java (17+) | `codes.grow.packages:grafana-loki` | see below |

Maven:

```xml
<dependency>
  <groupId>codes.grow.packages</groupId>
  <artifactId>grafana-loki</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle (Kotlin DSL):

```kotlin
implementation("codes.grow.packages:grafana-loki:0.1.0")
```

## Quickstart

Every package reads its configuration from the environment (`GROW_GRAFANA_LOKI_URL`,
`GROW_GRAFANA_LOKI_TENANT_ID`, …, see [Configuration](#configuration)) or from an explicit config
object. The examples check that Loki is ready, push a line with structured metadata and read it
back with the LogQL builder.

### TypeScript

```ts
import { LogQL, LokiClient } from "@grow-codes/grafana-loki";

const client = LokiClient.fromEnv();
try {
  const health = await client.health();
  console.log("healthy:", health.healthy, health.status, await client.serverVersion());

  // Push one stream; labels outside the allowlist go to structured metadata.
  await client.push([
    {
      labels: { service_name: "checkout", level: "error" },
      entries: [{ line: "payment failed", structuredMetadata: { order_id: "o-1" } }],
    },
  ]);

  // Read it back with a safely built LogQL query.
  const logql = LogQL.selector({ service_name: "checkout" }).lineContains("payment").build();
  const result = await client.queryRange(logql, { sinceMs: 5 * 60_000, limit: 10 });
  for (const s of result.streams)
    for (const e of s.entries) console.log(e.timestamp, e.line, e.structuredMetadata);
} finally {
  await client.close();
}
```

### Rust

```rust
use std::time::Duration;
use grow_grafana_loki::{Entry, LogQL, LokiClient, PushOptions, QueryRangeOptions, Stream};

#[tokio::main]
async fn main() -> grow_grafana_loki::Result<()> {
    let client = LokiClient::from_env()?;
    let health = client.health().await?;
    println!("healthy: {} {} {}", health.healthy, health.status, client.server_version().await?);

    // Push one stream; labels outside the allowlist go to structured metadata.
    let stream = Stream::new([("service_name", "checkout"), ("level", "error")])
        .entry(Entry::new("payment failed").metadata("order_id", "o-1"));
    client.push(&[stream], PushOptions::default()).await?;

    // Read it back with a safely built LogQL query.
    let logql = LogQL::selector([("service_name", "checkout")]).line_contains("payment").build()?;
    let opts = QueryRangeOptions { since: Some(Duration::from_secs(300)), limit: 10, ..Default::default() };
    for s in client.query_range(&logql, opts).await?.streams {
        for e in s.entries {
            println!("{:?} {} {:?}", e.timestamp, e.line, e.structured_metadata);
        }
    }
    client.close().await;
    Ok(())
}
```

### Go

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	loki "github.com/grow-codes-packages/grafana-loki/go"
)

func main() {
	ctx := context.Background()
	client, err := loki.NewClientFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	health, err := client.Health(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("healthy:", health.Healthy, health.Status)

	// Push one stream; labels outside the allowlist go to structured metadata.
	err = client.Push(ctx, []loki.Stream{{
		Labels:  map[string]string{"service_name": "checkout", "level": "error"},
		Entries: []loki.Entry{{Line: "payment failed", StructuredMetadata: map[string]string{"order_id": "o-1"}}},
	}}, loki.PushOptions{})
	if err != nil {
		log.Fatal(err) // errors.Is(err, loki.ErrAuthentication), loki.ErrValidation, ...
	}

	// Read it back with a safely built LogQL query.
	logql, err := loki.Selector(map[string]string{"service_name": "checkout"}).LineContains("payment").Build()
	if err != nil {
		log.Fatal(err)
	}
	res, err := client.QueryRange(ctx, logql, loki.QueryRangeOptions{Since: 5 * time.Minute, Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range res.Streams {
		for _, e := range s.Entries {
			fmt.Println(e.Timestamp, e.Line, e.StructuredMetadata)
		}
	}
}
```

### Python

```python
from grow_grafana_loki import LogQL, LokiClient, LokiEntry, LokiStream

with LokiClient.from_env() as client:
    health = client.health()
    print("healthy:", health.healthy, health.status, client.server_version())

    # Push one stream; labels outside the allowlist go to structured metadata.
    client.push([
        LokiStream(
            {"service_name": "checkout", "level": "error"},
            [LokiEntry("payment failed", structured_metadata={"order_id": "o-1"})],
        )
    ])

    # Read it back with a safely built LogQL query.
    logql = LogQL.selector({"service_name": "checkout"}).line_contains("payment").build()
    for stream in client.query_range(logql, since_s=300, limit=10).streams:
        for entry in stream.entries:
            print(entry.timestamp, entry.line, entry.structured_metadata)
```

### Java

```java
import codes.grow.packages.loki.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;

try (LokiClient client = LokiClient.fromEnv()) {
    var health = client.health();
    System.out.println("healthy: " + health.healthy() + " " + client.serverVersion());

    // Push one stream; labels outside the allowlist go to structured metadata.
    client.push(List.of(LokiStream.of(
        Map.of("service_name", "checkout", "level", "error"),
        LokiEntry.of(java.time.Instant.now(), "payment failed", Map.of("order_id", "o-1")))));

    // Read it back with a safely built LogQL query.
    String logql = LogQL.selector(Map.of("service_name", "checkout")).lineContains("payment").build();
    QueryResult result = client.queryRange(logql,
        QueryRangeOptions.builder().since(Duration.ofMinutes(5)).limit(10).build());
    for (StreamResult s : result.streams()) {
        for (StreamEntry e : s.entries()) {
            System.out.println(e.timestamp() + " " + e.line() + " " + e.structuredMetadata());
        }
    }
}
```

More complete programs live in [examples/](examples/) — one folder per language.

## Configuration

All settings can be passed in code or through environment variables. Precedence: **explicit value >
`GROW_GRAFANA_LOKI_*` variable > vendor variable > default.** A present but invalid value raises a
`ConfigError` that names the variable (never its value).

| Setting | Environment variables (in order) | Default |
| - | - | - |
| `url` | `GROW_GRAFANA_LOKI_URL`, `LOKI_URL` | `http://127.0.0.1:3100` |
| `tenantId` (`X-Scope-OrgID`) | `GROW_GRAFANA_LOKI_TENANT_ID`, `LOKI_TENANT_ID` | none (header not sent) |
| `username` (Basic) | `GROW_GRAFANA_LOKI_USERNAME`, `LOKI_USERNAME`, `LOKI_BASIC_AUTH_USER` | none |
| `password` (Basic, secret) | `GROW_GRAFANA_LOKI_PASSWORD`, `LOKI_PASSWORD`, `LOKI_BASIC_AUTH_PASSWORD` | none |
| `bearerToken` (secret) | `GROW_GRAFANA_LOKI_BEARER_TOKEN`, `LOKI_BEARER_TOKEN` | none |
| `multiTenantQueries` (allow `a\|b` tenants) | — | `false` |
| `allowedLabels` (cardinality guard allowlist) | — | `service_name`, `service_namespace`, `deployment_environment`, `host_name`, `level`, `job`, `instance`, `namespace`, `pod`, `container` |
| `disableLabelGuard` | — | `false` |
| `connectTimeout` | `GROW_GRAFANA_LOKI_CONNECT_TIMEOUT` | 10 s |
| `requestTimeout` (per attempt) | `GROW_GRAFANA_LOKI_REQUEST_TIMEOUT` | 30 s |
| `totalTimeout` (including retries) | `GROW_GRAFANA_LOKI_TOTAL_TIMEOUT` | 120 s |
| `retry.maxRetries` | `GROW_GRAFANA_LOKI_MAX_RETRIES` | 3 |
| `tls.caCertPath` | `GROW_GRAFANA_LOKI_CA_CERT` | system roots |
| `tls.clientCertPath` / `tls.clientKeyPath` | `GROW_GRAFANA_LOKI_CLIENT_CERT` / `GROW_GRAFANA_LOKI_CLIENT_KEY` | none |
| `tls.insecureSkipVerify` (tests only) | `GROW_GRAFANA_LOKI_INSECURE_SKIP_VERIFY` | `false` |
| `proxy` | `GROW_GRAFANA_LOKI_PROXY` | none (environment proxies are ignored) |
| `headers`, `userAgentSuffix`, `hooks`, `logger` | — | none |

Basic and Bearer are mutually exclusive, username and password go together. Loki has no
authentication of its own: credentials are for gateways and hosted Loki (Grafana Cloud: user =
instance id, password = access token); `auth_enabled: true` only makes the tenant header
mandatory. Durations accept milliseconds (`1500`) or units (`500ms`, `10s`, `1m30s`); booleans
accept `true/false/1/0/yes/no/on/off`. Field names follow each language's convention
(`tenant_id`, `bearer_token`, `request_timeout` in Python and Rust, `TenantID`, `BearerToken` in
Go). See [docs/configuration.md](docs/configuration.md) for the cardinality guard and the full
reference.

## Function overview

<!-- service:function-overview — the service fills this section; keep the heading. -->

The same functions exist in every language, named per language convention (`queryRange` in
TypeScript and Java, `query_range` in Python and Rust, `QueryRange` in Go). Every function is
documented with an example in all five languages in the
[documentation](https://grow-codes-packages.github.io/grafana-loki/functions/).

### Client and status

Liveness, authenticated round trip, server version, multi-tenancy and shutdown.

| Function | Loki API | Description |
| - | - | - |
| `fromEnv()` | — | client from `GROW_GRAFANA_LOKI_*` / `LOKI_*` |
| `health()` | `GET /ready` | `{healthy, status, details}`; never throws for an unhealthy or unreachable server |
| `ready()` | `GET /ready` | `true` when Loki is ready |
| `ping()` | `GET /loki/api/v1/labels?since=5m` | latency of an authenticated call (`AUTHENTICATION` on a missing tenant or bad credentials) |
| `buildInfo()` / `serverVersion()` | `GET /loki/api/v1/status/buildinfo` | version, revision, branch, … |
| `withTenant(id)` | — | client for another tenant (`X-Scope-OrgID`), shares the connections |
| `close()` | — | idempotent; flushes log clients, stops tails |

<details><summary>Examples in TypeScript, Rust, Go, Python and Java</summary>

TypeScript:

```ts
const info = await client.buildInfo();
const teamA = client.withTenant("team-a");
console.log(await client.ready(), info.version, await teamA.ping());
```

Rust:

```rust
let info = client.build_info().await?;
let team_a = client.with_tenant("team-a")?;
println!("{} {} {:?}", client.ready().await?, info.version, team_a.ping().await?);
```

Go:

```go
info, err := client.BuildInfo(ctx)
teamA, err := client.WithTenant("team-a")
latency, err := teamA.Ping(ctx)
```

Python:

```python
info = client.build_info()
team_a = client.with_tenant("team-a")
print(client.ready(), info.version, team_a.ping())
```

Java:

```java
BuildInfo info = client.buildInfo();
LokiClient teamA = client.withTenant("team-a");
System.out.println(client.ready() + " " + info.version() + " " + teamA.ping());
```

</details>

### Push

JSON (optionally gzip) or protobuf + Snappy push, structured metadata, label validation and the cardinality guard; OTLP as an alternative.

| Function | Loki API | Description |
| - | - | - |
| `push(streams, {format, gzip})` | `POST /loki/api/v1/push` | `LokiStream {labels, entries}`; entries have a timestamp (default now), a line and structured metadata; labels outside `allowedLabels` move to structured metadata |
| `pushOtlp(records, {gzip})` | `POST /otlp/v1/logs` | `LogRecord`s as OTLP/JSON; Loki maps resource attributes to labels itself |
| `CardinalityGuard`, `DEFAULT_ALLOWED_LABELS` | — | the guard on its own (`allowed`, `isAllowed`, `apply`) |

<details><summary>Examples in TypeScript, Rust, Go, Python and Java</summary>

TypeScript:

```ts
await client.push(
  [{ labels: { service_name: "api" }, entries: [{ line: "GET /", structuredMetadata: { request_id: "r-1" } }] }],
  { format: "protobuf" },
);
```

Rust:

```rust
let s = Stream::new([("service_name", "api")]).entry(Entry::new("GET /").metadata("request_id", "r-1"));
client.push(&[s], PushOptions::protobuf()).await?;
```

Go:

```go
err := client.Push(ctx, []loki.Stream{{Labels: map[string]string{"service_name": "api"},
	Entries: []loki.Entry{{Line: "GET /", StructuredMetadata: map[string]string{"request_id": "r-1"}}}}},
	loki.PushOptions{Format: loki.PushFormatProtobuf})
```

Python:

```python
client.push(
    [LokiStream({"service_name": "api"}, [LokiEntry("GET /", structured_metadata={"request_id": "r-1"})])],
    format="protobuf",
)
```

Java:

```java
client.push(
    List.of(LokiStream.of(Map.of("service_name", "api"),
        LokiEntry.of(Instant.now(), "GET /", Map.of("request_id", "r-1")))),
    PushOptions.of(PushFormat.PROTOBUF));
```

</details>

### Query

LogQL queries and the index APIs. Results keep index labels, structured metadata and parsed labels apart (categorized labels).

| Function | Loki API | Description |
| - | - | - |
| `query(logql, {time, limit, direction})` | `GET /loki/api/v1/query` | instant metric queries (log selectors are rejected client side) |
| `queryRange(logql, {start, end, since, step, interval, limit, direction})` | `GET /loki/api/v1/query_range` | streams or matrix; `start` inclusive, `end` exclusive, `limit` ≤ 5000 |
| `labels({start, end, since, query})` | `GET /loki/api/v1/labels` | label names |
| `labelValues(name, {…})` | `GET /loki/api/v1/label/{name}/values` | values of one label |
| `series(matchers, {start, end, since})` | `GET /loki/api/v1/series` | label sets of matching streams |
| `indexStats(query, {start, end})` | `GET /loki/api/v1/index/stats` | streams, chunks, bytes, entries (index only) |
| `volume(query, {…, targetLabels, aggregateBy})` | `GET /loki/api/v1/index/volume` | bytes per series or label |
| `volumeRange(query, {…, step})` | `GET /loki/api/v1/index/volume_range` | volume over time, always a matrix |
| `formatQuery(logql)` | `GET /loki/api/v1/format_query` | Loki's normalized LogQL |

<details><summary>Examples in TypeScript, Rust, Go, Python and Java</summary>

TypeScript:

```ts
const res = await client.queryRange('{service_name="api"} | request_id="r-1"', { sinceMs: 900_000 });
const levels = await client.labelValues("level");
const vol = await client.volumeRange('{service_name="api"}', { stepMs: 60_000 });
```

Rust:

```rust
let res = client.query_range(r#"{service_name="api"} | request_id="r-1""#, QueryRangeOptions::default()).await?;
let levels = client.label_values("level", LabelsOptions::default()).await?;
let vol = client.volume_range(r#"{service_name="api"}"#, VolumeOptions::default()).await?;
```

Go:

```go
res, err := client.QueryRange(ctx, `{service_name="api"} | request_id="r-1"`, loki.QueryRangeOptions{Since: 15 * time.Minute})
levels, err := client.LabelValues(ctx, "level", loki.LabelsOptions{})
vol, err := client.VolumeRange(ctx, `{service_name="api"}`, loki.VolumeOptions{Step: time.Minute})
```

Python:

```python
res = client.query_range('{service_name="api"} | request_id="r-1"', since_s=900)
levels = client.label_values("level")
vol = client.volume_range('{service_name="api"}', step_s=60)
```

Java:

```java
QueryResult res = client.queryRange("{service_name=\"api\"} | request_id=\"r-1\"",
    QueryRangeOptions.builder().since(Duration.ofMinutes(15)).build());
List<String> levels = client.labelValues("level");
QueryResult vol = client.volumeRange("{service_name=\"api\"}");
```

</details>

### Tail

Live tail over Loki's WebSocket API. Live only by default (`start` = now); auth and tenant headers go on the handshake.

| Function | Loki API | Description |
| - | - | - |
| `tail(logql, onEntries, {start, limit, delayFor})` | `GET /loki/api/v1/tail` (WebSocket) | callback per frame; dropped entries reported; stop with `stop()` or `close()` |

<details><summary>Examples in TypeScript, Rust, Go, Python and Java</summary>

TypeScript:

```ts
const tail = await client.tail('{service_name="api"}', (streams) => console.log(streams));
tail.stop();
```

Rust:

```rust
let tail = client.tail(r#"{service_name="api"}"#, |f: TailFrame| println!("{:?}", f.streams), LiveTailOptions::default()).await?;
tail.stop().await;
```

Go:

```go
tail, err := client.Tail(ctx, `{service_name="api"}`, func(f loki.TailFrame) { fmt.Println(f.Streams) }, loki.LiveTailOptions{})
defer tail.Stop()
```

Python:

```python
session = client.tail('{service_name="api"}', lambda frame: print(frame.streams))
session.stop()
```

Java:

```java
LokiTail tail = client.tail("{service_name=\"api\"}", frame -> System.out.println(frame.streams()));
tail.stop();
```

</details>

### LogQL builder

Builds LogQL with validated names and escaped literals (verified against the server).

| Function | Description |
| - | - |
| `LogQL.selector({...})`, `label` | stream selector and matchers |
| `lineContains`, `lineNotContains`, `lineMatches`, `lineNotMatches` | line filters |
| `json`, `logfmt`, `unpack`, `regexp`, `pattern`, `lineFormat`, `pipe` | parser and pipeline stages |
| `where(field, op, value)` | label filter (strings or numbers) |
| `build()`, `quote(text)` | render; escaping helper |

<details><summary>Examples in TypeScript, Rust, Go, Python and Java</summary>

TypeScript:

```ts
LogQL.selector({ app: "x" }).lineContains("err").json().where("status", ">=", 500).build();
// {app="x"} |= "err" | json | status >= 500
```

Rust:

```rust
LogQL::selector([("app", "x")]).line_contains("err").json().where_("status", ">=", 500).build()?;
```

Go:

```go
q, err := loki.Selector(map[string]string{"app": "x"}).LineContains("err").JSON().Where("status", ">=", 500).Build()
```

Python:

```python
LogQL.selector({"app": "x"}).line_contains("err").json().where("status", ">=", 500).build()
```

Java:

```java
LogQL.selector(Map.of("app", "x")).lineContains("err").json().where("status", ">=", 500).build();
```

</details>

### Logging category

The shared Grow Codes logging interface on top of Loki: a batching `LogClient` (push API or OTLP), logger facade, redaction, framework adapters and a `LogQueryClient`.

| Function | Description |
| - | - |
| `logClient()` / OTLP log client | batching log client (`send`, `sendBatch`, `flush`, `close`, `stats`, `logger`) with redaction and the cardinality guard |
| `LokiLogExporter`, `LokiOtlpExporter` | the exporters on their own |
| `logQueryClient()` → `search` | `query_range` merge-sorted across streams, opaque cursor that never repeats or skips entries sharing a nanosecond, free-text queries |
| `tail`, `listStreams`, `listLabels`, `waitForLog` | WebSocket tail per record, `series`, `labels`, polling helper |

<details><summary>Examples in TypeScript, Rust, Go, Python and Java</summary>

TypeScript:

```ts
const logs = client.logClient({ resource: { "service.name": "api" } });
logs.logger("http").warn("slow request", { ms: 1200 });
const page = await client.logQueryClient().search("slow request", { limit: 20 });
```

Rust:

```rust
let logs = client.new_log_client(LogClientOptions::default().resource_attribute("service.name", "api"))?;
logs.send(LogEntry::from("slow request"));
let page = client.log_query().search("slow request", SearchOptions::default().limit(20)).await?;
```

Go:

```go
logs, err := client.NewLogClient(loki.LogClientOptions{Resource: map[string]loki.AttrValue{"service.name": "api"}})
logs.Logger("http", nil).Warn(ctx, "slow request", "ms", 1200)
page, err := client.LogQuery().Search(ctx, "slow request", loki.SearchOptions{Limit: 20})
```

Python:

```python
logs = client.log_client(resource={"service.name": "api"})
logs.logger("http").warn("slow request", {"ms": 1200})
page = client.log_query().search("slow request", limit=20)
```

Java:

```java
BatchingLogClient logs = client.logClient();
logs.logger("http").warn("slow request", Map.of("ms", 1200));
LogSearchResult page = client.logQueryClient().search("slow request",
    SearchOptions.builder().limit(20).build());
```

</details>

<!-- /service:function-overview -->

## Testing with Docker

The integration suite runs every public function against a real Grafana Loki (`grafana/loki:3.7.10`)
in Docker: a single-tenant instance with the Grow Codes Loki configuration, a multi-tenant instance
(`auth_enabled: true`) and a test-only nginx gateway for Basic/Bearer authentication and
`429`/`502`/`503` fault injection. Each language package is packed and installed into a separate
consumer project first, so the tests use the package exactly as you would, and the shared
conformance scenarios run against the same stack.

```sh
make test-docker                     # all five languages
make test-docker LANGS="go python"   # a subset
```

No host ports are published and every run uses its own Compose project, so runs never collide; the
stack is always removed afterwards (`docker compose down -v`). Loki data lives on a tmpfs. Unit
tests need no Docker: `make test` or `make coverage`. Details: [docs/docker-testing.md](docs/docker-testing.md).

## Compatibility

| | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| Runtime | Node.js ≥ 22.12, ESM + CJS | stable, MSRV 1.85 | 1.26, 1.27 | 3.10 – 3.14 | 17, 21, 25 |
| Async | `Promise` + `AbortSignal` | `async` (tokio), `blocking` feature | `context.Context` | sync + `asyncio` | sync + `CompletableFuture` |
| WebSocket tail | `ws` | `tokio-tungstenite` | `coder/websocket` | `websockets` | `java.net.http.WebSocket` |
| Snappy (protobuf push) | `snappyjs` | `snap` | `golang/snappy` | `cramjam` (extra `snappy`) or built-in | `aircompressor` |
| Grafana Loki | `grafana/loki:3.7.10` | `grafana/loki:3.7.10` | `grafana/loki:3.7.10` | `grafana/loki:3.7.10` | `grafana/loki:3.7.10` |

Cases the Docker suite cannot produce (push status 260, the WAL-full "Ingester is shutting down",
Loki's own 429, `dropped_entries`, Grafana Cloud auth, TLS) are covered by unit tests and listed
with the service differences in [docs/compatibility.md](docs/compatibility.md).

## Contributing

Contributions are welcome — please read [CONTRIBUTING.md](CONTRIBUTING.md) and
[AGENTS.md](AGENTS.md) first. All five languages keep the same API; a change lands in all of them
together. Parts of this repository are generated by
[sdk-kit](https://github.com/grow-codes-packages/sdk-kit) and changed there.

## Security

Please report vulnerabilities privately to [opensource@grow.codes](mailto:opensource@grow.codes).
See [SECURITY.md](SECURITY.md).

## Licence

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for attributions.

## Trademarks and disclaimer

This is an independent, community-maintained project. It is **not affiliated with, endorsed or
sponsored by its owner (Grafana Labs)**. Grafana Loki is a trademark of Grafana Labs; it is used here only to name the
service this SDK connects to. The Grafana Loki server is licensed separately by its owner
(AGPL-3.0-only, with Apache-2.0 exceptions (`pkg/push`, `pkg/logproto`, `pkg/loghttp`, `clients/`, …) per Loki `LICENSING.md`).

The software is provided "AS IS", without warranties or conditions of any kind. Grow Codes is not
responsible for any damage, data loss, security incident, outage or cost that results from using it.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## About Grow Codes

This SDK is built and maintained by [Grow Codes](https://grow.codes). Grow Codes publishes the
client libraries it builds as open source in the
[grow-codes-packages](https://github.com/grow-codes-packages) organisation, generated from one
shared kit so that every service and every language behaves the same way. Learn more at
[grow.codes](https://grow.codes).
