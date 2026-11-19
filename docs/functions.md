# Functions

The Grafana Loki API of this SDK. Every function exists in all five languages with the same
behaviour, defaults and error codes; names follow each language's convention. Each module page
shows every function with an example per language.

| Module | Page | What it covers |
| - | - | - |
| Client and status | [Client and status](functions/client.md) | `fromEnv`, `health`, `ready`, `ping`, `buildInfo`, `serverVersion`, `withTenant`, `close` |
| Push | [Push](functions/push.md) | `push` (JSON, gzip, protobuf + Snappy), structured metadata, label validation, cardinality guard, `pushOtlp` |
| Query | [Query](functions/query.md) | `query`, `queryRange`, `labels`, `labelValues`, `series`, `indexStats`, `volume`, `volumeRange`, `formatQuery` |
| Tail | [Tail](functions/tail.md) | `tail` (WebSocket live tail) |
| LogQL builder | [LogQL builder](functions/logql.md) | `LogQL.selector(...).lineContains(...).json().where(...)` with escaping |
| Logging category | [Loki log client and search](functions/logging.md) | `logClient` / OTLP log client, `logQueryClient` (`search`, `tail`, `listStreams`, `listLabels`, `waitForLog`) |

## Names per language

| Function | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| from environment | `LokiClient.fromEnv()` | `LokiClient::from_env()` | `loki.NewClientFromEnv()` | `LokiClient.from_env()` | `LokiClient.fromEnv()` |
| health | `health()` | `health()` | `Health(ctx)` | `health()` | `health()` |
| ready | `ready()` | `ready()` | `Ready(ctx)` | `ready()` | `ready()` |
| ping | `ping()` → ms | `ping()` → `Duration` | `Ping(ctx)` → `time.Duration` | `ping()` → ms | `ping()` → `Duration` |
| build info | `buildInfo()` | `build_info()` | `BuildInfo(ctx)` | `build_info()` | `buildInfo()` |
| server version | `serverVersion()` | `server_version()` | `ServerVersion(ctx)` | `server_version()` | `serverVersion()` |
| tenant | `withTenant(id)` | `with_tenant(id)` | `WithTenant(id)` | `with_tenant(id)` | `withTenant(id)` |
| push | `push(streams, opts)` | `push(&streams, opts)` | `Push(ctx, streams, opts)` | `push(streams, ...)` | `push(streams, opts)` |
| OTLP push | `pushOtlp(records)` | `push_otlp(&records, gzip)` | `PushOTLP(ctx, records, opts)` | `push_otlp(records)` | `pushOtlp(records)` |
| instant query | `query(logql)` | `query(logql, opts)` | `Query(ctx, logql, opts)` | `query(logql)` | `query(logql)` |
| range query | `queryRange(logql)` | `query_range(logql, opts)` | `QueryRange(ctx, logql, opts)` | `query_range(logql)` | `queryRange(logql)` |
| label names | `labels()` | `labels(opts)` | `Labels(ctx, opts)` | `labels()` | `labels()` |
| label values | `labelValues(name)` | `label_values(name, opts)` | `LabelValues(ctx, name, opts)` | `label_values(name)` | `labelValues(name)` |
| series | `series(matchers)` | `series(&matchers, opts)` | `Series(ctx, matchers, opts)` | `series(matchers)` | `series(matchers)` |
| index stats | `indexStats(q)` | `index_stats(q, opts)` | `IndexStats(ctx, q, opts)` | `index_stats(q)` | `indexStats(q)` |
| volume | `volume(q)` | `volume(q, opts)` | `Volume(ctx, q, opts)` | `volume(q)` | `volume(q)` |
| volume over time | `volumeRange(q)` | `volume_range(q, opts)` | `VolumeRange(ctx, q, opts)` | `volume_range(q)` | `volumeRange(q)` |
| format LogQL | `formatQuery(q)` | `format_query(q)` | `FormatQuery(ctx, q)` | `format_query(q)` | `formatQuery(q)` |
| live tail | `tail(q, onEntries, opts)` | `tail(q, on_entries, opts)` | `Tail(ctx, q, onEntries, opts)` | `tail(q, on_entries)` | `tail(q, onFrame, opts)` |
| log client | `logClient(opts)` | `new_log_client(opts)` | `NewLogClient(opts)` | `log_client()` | `logClient(opts)` |
| OTLP log client | `logClient({ protocol: "otlp" })` | `new_otlp_log_client(opts)` | `NewOTLPLogClient(opts)` | `otlp_log_client()` | `logClient(…protocol(OTLP))` |
| log query client | `logQueryClient()` | `log_query()` | `LogQuery()` | `log_query()` | `logQueryClient()` |
| close | `await close()` | `close().await` | `Close()` | `close()` / `with` | `close()` / try-with-resources |

Async variants: TypeScript and Rust are async throughout; Python has `AsyncLokiClient` with the
same methods; Java has a `…Async` variant (`CompletableFuture`) of every I/O method; Go takes a
`context.Context`. Rust also has a blocking client (feature `blocking`).

## Behaviour shared by every function

- Every request carries the configured `Authorization` (Basic or Bearer) and `X-Scope-OrgID`
  headers; query calls also send `X-Loki-Response-Encoding-Flags: categorize-labels`, so results
  separate index labels, structured metadata and parsed labels.
- Errors map to the canonical codes ([Error handling](errors.md)): 400/405/422 → `VALIDATION`,
  401 (`no org id`, gateway) → `AUTHENTICATION`, 403 → `PERMISSION_DENIED`, 404 → `NOT_FOUND`,
  429 → `RATE_LIMITED` (with `Retry-After`), 500/502/503 → `UNAVAILABLE` (retryable), 504 →
  `TIMEOUT`. A 500/503 saying "Ingester is shutting down" (WAL disk at least 90 % full) is a
  retryable `UNAVAILABLE` with a hint about the cause. Push status 260 (ingestion blocked by the
  operator) is a non-retryable `UNAVAILABLE`.
- Timestamps are Unix nanoseconds on the wire and lossless in every language (TypeScript keeps
  `timestampNs` strings next to `Date` values).

Settings such as the tenant, credentials and the cardinality guard are described in
[Configuration](configuration.md#grafana-loki-settings).
