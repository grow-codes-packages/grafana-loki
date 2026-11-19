# Compatibility

## Runtimes

| Language | Supported | Tested in CI |
| - | - | - |
| TypeScript | Node.js ≥ 22.12, ESM and CommonJS | Node 22, 24 |
| Rust | MSRV 1.85, edition 2021, tokio | stable, 1.85 |
| Go | Go ≥ 1.26 | 1.26, 1.27 |
| Python | ≥ 3.10 | 3.10, 3.13, 3.14 |
| Java | ≥ 17 (Java 17 bytecode) | Temurin 17, 21, 25 |

## Grafana Loki versions

The integration suite runs against `grafana/loki:3.7.10` (the version of the Grow Codes stack
catalog) with the Grow Codes Loki configuration (TSDB schema v13, structured metadata on,
`volume_enabled: true`, 7-day `reject_old_samples_max_age`), a second instance with
`auth_enabled: true` for multi-tenancy, and a test-only nginx gateway for Basic/Bearer
authentication and fault injection. Other Loki 3.x versions that expose the same HTTP API are
expected to work; report problems as an issue. Hosted Loki (Grafana Cloud) uses the same paths
under its own base URL with Basic authentication.

## Functions the service cannot serve in the Docker suite

Every public function is called against the real service in the Docker suite. These **cases**
cannot be produced by the container and are covered by unit tests in all five languages:

| Case | Languages | Reason |
| - | - | - |
| push status **260** (ingestion administratively blocked) | all | needs `blocked_ingestion` limits with a runtime-config file; not part of the Grow config |
| push **500/503 "Ingester is shutting down"** (WAL disk ≥ 90 % full) | all | depends on host disk usage; the fixture keeps `/loki` on a tmpfs |
| Loki's own **429** ingestion rate limit text | all | needs more than 16 MB/s of ingest; the gateway's 429 + `Retry-After` is tested instead |
| `dropped_entries` in tail frames | all | only sent when a tailer falls behind under load |
| multi-tenant queries `a\|b` returning data | all | `multi_tenant_queries_enabled` is off (Loki's default); the suite checks the failure (400 on push, 500 on query) |
| real Grafana Cloud Basic/Bearer semantics | all | no credentials in CI; the nginx gateway proves the headers |
| HTTPS, mutual TLS and proxies | all | plain HTTP fixture; the shared transport tests cover them |
| positive persisted `indexStats.bytes` / `volumeRange` values | all | Loki counts chunk bytes in KB, so small test chunks report 0; shape, streams, chunks and entries are asserted |

No function raises `UNSUPPORTED`.

## Service behaviour the SDK relies on or smooths over

| Loki behaviour | What the SDK does |
| - | - |
| Loki has no authentication of its own; `auth_enabled` only requires `X-Scope-OrgID` | sends Basic/Bearer for gateways and hosted Loki, tenant header when configured |
| errors are plain text, OTLP errors a protobuf `google.rpc.Status` | the message is the body text; TypeScript, Python and Java decode the OTLP status text, Go and Rust report the HTTP status only for OTLP errors |
| `/loki/api/v1/query` rejects log selectors (400) | `query()` rejects them client side with `VALIDATION` |
| `index/volume_range` answers with a vector when every series has one point | `volumeRange` always returns a matrix |
| empty `labels` / `label/{name}/values` results omit `data`; invalid label names return 200 | decoded as `[]`; label names are validated client side |
| `query_range` streams are not globally ordered and a page can end inside a nanosecond | `search` merge-sorts all streams and re-queries the boundary nanosecond |
| a rejected WebSocket upgrade stays open for 120 s | the SDK closes the connection at once |
| without `start`, the tail replays the last hour | `tail` defaults to `start = now` (live only) |
| identical entries (labels, timestamp, line) are dropped silently | push is retried on 429/5xx with the identical payload |
| `volume` with `volume_enabled: false` returns an empty result | reported as "no data", not as an error |
| Loki adds `service_name` (`unknown_service`) and `detected_level` | tests and search allow server-added keys |

## Skipped conformance scenarios

| Scenario | Reason |
| - | - |
| — | none |

## Language differences

The API is the same in every language; only the conventions differ (naming, durations, async
model). Specific to this SDK:

| Topic | Difference |
| - | - |
| keywords | Rust names `where` and `match` `where_` and `match_` (`label` is the cross-language name) |
| exporter names | Go and Rust also keep their original names `PushExporter` / `OTLPExporter` (`OtlpExporter`); `LokiLogExporter` / `LokiOtlpExporter` are aliases |
| category tail | Go and Rust expose the category methods on `client.LogQuery()` / `client.log_query()` because the service `Tail` has a different signature; TypeScript and Java call it `logQueryClient()`, Python `log_query()` |
| `since` | Java `labels` / `series` / `indexStats` take a `TimeRange` or `LabelsOptions`; use `TimeRange.last(d)` / `LabelsOptions.last(d)` instead of a server-side `since` (Java `queryRange` has `since`) |
| matrix samples | TypeScript and Java call matrix points `samples`, the other languages `values` |
| per-log-client allowlist | TypeScript `logClient({ allowedLabels })` can override the config allowlist for one log client; the other languages use the client's `allowedLabels` (Python `LokiLogExporter(client, guard=…)`) |
| environment | Java cannot change the process environment, so `loadEnv` sets **system properties** instead and returns the names it set |
