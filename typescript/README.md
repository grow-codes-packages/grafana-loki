# @grow-codes/grafana-loki

TypeScript SDK for [Grafana Loki](https://grow-codes-packages.github.io/grafana-loki/) by Grow Codes.
Node.js 22.12 or newer, ESM and CommonJS, full type definitions. Tested against `grafana/loki:3.7.10`.

```sh
npm install @grow-codes/grafana-loki
```

```ts
import { LogQL, LokiClient } from "@grow-codes/grafana-loki";

// Reads GROW_GRAFANA_LOKI_URL (or LOKI_URL), GROW_GRAFANA_LOKI_TENANT_ID (or LOKI_TENANT_ID),
// Basic/Bearer credentials, timeouts, TLS and proxy settings.
const loki = LokiClient.fromEnv();
try {
  console.log(await loki.health()); // { healthy: true, status: "ok", details: {} }

  await loki.push([
    { labels: { service_name: "api" }, entries: [{ timestamp: new Date(), line: "boot ok" }] },
  ]);
  const q = LogQL.selector({ service_name: "api" }).lineContains("boot");
  const res = await loki.queryRange(q.toString(), { limit: 10 });
  console.log(res.streams.flatMap((s) => s.entries.map((e) => e.line)));

  // Batching log client with redaction, the cardinality guard and a logger facade.
  const logs = loki.logClient({ resource: { "service.name": "api" } });
  logs.logger("http").info("request served", { status: 200 });
} finally {
  await loki.close(); // flushes log clients, stops tails
}
```

Modules: push (JSON, protobuf + Snappy, OTLP), `query`, `queryRange`, `labels`, `labelValues`,
`series`, `tail` (WebSocket), `indexStats`, `volume`, `volumeRange`, `formatQuery`, the `LogQL`
builder, `ready`, `buildInfo`, `withTenant`, and the logging category (`logClient()`,
`logQueryClient()`, pino and winston transports).

Every error is a `LokiError` subclass with a canonical `code` (`VALIDATION`, `AUTHENTICATION`,
`RATE_LIMITED`, …), `httpStatus`, `requestId` and `retryable`. Credentials never appear in
messages, logs, hook data or `toString()` output.

Test helpers (in-memory fakes and the conformance runner) live in
`@grow-codes/grafana-loki/testing`.

Documentation: https://grow-codes-packages.github.io/grafana-loki/ · Licence: Apache-2.0.
Grafana Loki is a trademark of Grafana Labs; this package is not affiliated with or endorsed by it.
The protobuf push encoder follows the Apache-2.0 `pkg/push/push.proto` of Grafana Loki (see NOTICE).
