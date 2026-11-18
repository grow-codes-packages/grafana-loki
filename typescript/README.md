# @grow-codes/**KEY**

TypeScript SDK for [**DISPLAY**](https://grow-codes-packages.github.io/grafana-loki/) by Grow Codes.
Node.js 22.12 or newer, ESM and CommonJS, full type definitions.

```sh
npm install @grow-codes/grafana-loki
```

```ts
import { LokiClient } from "@grow-codes/grafana-loki";

// Reads GROW_GRAFANA_LOKI_URL, GROW_GRAFANA_LOKI_TOKEN, timeouts, TLS and proxy settings.
const client = LokiClient.fromEnv();
try {
  console.log(await client.health()); // { healthy: true, status: "ok", details: {} }
  console.log(`ping: ${(await client.ping()).toFixed(1)} ms`);
} finally {
  await client.close();
}
```

Every error is a `LokiError` subclass with a canonical `code` (`NOT_FOUND`,
`AUTHENTICATION`, `RATE_LIMITED`, …), `httpStatus`, `requestId` and `retryable`. Credentials
never appear in messages, logs, hook data or `toString()` output.

Test helpers (in-memory fakes and the conformance runner) live in
`@grow-codes/grafana-loki/testing`.

Documentation: https://grow-codes-packages.github.io/grafana-loki/ · Licence: Apache-2.0.
**DISPLAY** is a trademark of **VENDOR**; this package is not affiliated with or endorsed by it.
