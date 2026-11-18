// Grafana Loki TypeScript SDK tour: configuration from the environment, status, push, LogQL
// queries, the batching log client with a logger, search and a live tail.
//
//   (cd ../../typescript && npm install && npm run build)   # or use the published package
//   npm install
//   GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100 npm start
//   # multi-tenant Loki: GROW_GRAFANA_LOKI_TENANT_ID=team-a; gateways/Grafana Cloud:
//   # GROW_GRAFANA_LOKI_USERNAME + GROW_GRAFANA_LOKI_PASSWORD or GROW_GRAFANA_LOKI_BEARER_TOKEN

import { LogQL, LokiClient, LokiError, Severity } from "@grow-codes/grafana-loki";

const loki = LokiClient.fromEnv({
  // Warnings only; the default logger is silent.
  logger: { debug() {}, info() {}, warn: console.warn, error: console.error },
});
console.log(`config: ${loki.config}`); // credentials print as [REDACTED]
const run = Math.random().toString(36).slice(2, 10);

try {
  const health = await loki.health(); // never throws for an unreachable service
  console.log("health:", health);
  if (!health.healthy) process.exit(1);
  console.log("server version:", await loki.serverVersion());
  console.log(`ping: ${(await loki.ping()).toFixed(1)} ms`);

  // 1. Push one stream (JSON; use { format: "protobuf" } for Snappy protobuf).
  await loki.push([
    {
      labels: { service_name: "example", run },
      entries: [
        { timestamp: new Date(), line: `{"status": 503, "msg": "upstream down ${run}"}`, metadata: { request_id: "r-1" } },
        { timestamp: new Date(), line: `{"status": 200, "msg": "ok ${run}"}` },
      ],
    },
  ]);

  // 2. Query it back with the LogQL builder (escaping handled for you).
  const logql = LogQL.selector({ run }).lineContains("upstream").json().where("status", ">=", 500);
  const range = await loki.queryRange(logql.toString(), { limit: 10 });
  for (const s of range.streams) for (const e of s.entries) console.log("found:", e.line, e.parsed, e.structuredMetadata);

  // 3. Metrics: an instant query.
  const count = await loki.query(`sum(count_over_time({run="${run}"}[5m]))`);
  console.log("lines in the last 5 minutes:", count.vector[0]?.value);

  // 4. The batching log client + logger facade (redaction and cardinality guard included).
  const logs = loki.logClient({ resource: { "service.name": "example", "deployment.environment": "dev" } });
  const log = logs.logger("example");
  log.info(`user signed in ${run}`, { user_id: 42, password: "never-sent" });
  log.error(`payment failed ${run}`, { order: "A-17" });

  // 5. A live tail (WebSocket) that sees the next record.
  const query = loki.logQueryClient();
  const tail = query.tail(`{service_name="example"} |= "${run}"`, (r) => console.log("tail:", r.severityText, r.body));
  await logs.flush();
  const found = await query.waitForLog(`{service_name="example"} |= "payment failed ${run}"`, { timeoutMs: 10_000 });
  console.log("waitForLog:", found.severity === Severity.Error, found.attributes);
  await new Promise((r) => setTimeout(r, 1500));
  tail.stop();
} catch (err) {
  if (err instanceof LokiError) {
    // Canonical code + HTTP status; the message never contains credentials.
    console.error(`${err.code}: ${err.message}`, { httpStatus: err.httpStatus });
    process.exitCode = 1;
  } else {
    throw err;
  }
} finally {
  await loki.close(); // flushes log clients, stops tails
}
