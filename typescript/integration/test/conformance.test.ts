// Logging category against the real Loki (logClient + logQueryClient), then the shared
// conformance scenarios (conformance/scenarios) with the core and logging factories.
// Capabilities: core "version"; logging "query", "tail", "labels", "streams" (nothing skipped).

import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { after, before, test } from "node:test";
import { LokiClient, Severity, type LogRecord } from "@grow-codes/grafana-loki";
import { formatReport, runConformance } from "@grow-codes/grafana-loki/testing";
import { loadBootstrapEnv, runId, scenariosDir } from "./env.ts";

const clients: LokiClient[] = [];
const run = `${runId}-${randomBytes(4).toString("hex")}`.replace(/[^a-zA-Z0-9_-]/g, "_");

function loki(): LokiClient {
  const c = LokiClient.fromEnv({ retry: { maxRetries: 2, initialBackoffMs: 50 } });
  clients.push(c);
  return c;
}

before(() => {
  loadBootstrapEnv();
});

after(async () => {
  await Promise.all(clients.map((c) => c.close()));
});

for (const protocol of ["json", "protobuf", "otlp"] as const) {
  test(`logClient (${protocol}) → search / waitForLog / listStreams / listLabels`, async () => {
    const client = loki();
    const service = `sdk-ts-${protocol}-${run}`;
    const logs = client.logClient({
      protocol,
      resource: { "service.name": service, "deployment.environment": "test" },
      batch: { flushIntervalMs: 100 },
    });
    const query = client.logQueryClient();
    const log = logs.logger("integration", { component: "it" });
    const t0 = Date.now();
    log.info(`hello ${protocol} ${run}`, { request_id: `r-${run}`, password: `hunter2-${run}` });
    logs.send({
      timestamp: new Date(t0 + 5), // after the logger record, so the asc page order is certain
      message: `traced ${protocol} ${run}`,
      severity: Severity.Error,
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      spanId: "00f067aa0ba902b7",
      traceFlags: 1,
    });
    await logs.flush();
    assert.deepEqual(logs.stats(), { queued: 0, sent: 2, dropped: 0, failed: 0, retries: 0 });

    const traced = await query.waitForLog(`{service_name="${service}"}`, {
      predicate: (r) => r.traceId !== undefined,
      timeoutMs: 20_000,
      intervalMs: 250,
    });
    assert.equal(traced.body, `traced ${protocol} ${run}`);
    assert.equal(traced.traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert.equal(traced.spanId, "00f067aa0ba902b7");
    assert.equal(traced.severity, Severity.Error);
    assert.equal(traced.resource["service.name"], service);

    const found = await query.search(`hello ${protocol} ${run}`, { limit: 10 });
    assert.equal(found.records.length, 1);
    const hello = found.records[0] as LogRecord;
    assert.equal(hello.severity, Severity.Info);
    assert.equal(hello.attributes.request_id, `r-${run}`);
    assert.ok(!JSON.stringify(found).includes(`hunter2-${run}`), "password must be redacted");

    const page1 = await query.search(`{service_name="${service}"}`, { limit: 1, order: "asc" });
    assert.equal(page1.records[0]?.body, `hello ${protocol} ${run}`);
    assert.ok(page1.nextCursor !== undefined);
    const page2 = await query.search(`{service_name="${service}"}`, {
      limit: 1,
      order: "asc",
      cursor: page1.nextCursor,
    });
    assert.equal(page2.records[0]?.body, `traced ${protocol} ${run}`);

    const streams = await query.listStreams();
    assert.ok(
      streams.some((s) => s.includes(`service_name="${service}"`)),
      streams.join("\n"),
    );
    assert.ok((await query.listLabels()).includes("service_name"));
    await logs.close();
  });
}

test("logQueryClient.tail delivers records sent after it started", async () => {
  const client = loki();
  const service = `sdk-ts-tail-${run}`;
  const logs = client.logClient({
    resource: { "service.name": service },
    batch: { flushIntervalMs: 100 },
  });
  const got: LogRecord[] = [];
  const errors: unknown[] = [];
  const handle = client.logQueryClient().tail(`{service_name="${service}"}`, (r) => got.push(r), {
    onError: (e) => errors.push(e),
  });
  await new Promise((r) => setTimeout(r, 500));
  logs.send({ message: `live ${run}`, attributes: { n: 1 } });
  await logs.flush();
  const deadline = Date.now() + 15_000;
  while (got.length === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));
  handle.stop();
  assert.deepEqual(errors, []);
  assert.equal(got[0]?.body, `live ${run}`);
  assert.equal(got[0]?.attributes.n, "1");
});

test("conformance: core + logging", async () => {
  const report = await runConformance({
    scenariosDir,
    categories: ["core", "logging"],
    factories: {
      core: () => loki(),
      logging: (ctx) => {
        const client = loki();
        return {
          client: client.logClient({
            batch: { flushIntervalMs: 200, ...(ctx.batchOptions ?? {}) },
            resource: { "service.name": "sdk-ts-conformance" },
          }),
          query: client.logQueryClient(),
        };
      },
    },
    capabilities: { core: ["version"], logging: ["query", "tail", "labels", "streams"] },
  });
  console.log(formatReport(report));
  assert.equal(report.failed, 0, formatReport(report));
  assert.equal(report.skipped, 0, formatReport(report));
  assert.ok(report.passed >= 4);
});
