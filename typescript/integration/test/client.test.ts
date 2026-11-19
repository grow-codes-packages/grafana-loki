// Integration tests against the real Grafana Loki from docker/compose.yml: every public function
// of @grow-codes/grafana-loki is called at least once with a round-trip assertion.

import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { after, before, describe, test } from "node:test";
import {
  DEFAULT_ALLOWED_LABELS,
  LogQL,
  LokiClient,
  LokiConfig,
  LokiError,
  Severity,
  type LogRecord,
  type LokiStreamResult,
} from "@grow-codes/grafana-loki";
import { loadBootstrapEnv, runId } from "./env.ts";

const run = `${runId}-${randomBytes(4).toString("hex")}`.replace(/[^a-zA-Z0-9_-]/g, "_");
const clients: LokiClient[] = [];
// The tests select streams by run/kind, so they extend the cardinality guard allowlist.
const allowedLabels = [...DEFAULT_ALLOWED_LABELS, "run", "kind", "nasty"];

function env(name: string): string {
  const value = process.env[name];
  assert.ok(value, `${name} must be set (from /bootstrap/env)`);
  return value;
}

function track(client: LokiClient): LokiClient {
  clients.push(client);
  return client;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function until<T>(
  fn: () => Promise<T>,
  ok: (v: T) => boolean,
  timeoutMs = 30_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (ok(v)) return v;
    if (Date.now() > deadline) return v;
    await sleep(300);
  }
}

function lines(streams: LokiStreamResult[]): string[] {
  return streams.flatMap((s) => s.entries.map((e) => e.line));
}

let loki: LokiClient;

before(() => {
  loadBootstrapEnv();
  assert.ok(process.env.GROW_GRAFANA_LOKI_URL, "GROW_GRAFANA_LOKI_URL must be set");
  loki = track(
    LokiClient.fromEnv({ retry: { maxRetries: 2, initialBackoffMs: 50 }, allowedLabels }),
  );
});

after(async () => {
  await Promise.all(clients.map((c) => c.close()));
});

describe("status", () => {
  test("health, ready, ping, buildInfo, serverVersion", async () => {
    assert.deepEqual(await loki.health(), { healthy: true, status: "ok", details: {} });
    assert.equal(await loki.ready(), true);
    const latency = await loki.ping();
    assert.ok(latency >= 0);
    const info = await loki.buildInfo();
    assert.equal(info.version, env("GROW_GRAFANA_LOKI_EXPECTED_VERSION"));
    assert.match(info.revision, /\S/);
    assert.equal(await loki.serverVersion(), info.version);
  });
});

describe("push + query round trips", () => {
  test("push (JSON, gzip, structured metadata) → queryRange (categorized)", async () => {
    const now = Date.now();
    await loki.push([
      {
        labels: { service_name: "sdk-ts", run, kind: "json" },
        entries: [
          {
            timestamp: new Date(now - 2),
            line: `json first ${run}`,
            structuredMetadata: { request_id: `req-${run}` },
          },
          { timestamp: BigInt(now - 1) * 1_000_000n, line: `json second ${run}` },
        ],
      },
    ]);
    await loki.push(
      [
        {
          labels: { service_name: "sdk-ts", run, kind: "gzip" },
          entries: [{ timestamp: new Date(now), line: `gzip ${run}` }],
        },
      ],
      { gzip: true },
    );
    const res = await loki.queryRange(`{run="${run}", kind=~"json|gzip"}`, {
      start: new Date(now - 60_000),
      end: new Date(now + 60_000),
      direction: "forward",
      limit: 100,
    });
    assert.equal(res.resultType, "streams");
    assert.deepEqual(
      lines(res.streams).sort(),
      [`gzip ${run}`, `json first ${run}`, `json second ${run}`].sort(),
    );
    const first = res.streams
      .flatMap((s) => s.entries)
      .find((e) => e.line.startsWith("json first"));
    assert.equal(first?.structuredMetadata.request_id, `req-${run}`);
    for (const s of res.streams) assert.equal(s.labels.run, run);
  });

  test("push applies the cardinality guard (default allowlist)", async () => {
    const plain = track(LokiClient.fromEnv({ retry: { maxRetries: 2, initialBackoffMs: 50 } }));
    const service = `sdk-ts-guard-${run}`;
    await plain.push([
      {
        labels: { service_name: service, run, kind: "guard" },
        entries: [{ line: `guard ${run}` }],
      },
    ]);
    const res = await plain.queryRange(`{service_name="${service}"} | run="${run}"`);
    assert.equal(res.streams.length, 1);
    assert.equal(res.streams[0]?.labels.run, undefined);
    assert.equal(res.streams[0]?.entries[0]?.structuredMetadata.run, run);
    assert.equal(res.streams[0]?.entries[0]?.structuredMetadata.kind, "guard");
  });

  test("push (protobuf + snappy) → queryRange", async () => {
    await loki.push(
      [
        {
          labels: { service_name: "sdk-ts", run, kind: "protobuf" },
          entries: [
            {
              timestamp: new Date(),
              line: `protobuf ${run}`,
              structuredMetadata: { request_id: `pb-${run}` },
            },
          ],
        },
      ],
      { format: "protobuf" },
    );
    const res = await loki.queryRange(`{run="${run}", kind="protobuf"}`);
    const e = res.streams[0]?.entries[0];
    assert.equal(e?.line, `protobuf ${run}`);
    assert.equal(e?.structuredMetadata.request_id, `pb-${run}`);
  });

  test("pushOtlp → queryRange by trace_id", async () => {
    const traceId = randomBytes(16).toString("hex");
    const record: LogRecord = {
      timestamp: new Date(),
      severity: Severity.Error,
      severityText: "ERROR",
      body: `otlp ${run}`,
      attributes: { "request.id": `otlp-${run}`, "http.status_code": 503 },
      resource: { "service.name": `otlp-${run}`, "deployment.environment": "test" },
      traceId,
      spanId: randomBytes(8).toString("hex"),
      traceFlags: 1,
    };
    await loki.pushOtlp([record], { gzip: true });
    const res = await loki.queryRange(`{service_name="otlp-${run}"} | trace_id="${traceId}"`);
    const e = res.streams[0]?.entries[0];
    assert.equal(e?.line, `otlp ${run}`);
    assert.equal(e?.structuredMetadata.request_id, `otlp-${run}`);
    assert.equal(e?.structuredMetadata.severity_text, "ERROR");
    assert.equal(res.streams[0]?.labels.deployment_environment, "test");
  });

  test("query (instant metric) counts pushed lines; log selector is rejected", async () => {
    await loki.push([
      {
        labels: { service_name: "sdk-ts", run, kind: "metric" },
        entries: [1, 2, 3].map((i) => ({
          timestamp: new Date(Date.now() - i),
          line: `metric ${i} ${run}`,
        })),
      },
    ]);
    const res = await loki.query(`sum(count_over_time({run="${run}", kind="metric"}[10m]))`, {
      time: new Date(),
    });
    assert.equal(res.resultType, "vector");
    assert.equal(res.vector[0]?.value, 3);
    await assert.rejects(
      loki.query(`{run="${run}"}`),
      (e: unknown) => e instanceof LokiError && e.code === "VALIDATION",
    );
    await assert.rejects(
      loki.query("sum("),
      (e: unknown) => e instanceof LokiError && e.code === "VALIDATION" && e.httpStatus === 400,
    );
  });

  test("queryRange metric matrix", async () => {
    const res = await loki.queryRange(`sum(count_over_time({run="${run}", kind="metric"}[1m]))`, {
      start: new Date(Date.now() - 10 * 60_000),
      end: new Date(),
      stepMs: 60_000,
    });
    assert.equal(res.resultType, "matrix");
    assert.ok(res.matrix[0]!.samples.some((s) => s.value >= 1));
  });

  test("labels, labelValues, series", async () => {
    const window = { start: new Date(Date.now() - 3600_000), end: new Date(Date.now() + 60_000) };
    assert.ok((await loki.labels(window)).includes("run"));
    assert.ok((await loki.labels({ query: `{run="${run}"}` })).includes("kind"));
    assert.ok((await loki.labelValues("run", window)).includes(run));
    assert.deepEqual(
      (await loki.labelValues("kind", { query: `{run="${run}"}` })).filter((k) => k === "json"),
      ["json"],
    );
    assert.deepEqual(await loki.labelValues("does_not_exist_label"), []);
    const series = await loki.series(
      [`{run="${run}", kind="json"}`, `{run="${run}", kind="gzip"}`],
      window,
    );
    assert.deepEqual(series.map((s) => s.kind).sort(), ["gzip", "json"]);
  });

  test("LogQL builder output is valid (formatQuery) and finds escaped literals", async () => {
    const nasty = `quote " backslash \\ tab \t ü 🚀 ${run}`;
    const labelValue = `q"\\ü-${run}`;
    await loki.push([
      {
        labels: { service_name: "sdk-ts", run, kind: "escape", nasty: labelValue },
        entries: [{ timestamp: new Date(), line: `raw ${nasty}` }],
      },
      {
        labels: { service_name: "sdk-ts", run, kind: "jsonw" },
        entries: [
          { timestamp: new Date(), line: `{"status": 503, "msg": "down"}` },
          { timestamp: new Date(), line: `{"status": 200, "msg": "ok"}` },
        ],
      },
    ]);
    const escaped = LogQL.selector({ run, nasty: labelValue }).lineContains(nasty);
    const hit = await loki.queryRange(escaped.toString());
    assert.deepEqual(lines(hit.streams), [`raw ${nasty}`]);
    const q = LogQL.selector({ run, kind: "jsonw" })
      .lineContains("status")
      .json()
      .where("status", ">=", 500);
    const formatted = await loki.formatQuery(q.toString());
    assert.match(formatted, /status>=500/);
    const res = await loki.queryRange(q.toString());
    assert.equal(res.streams.flatMap((s) => s.entries).length, 1);
    assert.equal(res.streams[0]?.entries[0]?.parsed.status, "503");
    await assert.rejects(
      loki.formatQuery("{"),
      (e: unknown) => e instanceof LokiError && e.code === "VALIDATION",
    );
  });

  test("tail (WebSocket) receives an entry pushed after connecting", async () => {
    const got: string[] = [];
    const handle = await loki.tail(
      `{run="${run}", kind="tail"}`,
      (streams) => got.push(...lines(streams)),
      {
        start: new Date(),
        delayForMs: 0,
      },
    );
    await sleep(300);
    await loki.push([
      {
        labels: { service_name: "sdk-ts", run, kind: "tail" },
        entries: [{ timestamp: new Date(), line: `tail ${run}` }],
      },
    ]);
    await until(
      async () => got.length,
      (n) => n > 0,
      15_000,
    );
    handle.stop();
    await handle.closed;
    assert.deepEqual(got, [`tail ${run}`]);
    await assert.rejects(
      loki.tail(`{run="${run}"} |= "`, () => {}),
      (e: unknown) => e instanceof LokiError && e.code === "VALIDATION",
    );
  });

  test("indexStats, volume, volumeRange after an ingester flush", async () => {
    const res = await fetch(new URL("/flush", env("GROW_GRAFANA_LOKI_URL")), { method: "POST" });
    assert.equal(res.status, 204);
    const window = { start: new Date(Date.now() - 3600_000), end: new Date(Date.now() + 60_000) };
    const stats = await until(
      () => loki.indexStats(`{run="${run}"}`, window),
      (s) => s.streams > 0,
    );
    assert.ok(stats.streams >= 1, JSON.stringify(stats));
    assert.ok(stats.entries >= 1, JSON.stringify(stats));
    assert.equal(typeof stats.bytes, "number");
    // volume / volume_range can lag behind index stats right after a flush: poll them too.
    const volume = await until(
      () =>
        loki.volume(`{run="${run}"}`, {
          ...window,
          targetLabels: ["kind"],
          aggregateBy: "labels",
          limit: 50,
        }),
      (v) => v.vector.some((e) => "kind" in e.metric),
    );
    assert.equal(volume.resultType, "vector");
    assert.ok(
      volume.vector.some((v) => "kind" in v.metric),
      JSON.stringify(volume.vector),
    );
    const range = await until(
      () => loki.volumeRange(`{run="${run}"}`, { ...window, stepMs: 60_000 }),
      (r) => r.matrix.length >= 1,
    );
    assert.equal(range.resultType, "matrix");
    assert.ok(range.matrix.length >= 1);
  });
});

describe("multi-tenancy", () => {
  test("withTenant isolates tenants; no tenant → AuthenticationError", async () => {
    const root = track(
      new LokiClient({
        url: env("GROW_GRAFANA_LOKI_MULTI_TENANT_URL"),
        retry: { maxRetries: 0 },
        allowedLabels,
      }),
    );
    await assert.rejects(
      root.ping(),
      (e: unknown) => e instanceof LokiError && e.code === "AUTHENTICATION",
    );
    const a = root.withTenant(env("GROW_GRAFANA_LOKI_TENANT_A"));
    const b = root.withTenant(env("GROW_GRAFANA_LOKI_TENANT_B"));
    assert.ok((await a.ping()) >= 0);
    await a.push([
      {
        labels: { service_name: "sdk-ts", run, kind: "tenant" },
        entries: [{ timestamp: new Date(), line: `tenant a ${run}` }],
      },
    ]);
    assert.deepEqual(lines((await a.queryRange(`{run="${run}"}`)).streams), [`tenant a ${run}`]);
    assert.deepEqual((await b.queryRange(`{run="${run}"}`)).streams, []);
    assert.deepEqual(await b.labelValues("run", { query: `{run="${run}"}` }), []);
    const got: string[] = [];
    const tail = await a.tail(`{run="${run}"}`, (s) => got.push(...lines(s)), {
      limit: 10,
      start: new Date(Date.now() - 600_000), // replay: the line was pushed before the tail
    });
    await until(
      async () => got.length,
      (n) => n > 0,
      10_000,
    );
    tail.stop();
    assert.deepEqual(got, [`tenant a ${run}`]);
    await assert.rejects(
      root.tail(`{run="${run}"}`, () => {}),
      (e: unknown) => e instanceof LokiError && e.code === "AUTHENTICATION",
    );
    assert.throws(
      () => root.withTenant("a|b"),
      (e: unknown) => e instanceof LokiError && e.code === "VALIDATION",
    );
    await a.close();
    await b.close();
  });
});

describe("auth gateway", () => {
  const url = (): string => env("GROW_GRAFANA_LOKI_AUTH_PROXY_URL");

  test("Basic and Bearer reach Loki; wrong credentials → AuthenticationError without leaking", async () => {
    const basic = track(
      new LokiClient({
        url: url(),
        username: env("GROW_GRAFANA_LOKI_AUTH_PROXY_USERNAME"),
        password: env("GROW_GRAFANA_LOKI_AUTH_PROXY_PASSWORD"),
      }),
    );
    assert.ok((await basic.ping()) >= 0);
    const bearer = track(
      new LokiClient({
        url: url(),
        bearerToken: env("GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN"),
        allowedLabels,
      }),
    );
    await bearer.push([
      {
        labels: { service_name: "sdk-ts", run, kind: "gateway" },
        entries: [{ timestamp: new Date(), line: `gateway ${run}` }],
      },
    ]);
    assert.deepEqual(lines((await bearer.queryRange(`{run="${run}", kind="gateway"}`)).streams), [
      `gateway ${run}`,
    ]);
    const got: string[] = [];
    const tail = await bearer.tail(`{run="${run}", kind="gateway"}`, (s) => got.push(...lines(s)), {
      limit: 5,
      start: new Date(Date.now() - 600_000),
    });
    await until(
      async () => got.length,
      (n) => n > 0,
      10_000,
    );
    tail.stop();
    const wrong = "wrong-gateway-password-xyz";
    const bad = track(
      new LokiClient({ url: url(), username: "nobody", password: wrong, retry: { maxRetries: 0 } }),
    );
    await assert.rejects(bad.ping(), (e: unknown) => {
      assert.ok(e instanceof LokiError);
      assert.equal(e.code, "AUTHENTICATION");
      assert.ok(!e.message.includes(wrong));
      return true;
    });
    const health = await bad.health();
    assert.equal(health.healthy, false);
  });

  test("429 with Retry-After is retried and surfaces as RateLimitError; 503 as UnavailableError", async () => {
    const attempts: number[] = [];
    const limited = track(
      new LokiClient({
        url: url(),
        bearerToken: env("GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN"),
        headers: { "X-Grow-Fault": "429" },
        retry: { maxRetries: 1, initialBackoffMs: 10 },
        hooks: { onRequest: (i) => attempts.push(i.attempt) },
      }),
    );
    const started = Date.now();
    await assert.rejects(
      limited.labels(),
      (e: unknown) =>
        e instanceof LokiError && e.code === "RATE_LIMITED" && e.retryAfterMs === 1000,
    );
    assert.deepEqual(attempts, [1, 2]);
    assert.ok(Date.now() - started >= 900);
    const unavailable = track(
      new LokiClient({ url: url(), headers: { "X-Grow-Fault": "503" }, retry: { maxRetries: 0 } }),
    );
    await assert.rejects(
      unavailable.labels(),
      (e: unknown) => e instanceof LokiError && e.code === "UNAVAILABLE" && e.httpStatus === 503,
    );
  });
});

describe("config", () => {
  test("fromEnv targets the single-tenant instance and redacts credentials", () => {
    const cfg = LokiConfig.fromEnv({ bearerToken: "integration-secret-token" });
    assert.equal(cfg.url, env("GROW_GRAFANA_LOKI_URL"));
    assert.ok(!String(cfg).includes("integration-secret-token"));
  });
});
