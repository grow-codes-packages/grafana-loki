import { afterEach, describe, expect, it } from "vitest";
import {
  LokiClient,
  LokiLogExporter,
  LokiLogQueryClient,
  LokiOtlpExporter,
  Severity,
  decodeCursor,
  encodeCursor,
  entryToLogRecord,
  mergeStreams,
  toSearchLogQL,
  ValidationError,
  type LokiPusher,
  type LokiQuerySource,
  type LokiStreamResult,
  type LogRecord,
} from "../src/index.js";
import { fixture, fixtureResponse, startFakeLoki } from "./helpers/loki.js";
import { type MockServer, startServer } from "./helpers/server.js";

const servers: MockServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

function entry(ns: string, line: string, md: Record<string, string> = {}) {
  return {
    timestampNs: ns,
    timestamp: new Date(Number(BigInt(ns) / 1_000_000n)),
    line,
    structuredMetadata: md,
    parsed: {},
  };
}

describe("search helpers", () => {
  it("toSearchLogQL keeps LogQL and wraps free text", () => {
    expect(toSearchLogQL(' {a="b"} |= "x" ')).toBe('{a="b"} |= "x"');
    expect(toSearchLogQL("")).toBe('{service_name=~".+"}');
    expect(toSearchLogQL('say "hi"')).toBe('{service_name=~".+"} |= "say \\"hi\\""');
  });

  it("cursor round trip and validation", () => {
    expect(decodeCursor(encodeCursor("123", 2))).toEqual({ ns: "123", skip: 2 });
    expect(() => decodeCursor("bm9wZQ")).toThrow(ValidationError);
  });

  it("mergeStreams orders across streams with stable ties", () => {
    const streams: LokiStreamResult[] = [
      { labels: { s: "b" }, entries: [entry("3", "b3"), entry("1", "b1")] },
      {
        labels: { s: "a" },
        entries: [entry("2", "a2"), entry("3", "a3"), entry("3", "a3x"), entry("10", "a10")],
      },
    ];
    expect(mergeStreams(streams, "asc").map((f) => f.entry.line)).toEqual([
      "b1",
      "a2",
      "a3",
      "a3x",
      "b3",
      "a10",
    ]);
    expect(mergeStreams(streams, "desc").map((f) => f.entry.line)).toEqual([
      "a10",
      "a3",
      "a3x",
      "b3",
      "a2",
      "b1",
    ]);
    const same: LokiStreamResult[] = [{ labels: {}, entries: [entry("1", "x"), entry("1", "x")] }];
    expect(mergeStreams(same, "asc")).toHaveLength(2);
  });

  it("entryToLogRecord maps an OTLP record read back from Loki", () => {
    const body = fixture("queryRange.otlp-record").response.body as {
      data: {
        result: {
          stream: Record<string, string>;
          values: [string, string, { structuredMetadata: Record<string, string> }][];
        }[];
      };
    };
    const s = body.data.result[0]!;
    const v = s.values[0]!;
    const r = entryToLogRecord(s.stream, entry(v[0], v[1], v[2].structuredMetadata));
    expect(r).toMatchObject({
      body: "otlp json body c023892b83",
      severity: Severity.Error,
      severityText: "ERROR",
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      spanId: "00f067aa0ba902b7",
      traceFlags: 1,
      resource: { "service.name": "otlp-json-c023892b83", "deployment.environment": "test" },
      attributes: {
        request_id: "otlp-c023892b83",
        http_status_code: "503",
        scope_name: "grow-sdk-verify",
        detected_level: "error",
      },
    });
    expect(r.attributes).not.toHaveProperty("flags");
    expect(r.attributes).not.toHaveProperty("trace_id");
    expect(r.observedTimestamp).toEqual(new Date(1795035007690));
  });

  it("entryToLogRecord severity fallbacks and SDK metadata", () => {
    const sdk = entryToLogRecord(
      { level: "warn", host_name: "h" },
      {
        ...entry("1000000", "x", {
          severity_text: "WARNING",
          severity_number: "14",
          trace_id: "4BF92F3577B34DA6A3CE929D0E0E4736",
          span_id: "bad",
          trace_flags: "1",
          flags: "7",
        }),
        parsed: { status: "200" },
      },
    );
    expect(sdk).toMatchObject({
      severity: 14,
      severityText: "WARNING",
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      traceFlags: 1,
    });
    expect(sdk.spanId).toBeUndefined();
    expect(sdk.attributes).toEqual({ flags: "7", status: "200" });
    expect(sdk.resource).toEqual({ level: "warn", "host.name": "h" });
    expect(entryToLogRecord({}, entry("1", "x", { severity_text: "error" })).severity).toBe(
      Severity.Error,
    );
    expect(entryToLogRecord({}, entry("1", "x", { detected_level: "debug" })).severityText).toBe(
      "DEBUG",
    );
    expect(entryToLogRecord({ level: "fatal" }, entry("1", "x")).severity).toBe(Severity.Fatal);
    const plain = entryToLogRecord(
      {},
      entry("1", "x", { trace_id: "nothex", observed_timestamp: "abc" }),
    );
    expect(plain).toMatchObject({ severity: Severity.Info, severityText: "INFO" });
    expect(plain.traceId).toBeUndefined();
    expect(plain.observedTimestamp).toBeUndefined();
    expect(
      entryToLogRecord(
        {},
        entry("1", "x", { trace_id: "4bf92f3577b34da6a3ce929d0e0e4736", trace_flags: "x" }),
      ).traceFlags,
    ).toBeUndefined();
  });
});

describe("LokiLogQueryClient.search pagination", () => {
  function sourceWith(all: { ns: string; line: string; stream: string }[]): {
    source: LokiQuerySource;
    calls: unknown[];
  } {
    const calls: unknown[] = [];
    const source = {
      queryRange: async (
        _q: string,
        o: { start?: unknown; end?: unknown; limit?: number; direction?: string } = {},
      ) => {
        calls.push(o);
        const start = o.start === undefined ? 0n : (o.start as bigint);
        const end = o.end === undefined ? 1n << 70n : (o.end as bigint);
        const fwd = o.direction === "forward";
        const hits = all
          .filter((e) => BigInt(e.ns) >= start && BigInt(e.ns) < end)
          .sort(
            (a, b) =>
              (BigInt(a.ns) < BigInt(b.ns)
                ? -1
                : BigInt(a.ns) > BigInt(b.ns)
                  ? 1
                  : a.line < b.line
                    ? -1
                    : 1) * (fwd ? 1 : -1),
          )
          .slice(0, o.limit);
        const streams = new Map<string, LokiStreamResult>();
        for (const h of hits) {
          const s = streams.get(h.stream) ?? { labels: { s: h.stream }, entries: [] };
          s.entries.push(entry(h.ns, h.line));
          streams.set(h.stream, s);
        }
        return {
          resultType: "streams" as const,
          streams: [...streams.values()],
          vector: [],
          matrix: [],
          stats: {},
        };
      },
      labels: async () => ["s"],
      series: async (): Promise<Record<string, string>[]> => [
        { b: "2", a: "1" },
        { a: "1", b: "2" },
        { z: "0" },
      ],
      tail: async () => {
        throw new Error("unused");
      },
    } satisfies LokiQuerySource;
    return { source, calls };
  }

  const data = [
    { ns: "100", line: "a", stream: "x" },
    { ns: "200", line: "b", stream: "x" },
    { ns: "200", line: "c", stream: "y" },
    { ns: "200", line: "d", stream: "y" },
    { ns: "300", line: "e", stream: "x" },
  ];

  it("pages through equal timestamps without loss or duplicates (desc and asc)", async () => {
    for (const order of ["desc", "asc"] as const) {
      const { source } = sourceWith(data);
      const q = new LokiLogQueryClient(source);
      const seen: string[] = [];
      let cursor: string | undefined;
      let pages = 0;
      do {
        const page = await q.search('{s=~".+"}', {
          limit: 2,
          order,
          ...(cursor === undefined ? {} : { cursor }),
        });
        seen.push(...page.records.map((r) => r.body));
        cursor = page.nextCursor;
        pages++;
      } while (cursor !== undefined && pages < 10);
      const expected = order === "desc" ? ["e", "b", "c", "d", "a"] : ["a", "b", "c", "d", "e"];
      expect(seen, order).toEqual(expected);
    }
  });

  it("sends the window and validates limit; listStreams/listLabels", async () => {
    const { source, calls } = sourceWith(data);
    const q = new LokiLogQueryClient(source);
    const ctl = new AbortController();
    const res = await q.search("free text", {
      start: new Date(0),
      end: new Date(1),
      limit: 10,
      signal: ctl.signal,
    });
    expect(res.nextCursor).toBeUndefined();
    expect(calls[0]).toMatchObject({
      start: new Date(0),
      end: new Date(1),
      limit: 10,
      direction: "backward",
      signal: ctl.signal,
    });
    await expect(q.search("x", { limit: 0 })).rejects.toThrow(/limit/);
    await expect(q.search("x", { limit: 5001 })).rejects.toThrow(/limit/);
    expect(await q.listStreams()).toEqual(['{a="1", b="2"}', '{z="0"}']);
    expect(await q.listLabels()).toEqual(["s"]);
  });
});

describe("logClient + exporters against a fake Loki", () => {
  it("json: send → flush → search, listStreams, listLabels, waitForLog", async () => {
    const s = await startFakeLoki();
    servers.push(s);
    const loki = new LokiClient({ url: s.url, retry: { maxRetries: 0 } });
    const logs = loki.logClient({ resource: { "service.name": "svc", "service.version": "1" } });
    const query = loki.logQueryClient();
    const log = logs.logger("test");
    log.warn("hello world", { user_id: 7, password: "hunter2" });
    logs.send({
      message: "second",
      severity: Severity.Error,
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      spanId: "00f067aa0ba902b7",
    });
    await logs.flush();
    expect(s.entries).toHaveLength(2);
    expect(s.entries[0]!.labels).toEqual({ service_name: "svc", level: "warn" });
    expect(s.entries[0]!.metadata).toMatchObject({
      service_version: "1",
      user_id: "7",
      password: "[REDACTED]",
      logger_name: "test",
    });
    const found = await query.search("hello");
    expect(found.records).toHaveLength(1);
    expect(found.records[0]).toMatchObject({
      body: "hello world",
      severity: Severity.Warn,
      resource: { "service.name": "svc" },
    });
    const err = await query.waitForLog('{service_name="svc"}', {
      predicate: (r) => r.traceId !== undefined,
      timeoutMs: 2000,
      intervalMs: 50,
    });
    expect(err).toMatchObject({
      body: "second",
      spanId: "00f067aa0ba902b7",
      severity: Severity.Error,
    });
    expect(await query.listStreams()).toEqual([
      '{level="error", service_name="svc"}',
      '{level="warn", service_name="svc"}',
    ]);
    expect(await query.listLabels()).toEqual(["level", "service_name"]);
    logs.send({ message: "flushed on close" });
    await loki.close();
    expect(s.entries).toHaveLength(3);
  });

  it("protobuf and otlp protocols, custom allowed labels", async () => {
    const s = await startFakeLoki();
    servers.push(s);
    const loki = new LokiClient({ url: s.url, retry: { maxRetries: 0 } });
    const pb = loki.logClient({ protocol: "protobuf", batch: { gzip: false } });
    pb.send({ message: "pb" });
    await pb.flush();
    const otlp = loki.logClient({ protocol: "otlp" });
    otlp.send({ message: "otlp" });
    await otlp.flush();
    const custom = loki.logClient({ allowedLabels: ["app"] });
    custom.send({ message: "c", attributes: { app: "web", pod: "p1" } });
    await custom.flush();
    const types = s.requests.map((r) => `${r.url} ${r.headers["content-type"]}`);
    expect(types).toEqual([
      "/loki/api/v1/push application/x-protobuf",
      "/otlp/v1/logs application/json",
      "/loki/api/v1/push application/json",
    ]);
    expect(s.entries[0]!.labels).toEqual({ app: "web" });
    expect(s.entries[0]!.metadata.pod).toBe("p1");
    expect(pb.stats()).toMatchObject({ sent: 1, failed: 0 });
    await loki.close();
  });

  it("exporters report rejected batches and rethrow foreign errors", async () => {
    const records: LogRecord[] = [
      {
        timestamp: new Date(),
        severity: Severity.Info,
        severityText: "INFO",
        body: "x",
        attributes: {},
        resource: {},
      },
    ];
    const s = await startServer([fixtureResponse("push.too-old")]);
    servers.push(s);
    const loki = new LokiClient({ url: s.url, retry: { maxRetries: 0 } });
    const exp = new LokiLogExporter(loki);
    const res = await exp.export(records, { gzip: true });
    expect(res).toMatchObject({ accepted: 0, rejected: 1, error: { code: "VALIDATION" } });
    expect(await exp.export([], { gzip: false })).toEqual({ accepted: 0, rejected: 0 });
    await exp.shutdown();
    const boom: LokiPusher = {
      push: () => Promise.reject(new Error("boom")),
      pushOtlp: () => Promise.reject(new Error("boom")),
    };
    await expect(new LokiLogExporter(boom).export(records, { gzip: false })).rejects.toThrow(
      "boom",
    );
    const otlp = new LokiOtlpExporter(boom);
    await expect(
      otlp.export(records, { gzip: false, signal: new AbortController().signal }),
    ).rejects.toThrow("boom");
    await otlp.shutdown();
    const ok: LokiPusher = { push: () => Promise.resolve(), pushOtlp: () => Promise.resolve() };
    expect(await new LokiOtlpExporter(ok).export(records, { gzip: false })).toEqual({
      accepted: 1,
      rejected: 0,
    });
    expect(
      await new LokiLogExporter(ok).export(records, {
        gzip: false,
        signal: new AbortController().signal,
      }),
    ).toEqual({ accepted: 1, rejected: 0 });
    await loki.close();
  });
});
