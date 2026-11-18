import { gunzipSync } from "node:zlib";
import { uncompress } from "snappyjs";
import { afterEach, describe, expect, it } from "vitest";
import {
  LokiClient,
  Severity,
  decodeRpcStatusMessage,
  encodePushRequest,
  recordsToOtlpJson,
  snappyCompress,
  toOtlpValue,
  type LogRecord,
} from "../src/index.js";
import { decodeProto, fixture, fixtureResponse } from "./helpers/loki.js";
import { type MockServer, startServer } from "./helpers/server.js";

const servers: MockServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function serve(...responses: Parameters<typeof startServer>[0][]): Promise<MockServer> {
  const s = await startServer(responses[0] ?? [{ status: 204 }]);
  servers.push(s);
  return s;
}

const fast = { retry: { maxRetries: 2, initialBackoffMs: 1 } };
const dec = new TextDecoder();

describe("push (JSON)", () => {
  it("sends the recorded JSON body with structured metadata", async () => {
    const fx = fixture("push.json-structured-metadata");
    const s = await serve([fixtureResponse("push.json-structured-metadata")]);
    const client = new LokiClient({ url: s.url, tenantId: "t1", ...fast });
    const body = fx.request.body as {
      streams: {
        stream: Record<string, string>;
        values: [string, string, Record<string, string>?][];
      }[];
    };
    await client.push(
      body.streams.map((st) => ({
        labels: st.stream,
        entries: st.values.map(([ts, line, md]) => ({
          timestamp: ts,
          line,
          ...(md === undefined ? {} : { metadata: md }),
        })),
      })),
    );
    const req = s.requests[0]!;
    expect(req.method).toBe("POST");
    expect(req.url).toBe("/loki/api/v1/push");
    expect(req.headers["content-type"]).toBe("application/json");
    expect(req.headers["x-scope-orgid"]).toBe("t1");
    expect(JSON.parse(req.body)).toEqual(body);
    await client.close();
  });

  it("accepts Date, bigint and RFC 3339 timestamps and gzip", async () => {
    const s = await serve([{ status: 204 }]);
    const client = new LokiClient({ url: s.url, ...fast });
    await client.push(
      [
        {
          labels: { app: "x" },
          entries: [
            { timestamp: new Date(1_700_000_000_123), line: "a" },
            { timestamp: 1_700_000_000_123_456_789n, line: "b", metadata: {} },
            { timestamp: "2023-11-14T22:13:20.5Z", line: "c" },
          ],
        },
        { labels: { app: "empty" }, entries: [] },
      ],
      { gzip: true },
    );
    const req = s.requests[0]!;
    expect(req.headers["content-encoding"]).toBe("gzip");
    const body = JSON.parse(gunzipSync(req.rawBody).toString("utf8"));
    expect(body).toEqual({
      streams: [
        {
          stream: { app: "x" },
          values: [
            ["1700000000123000000", "a"],
            ["1700000000123456789", "b"],
            ["1700000000500000000", "c"],
          ],
        },
      ],
    });
    await client.push([]);
    await client.push([{ labels: { a: "b" }, entries: [] }]);
    expect(s.requests).toHaveLength(1);
    await client.close();
  });

  it("validates labels, metadata, lines and timestamps client side", async () => {
    const client = new LokiClient({ url: "http://127.0.0.1:1" });
    const entry = { timestamp: new Date(), line: "x" };
    await expect(
      client.push([{ labels: { "bad-label": "v" }, entries: [entry] }]),
    ).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await expect(client.push([{ labels: { __name__: "v" }, entries: [entry] }])).rejects.toThrow(
      /reserved/,
    );
    await expect(client.push([{ labels: { a: "" }, entries: [entry] }])).rejects.toThrow(
      /non-empty/,
    );
    await expect(
      client.push([{ labels: { a: "b" }, entries: [{ ...entry, metadata: { "x-y": "1" } }] }]),
    ).rejects.toThrow(/structured metadata key/);
    await expect(
      client.push([
        { labels: { a: "b" }, entries: [{ ...entry, metadata: { n: 1 as unknown as string } }] },
      ]),
    ).rejects.toThrow(/must be a string/);
    await expect(
      client.push([
        { labels: { a: "b" }, entries: [{ timestamp: new Date(), line: 5 as unknown as string }] },
      ]),
    ).rejects.toThrow(/line/);
    await expect(
      client.push([{ labels: { a: "b" }, entries: [{ timestamp: "yesterday", line: "x" }] }]),
    ).rejects.toThrow(/timestamp/);
    await expect(
      client.push([
        { labels: { a: "b" }, entries: [{ timestamp: new Date(Number.NaN), line: "x" }] },
      ]),
    ).rejects.toThrow(/invalid date/);
    await expect(
      client.push([{ labels: { a: "b" }, entries: [{ timestamp: -1n, line: "x" }] }]),
    ).rejects.toThrow(/negative/);
    await expect(
      client.push([{ labels: { a: "b" }, entries: [entry] }], { format: "xml" as "json" }),
    ).rejects.toThrow(/unknown format/);
    await client.close();
  });

  it("maps recorded push errors", async () => {
    const cases: [string, string, number][] = [
      ["push.invalid-label-name", "VALIDATION", 400],
      ["push.too-old", "VALIDATION", 400],
      ["push.mt-no-tenant", "AUTHENTICATION", 401],
      ["push.mt-invalid-tenant-slash", "VALIDATION", 400],
    ];
    for (const [name, code, status] of cases) {
      const s = await serve([fixtureResponse(name)]);
      const client = new LokiClient({ url: s.url, ...fast });
      const err = await client
        .push([{ labels: { a: "b" }, entries: [{ timestamp: new Date(), line: "x" }] }])
        .catch((e: unknown) => e);
      expect(err, name).toMatchObject({ code, httpStatus: status, retryable: false });
      expect(s.requests, name).toHaveLength(1);
      await client.close();
    }
  });

  it("retries 'Ingester is shutting down' and gateway 503/502, then succeeds", async () => {
    const s = await serve([
      { status: 500, body: "rpc error: code = Unknown desc = Ingester is shutting down\n" },
      fixtureResponse("transport.gateway-fault-502"),
      { status: 204 },
    ]);
    const client = new LokiClient({ url: s.url, ...fast });
    await client.push([{ labels: { a: "b" }, entries: [{ timestamp: new Date(), line: "x" }] }]);
    expect(s.requests).toHaveLength(3);
    await client.close();

    const down = await serve([
      { status: 500, body: "rpc error: code = Unknown desc = Ingester is shutting down\n" },
    ]);
    const c2 = new LokiClient({ url: down.url, retry: { maxRetries: 0 } });
    await expect(
      c2.push([{ labels: { a: "b" }, entries: [{ timestamp: new Date(), line: "x" }] }]),
    ).rejects.toMatchObject({ code: "UNAVAILABLE", retryable: true, httpStatus: 500 });
    await c2.close();
  });

  it("treats status 260 (blocked ingestion) as a non-retryable error", async () => {
    const s = await serve([{ status: 260 }]);
    const client = new LokiClient({ url: s.url, ...fast });
    await expect(
      client.push([{ labels: { a: "b" }, entries: [{ timestamp: new Date(), line: "x" }] }]),
    ).rejects.toMatchObject({ code: "UNAVAILABLE", httpStatus: 260, retryable: false });
    await client.close();
  });

  it("maps Loki's own 429 to RateLimitError", async () => {
    const s = await serve([
      {
        status: 429,
        body: "Ingestion rate limit exceeded for user fake (limit: 4194304 bytes/sec) while attempting to ingest '1' lines totaling '10' bytes, reduce log volume or contact your Loki administrator to see if the limit can be increased\n",
      },
    ]);
    const client = new LokiClient({ url: s.url, retry: { maxRetries: 0 } });
    await expect(
      client.push([{ labels: { a: "b" }, entries: [{ timestamp: new Date(), line: "x" }] }]),
    ).rejects.toMatchObject({ code: "RATE_LIMITED", httpStatus: 429, retryable: true });
    await client.close();
  });
});

describe("push (protobuf + snappy)", () => {
  it("encodes the recorded PushRequest exactly", async () => {
    const fx = fixture("push.protobuf-snappy");
    const decoded = (
      fx.request.body as {
        decoded: {
          streams: {
            entries: {
              timestamp: string;
              line: string;
              structuredMetadata: { name: string; value: string }[];
            }[];
          }[];
        };
      }
    ).decoded;
    const entry = decoded.streams[0]!.entries[0]!;
    const s = await serve([fixtureResponse("push.protobuf-snappy")]);
    const client = new LokiClient({ url: s.url, ...fast });
    await client.push(
      [
        {
          labels: { service_name: "sdk-verify", env: "test", run: "c023892b83", enc: "protobuf" },
          entries: [
            {
              timestamp: entry.timestamp,
              line: entry.line,
              metadata: { request_id: "pb-c023892b83" },
            },
          ],
        },
      ],
      { format: "protobuf", gzip: true },
    );
    const req = s.requests[0]!;
    expect(req.headers["content-type"]).toBe("application/x-protobuf");
    expect(req.headers["content-encoding"]).toBeUndefined();
    const raw = uncompress(new Uint8Array(req.rawBody));
    const pushReq = decodeProto(raw);
    const stream = decodeProto(pushReq.get(1)![0] as Uint8Array);
    expect(dec.decode(stream.get(1)![0] as Uint8Array)).toBe(
      '{enc="protobuf", env="test", run="c023892b83", service_name="sdk-verify"}',
    );
    const e = decodeProto(stream.get(2)![0] as Uint8Array);
    const ts = decodeProto(e.get(1)![0] as Uint8Array);
    expect((ts.get(1)![0] as bigint) * 1_000_000_000n + (ts.get(2)![0] as bigint)).toBe(
      BigInt(entry.timestamp),
    );
    expect(dec.decode(e.get(2)![0] as Uint8Array)).toBe(entry.line);
    const pair = decodeProto(e.get(3)![0] as Uint8Array);
    expect(dec.decode(pair.get(1)![0] as Uint8Array)).toBe("request_id");
    expect(dec.decode(pair.get(2)![0] as Uint8Array)).toBe("pb-c023892b83");
    await client.close();
  });

  it("omits zero timestamp fields, quotes label values and round-trips snappy", () => {
    const bytes = encodePushRequest([
      { labels: { a: 'q"\\\n' }, entries: [{ timestamp: 0n, line: "" }] },
    ]);
    const stream = decodeProto(decodeProto(bytes).get(1)![0] as Uint8Array);
    expect(dec.decode(stream.get(1)![0] as Uint8Array)).toBe('{a="q\\"\\\\\\n"}');
    const e = decodeProto(stream.get(2)![0] as Uint8Array);
    expect((e.get(1)![0] as Uint8Array).length).toBe(0);
    const big = new Uint8Array(5000).fill(65);
    expect(uncompress(snappyCompress(big))).toEqual(big);
    expect(snappyCompress(big).length).toBeLessThan(big.length);
  });

  it("maps the recorded protobuf errors", async () => {
    const s = await serve([fixtureResponse("push.protobuf-bad-labels")]);
    const client = new LokiClient({ url: s.url, ...fast });
    await expect(
      client.push([{ labels: { a: "b" }, entries: [{ timestamp: new Date(), line: "x" }] }], {
        format: "protobuf",
      }),
    ).rejects.toMatchObject({
      code: "VALIDATION",
      message: expect.stringContaining("couldn't parse labels"),
    });
    await client.close();
  });
});

describe("pushOtlp", () => {
  const record: LogRecord = {
    timestamp: new Date(1_795_035_007_690),
    severity: Severity.Error,
    severityText: "ERROR",
    body: "otlp json body",
    attributes: {
      "request.id": "r1",
      "http.status_code": 503,
      ratio: 0.5,
      ok: true,
      list: ["a", 1],
      obj: { k: "v" },
    },
    resource: { "service.name": "svc", "deployment.environment": "test" },
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    spanId: "00f067aa0ba902b7",
    traceFlags: 1,
  };

  it("posts OTLP/JSON matching the recorded request shape", async () => {
    const s = await serve([fixtureResponse("otlpLogs.json")]);
    const client = new LokiClient({ url: s.url, tenantId: "t", ...fast });
    await client.pushOtlp([record, { ...record, resource: { "service.name": "other" } }]);
    await client.pushOtlp([]);
    expect(s.requests).toHaveLength(1);
    const req = s.requests[0]!;
    expect(req.url).toBe("/otlp/v1/logs");
    expect(req.headers["x-scope-orgid"]).toBe("t");
    const body = JSON.parse(req.body);
    expect(body.resourceLogs).toHaveLength(2);
    const lr = body.resourceLogs[0].scopeLogs[0].logRecords[0];
    expect(lr).toMatchObject({
      timeUnixNano: "1795035007690000000",
      observedTimeUnixNano: "1795035007690000000",
      severityNumber: 17,
      severityText: "ERROR",
      body: { stringValue: "otlp json body" },
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      spanId: "00f067aa0ba902b7",
      flags: 1,
    });
    expect(lr.attributes).toContainEqual({ key: "http.status_code", value: { intValue: "503" } });
    expect(body.resourceLogs[0].resource.attributes).toContainEqual({
      key: "service.name",
      value: { stringValue: "svc" },
    });
    await client.close();
  });

  it("gzips, and maps protobuf google.rpc.Status error bodies to readable messages", async () => {
    const s = await serve([
      fixtureResponse("otlpLogs.invalid-body"),
      fixtureResponse("otlpLogs.empty-json"),
    ]);
    const client = new LokiClient({ url: s.url, ...fast });
    const err = await client.pushOtlp([record], { gzip: true }).catch((e: unknown) => e);
    expect(err).toMatchObject({
      code: "VALIDATION",
      message: "pushOtlp: proto: illegal wireType 6 (HTTP 400)",
    });
    expect(s.requests[0]!.headers["content-encoding"]).toBe("gzip");
    expect(JSON.parse(gunzipSync(s.requests[0]!.rawBody).toString()).resourceLogs).toHaveLength(1);
    await expect(client.pushOtlp([record])).rejects.toMatchObject({
      code: "VALIDATION",
      httpStatus: 422,
      message: expect.stringContaining("at least one valid stream"),
    });
    await client.close();
  });

  it("encodes values and decodes rpc status bodies", () => {
    expect(toOtlpValue(1.5)).toEqual({ doubleValue: 1.5 });
    expect(toOtlpValue(false)).toEqual({ boolValue: false });
    expect(
      recordsToOtlpJson([
        {
          ...record,
          traceId: undefined,
          spanId: undefined,
          traceFlags: undefined,
          observedTimestamp: new Date(0),
        } as unknown as LogRecord,
      ]),
    ).toMatchObject({
      resourceLogs: [{ scopeLogs: [{ logRecords: [{ observedTimeUnixNano: "0" }] }] }],
    });
    expect(decodeRpcStatusMessage(Uint8Array.from([0x08, 0x03, 0x12, 0x02, 0x68, 0x69]))).toBe(
      "hi",
    );
    expect(decodeRpcStatusMessage(Uint8Array.from([0x12, 0x09, 0x68]))).toBeUndefined();
    expect(decodeRpcStatusMessage(Uint8Array.from([0x0d, 0, 0, 0, 0]))).toBeUndefined();
    expect(decodeRpcStatusMessage(Uint8Array.from([0x80]))).toBeUndefined();
    expect(decodeRpcStatusMessage(Uint8Array.from([0x08, 0x80]))).toBeUndefined();
  });

  it("strips control characters when the body is not a Status message", async () => {
    const s = await serve([{ status: 400, body: "\u0001bad\u0002 body" }]);
    const client = new LokiClient({ url: s.url, ...fast });
    await expect(client.pushOtlp([record])).rejects.toMatchObject({
      message: "pushOtlp: bad body (HTTP 400)",
    });
    await client.close();
  });
});
