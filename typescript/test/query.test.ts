import { afterEach, describe, expect, it } from "vitest";
import {
  LokiClient,
  decodeQueryResult,
  decodeSeries,
  decodeStreams,
  decodeStringList,
  decodeTailFrame,
  nanosToDate,
  stringMap,
  toNanos,
} from "../src/index.js";
import { fixture, fixtureResponse } from "./helpers/loki.js";
import { type MockResponse, type MockServer, startServer } from "./helpers/server.js";

const servers: MockServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function clientFor(
  ...responses: MockResponse[]
): Promise<{ client: LokiClient; s: MockServer }> {
  const s = await startServer(responses);
  servers.push(s);
  return { client: new LokiClient({ url: s.url, retry: { maxRetries: 0 } }), s };
}

function params(url: string): URLSearchParams {
  return new URL(url, "http://x").searchParams;
}

describe("query", () => {
  it("decodes a metric vector", async () => {
    const { client, s } = await clientFor(fixtureResponse("query.metric-vector"));
    const q = fixture("query.metric-vector").request.query as Record<string, string>;
    const res = await client.query(q.query as string, {
      time: BigInt(q.time as string),
      limit: 10,
      direction: "forward",
    });
    expect(res.resultType).toBe("vector");
    expect(res.vector).toHaveLength(3);
    expect(res.vector[2]).toEqual({
      metric: { level: "info" },
      timestamp: new Date(1795035067710),
      value: 8,
    });
    expect(res.streams).toEqual([]);
    const p = params(s.requests[0]!.url);
    expect(p.get("time")).toBe(q.time);
    expect(p.get("limit")).toBe("10");
    expect(p.get("direction")).toBe("forward");
    expect(s.requests[0]!.headers["x-loki-response-encoding-flags"]).toBe("categorize-labels");
    await client.close();
  });

  it("rejects log selectors and empty queries client side", async () => {
    const { client, s } = await clientFor({ status: 200 });
    await expect(client.query('{app="x"} |= "err"')).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.query("  ")).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.query("vector(1)", { limit: 0 })).rejects.toThrow(/limit/);
    expect(s.requests).toHaveLength(0);
    await client.close();
  });

  it("maps a parse error to ValidationError with Loki's text", async () => {
    const { client } = await clientFor(fixtureResponse("query.parse-error"));
    await expect(client.query("sum(")).rejects.toMatchObject({
      code: "VALIDATION",
      httpStatus: 400,
      message: expect.stringMatching(/^query: parse error/),
    });
    await client.close();
  });
});

describe("queryRange", () => {
  it("decodes streams and sends the parameters", async () => {
    const { client, s } = await clientFor(fixtureResponse("queryRange.streams-forward"));
    const q = fixture("queryRange.streams-forward").request.query as Record<string, string>;
    const res = await client.queryRange(q.query as string, {
      start: q.start as string,
      end: BigInt(q.end as string),
      limit: 100,
      direction: "forward",
      stepMs: 30_000,
      intervalMs: 1500,
    });
    expect(res.resultType).toBe("streams");
    expect(res.streams).toHaveLength(2);
    expect(res.streams[1]!.entries[0]).toEqual({
      timestampNs: "1795034767064713072",
      timestamp: new Date(1795034767064),
      line: "hello c023892b83 first",
      structuredMetadata: {},
      parsed: {},
    });
    expect(res.stats).toHaveProperty("summary");
    const p = params(s.requests[0]!.url);
    expect(p.get("start")).toBe(q.start);
    expect(p.get("end")).toBe(q.end);
    expect(p.get("step")).toBe("30000ms");
    expect(p.get("interval")).toBe("1500ms");
    await client.close();
  });

  it("decodes categorized labels (structured metadata + parsed)", async () => {
    const { client } = await clientFor(fixtureResponse("queryRange.categorized-labels"));
    const res = await client.queryRange('{run="c023892b83"} |= "json line" | json');
    const e = res.streams[0]!.entries[1]!;
    expect(e.structuredMetadata).toEqual({
      detected_level: "info",
      request_id: "req-c023892b83",
      trace_id: "0af7651916cd43dd8448eb211c80319c",
    });
    expect(e.parsed).toEqual({ msg: "json line c023892b83", status: "503" });
    expect(res.streams[0]!.labels).toEqual({
      env: "test",
      level: "info",
      run: "c023892b83",
      service_name: "sdk-verify",
    });
    await client.close();
  });

  it("decodes a matrix and validates options", async () => {
    const { client } = await clientFor(fixtureResponse("queryRange.metric-matrix"));
    const res = await client.queryRange('sum by (level) (count_over_time({run="x"}[1m]))', {
      start: new Date(0),
      end: "2026-11-18T00:00:00Z",
    });
    expect(res.resultType).toBe("matrix");
    expect(res.matrix[0]!.samples.length).toBeGreaterThan(0);
    expect(typeof res.matrix[0]!.samples[0]!.value).toBe("number");
    await expect(client.queryRange('{a="b"}', { limit: 5001 })).rejects.toThrow(/limit/);
    await expect(client.queryRange('{a="b"}', { stepMs: 0 })).rejects.toThrow(/stepMs/);
    await expect(client.queryRange('{a="b"}', { start: "not a time" })).rejects.toThrow(/start/);
    await client.close();
  });

  it("maps recorded errors", async () => {
    for (const [name, code] of [
      ["queryRange.end-before-start", "VALIDATION"],
      ["queryRange.empty-compatible-matcher", "VALIDATION"],
      ["queryRange.limit-too-high", "VALIDATION"],
      ["queryRange.mt-no-tenant", "AUTHENTICATION"],
    ] as const) {
      const { client } = await clientFor(fixtureResponse(name));
      await expect(client.queryRange('{a="b"}'), name).rejects.toMatchObject({ code });
      await client.close();
    }
  });
});

describe("labels, labelValues, series", () => {
  it("labels decodes data and a missing data as []", async () => {
    const { client, s } = await clientFor(fixtureResponse("labels.ok"), {
      status: 200,
      body: { status: "success" },
    });
    expect(
      await client.labels({ start: new Date(1000), end: new Date(2000), query: '{a="b"}' }),
    ).toContain("service_name");
    expect(params(s.requests[0]!.url).get("start")).toBe("1000000000");
    expect(params(s.requests[0]!.url).get("query")).toBe('{a="b"}');
    expect(await client.labels()).toEqual([]);
    await client.close();
  });

  it("labelValues path-encodes, validates and decodes", async () => {
    const { client, s } = await clientFor(
      fixtureResponse("labelValues.ok"),
      fixtureResponse("labelValues.unknown-label"),
    );
    expect(await client.labelValues("level", { query: '{run="x"}' })).toEqual(
      fixture("labelValues.ok").response.body &&
        (fixture("labelValues.ok").response.body as { data: string[] }).data,
    );
    expect(s.requests[0]!.url.startsWith("/loki/api/v1/label/level/values?")).toBe(true);
    expect(await client.labelValues("does_not_exist")).toEqual([]);
    await expect(client.labelValues("bad-name")).rejects.toMatchObject({ code: "VALIDATION" });
    await client.close();
  });

  it("series sends repeated match[] and requires a matcher", async () => {
    const { client, s } = await clientFor(fixtureResponse("series.ok"));
    const matchers = fixture("series.ok").request.query!["match[]"] as string[];
    const res = await client.series(matchers, { start: 1n, end: 2n });
    expect(res).toHaveLength(4);
    expect(res[0]).toMatchObject({ service_name: "sdk-verify" });
    expect(params(s.requests[0]!.url).getAll("match[]")).toEqual(matchers);
    await expect(client.series([])).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.series(" ")).rejects.toMatchObject({ code: "VALIDATION" });
    await client.close();
  });

  it("series maps an invalid matcher", async () => {
    const { client } = await clientFor(fixtureResponse("series.invalid-matcher"));
    await expect(client.series("{a")).rejects.toMatchObject({ code: "VALIDATION" });
    await client.close();
  });
});

describe("indexStats, volume, volumeRange, formatQuery", () => {
  it("indexStats decodes the un-enveloped body", async () => {
    const { client } = await clientFor(
      fixtureResponse("indexStats.after-flush"),
      fixtureResponse("indexStats.invalid-query"),
    );
    expect(await client.indexStats('{run="x"}', { start: new Date(0), end: new Date() })).toEqual({
      streams: 8,
      chunks: 8,
      bytes: 0,
      entries: 13,
    });
    await expect(client.indexStats("{")).rejects.toMatchObject({ code: "VALIDATION" });
    await expect(client.indexStats("")).rejects.toMatchObject({ code: "VALIDATION" });
    await client.close();
  });

  it("volume sends targetLabels/aggregateBy and decodes the vector", async () => {
    const { client, s } = await clientFor(
      fixtureResponse("volume.target-labels"),
      fixtureResponse("volume.ok"),
    );
    const res = await client.volume('{run="x"}', {
      targetLabels: ["level"],
      aggregateBy: "labels",
      limit: 10,
    });
    expect(res.vector[0]).toMatchObject({ metric: { level: "" }, value: 721 });
    const p = params(s.requests[0]!.url);
    expect(p.get("targetLabels")).toBe("level");
    expect(p.get("aggregateBy")).toBe("labels");
    expect(p.get("limit")).toBe("10");
    await client.volume('{run="x"}', { targetLabels: [] });
    expect(params(s.requests[1]!.url).has("targetLabels")).toBe(false);
    await expect(client.volume("")).rejects.toThrow(/empty/);
    await expect(client.volume('{a="b"}', { limit: -1 })).rejects.toThrow(/limit/);
    await client.close();
  });

  it("volumeRange decodes the matrix with a step", async () => {
    const { client, s } = await clientFor(fixtureResponse("volumeRange.before-flush"));
    const res = await client.volumeRange('{run="x"}', { stepMs: 60_000 });
    expect(res.resultType).toBe("matrix");
    expect(res.matrix[0]!.samples.map((x) => x.value)).toEqual([544, 74, 187]);
    expect(params(s.requests[0]!.url).get("step")).toBe("60000ms");
    await expect(client.volumeRange(" ")).rejects.toThrow(/empty/);
    await client.close();
  });

  it("volumeRange converts Loki's single-point vector answer into a matrix", async () => {
    const { client } = await clientFor({
      status: 200,
      body: {
        status: "success",
        data: {
          resultType: "vector",
          result: [{ metric: { run: "probe" }, value: [1795037039.999, "30"] }],
        },
      },
    });
    const res = await client.volumeRange('{run="probe"}', { stepMs: 60_000 });
    expect(res.resultType).toBe("matrix");
    expect(res.vector).toEqual([]);
    expect(res.matrix).toEqual([
      { metric: { run: "probe" }, samples: [{ timestamp: new Date(1795037039999), value: 30 }] },
    ]);
    await client.close();
  });

  it("formatQuery returns data and maps the JSON error", async () => {
    const { client } = await clientFor(
      fixtureResponse("formatQuery.ok"),
      fixtureResponse("formatQuery.parse-error"),
      { status: 200, body: { status: "success" } },
    );
    expect(await client.formatQuery('{app="x"} |= "err" | json | status >= 500')).toBe(
      '{app="x"} |= "err" | json | status>=500',
    );
    await expect(client.formatQuery("{")).rejects.toMatchObject({
      code: "VALIDATION",
      message: expect.stringContaining("parse error"),
    });
    await expect(client.formatQuery("{a}")).rejects.toMatchObject({ code: "UNAVAILABLE" });
    await expect(client.formatQuery("")).rejects.toMatchObject({ code: "VALIDATION" });
    await client.close();
  });
});

describe("result decoders", () => {
  it("handle scalar, special values and malformed input", () => {
    expect(
      decodeQueryResult({ data: { resultType: "scalar", result: [1.5, "+Inf"] } }).scalar,
    ).toEqual({
      timestamp: new Date(1500),
      value: Number.POSITIVE_INFINITY,
    });
    const v = decodeQueryResult({
      data: {
        resultType: "vector",
        result: [
          { metric: {}, value: [1, "-Inf"] },
          { value: [1, "NaN"] },
          { value: [1, 7] },
          { value: [1, null] },
        ],
      },
    }).vector;
    expect(v.map((x) => x.value)).toEqual([Number.NEGATIVE_INFINITY, Number.NaN, 7, Number.NaN]);
    expect(decodeQueryResult({ data: { resultType: "vector" } }).vector).toEqual([]);
    expect(decodeQueryResult({ data: { resultType: "matrix" } }).matrix).toEqual([]);
    expect(
      decodeQueryResult({ data: { resultType: "matrix", result: [{ metric: {} }] } }).matrix,
    ).toEqual([{ metric: {}, samples: [] }]);
    expect(
      decodeQueryResult({ data: { resultType: "streams", result: [{ stream: { a: "b" } }] } })
        .streams,
    ).toEqual([{ labels: { a: "b" }, entries: [] }]);
    expect(() => decodeQueryResult({})).toThrow(/missing data/);
    expect(() => decodeQueryResult({ data: { resultType: "weird" } })).toThrow(
      /unknown resultType/,
    );
    expect(() => decodeQueryResult({ data: { resultType: "vector", result: [1] } })).toThrow(
      /vector/,
    );
    expect(() => decodeQueryResult({ data: { resultType: "matrix", result: [1] } })).toThrow(
      /matrix/,
    );
    expect(() => decodeQueryResult({ data: { resultType: "scalar", result: "x" } })).toThrow(
      /sample/,
    );
    expect(() => decodeStreams({})).toThrow(/not a list/);
    expect(() => decodeStreams([1])).toThrow(/malformed stream/);
    expect(() => decodeStreams([{ values: [[1, "x"]] }])).toThrow(/stream value/);
    expect(() => decodeStreams([{ values: [["12a", "x"]] }])).toThrow(/timestamp/);
    expect(decodeStreams(null)).toEqual([]);
    expect(() => decodeStringList(null)).toThrow();
    expect(() => decodeStringList({ data: {} })).toThrow(/not a list/);
    expect(decodeStringList({ data: ["a", 1] })).toEqual(["a"]);
    expect(() => decodeSeries(1)).toThrow();
    expect(() => decodeSeries({ data: 1 })).toThrow(/not a list/);
    expect(decodeSeries({})).toEqual([]);
    expect(stringMap({ a: 1, b: null, c: "x", d: { e: 1 } })).toEqual({
      a: "1",
      c: "x",
      d: '{"e":1}',
    });
    expect(stringMap("x")).toEqual({});
    expect(() => decodeTailFrame(1)).toThrow(/tail frame/);
    expect(
      decodeTailFrame({ streams: [], dropped_entries: [{ labels: { a: "b" }, timestamp: 5 }, 1] })
        .droppedEntries,
    ).toEqual([{ labels: { a: "b" }, timestampNs: "5" }]);
  });

  it("time helpers", () => {
    expect(toNanos("  123 ")).toBe("123");
    expect(nanosToDate(1_500_000_000n)).toEqual(new Date(1500));
    expect(() => toNanos("")).toThrow();
  });
});
