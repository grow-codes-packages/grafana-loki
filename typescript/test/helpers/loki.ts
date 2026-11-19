// Test helpers: recorded Loki fixtures and a small in-memory fake Loki HTTP server.

import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { type MockResponse, type MockServer, type RecordedRequest, startServer } from "./server.js";

/** A recorded fixture (`test/fixtures/<function>.<case>.json`). */
export interface Fixture {
  request: {
    method: string;
    path: string;
    query?: Record<string, string | string[]>;
    headers?: Record<string, string>;
    body?: unknown;
  };
  response: { status: number; contentType?: string; body?: unknown; frame?: unknown };
  note?: string;
}

/** Loads a fixture by name (without `.json`). */
export function fixture(name: string): Fixture {
  return JSON.parse(
    readFileSync(new URL(`../fixtures/${name}.json`, import.meta.url), "utf8"),
  ) as Fixture;
}

/** The fixture's response as a mock server response. */
export function fixtureResponse(name: string, extraHeaders: Record<string, string> = {}): MockResponse {
  const { response } = fixture(name);
  const headers: Record<string, string> = { ...extraHeaders };
  if (response.contentType) headers["content-type"] = response.contentType;
  const body = response.body;
  if (body === undefined || body === "") return { status: response.status, headers };
  if (typeof body === "string") return { status: response.status, headers, body };
  if (typeof body === "object" && body !== null && "base64" in body) {
    return {
      status: response.status,
      headers,
      body: Buffer.from((body as { base64: string }).base64, "base64"),
    };
  }
  return { status: response.status, headers, body: JSON.stringify(body) };
}

/** One stored entry of the fake. */
export interface StoredEntry {
  tenant: string;
  labels: Record<string, string>;
  ts: string;
  line: string;
  metadata: Record<string, string>;
}

/** Options of {@link startFakeLoki}. */
export interface FakeLokiOptions {
  /** Require `X-Scope-OrgID` (auth_enabled: true). */
  tenantRequired?: boolean;
  /** Override the response for a request (return undefined to fall through). */
  override?: (req: RecordedRequest, index: number) => MockResponse | undefined;
}

/** A running fake Loki. */
export interface FakeLoki extends MockServer {
  entries: StoredEntry[];
}

function parseQuery(url: string): URLSearchParams {
  return new URL(url, "http://x").searchParams;
}

function matches(q: string, e: StoredEntry): boolean {
  const selector = /^\{([^}]*)\}/.exec(q.trim());
  if (selector === null) return false;
  for (const m of (selector[1] as string).matchAll(/([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!=|=)\s*("(?:[^"\\]|\\.)*")/g)) {
    const [, name, op, quoted] = m as unknown as [string, string, string, string];
    const value = JSON.parse(quoted) as string;
    const actual = e.labels[name] ?? "";
    if (op === "=" && actual !== value) return false;
    if (op === "!=" && actual === value) return false;
    if (op === "=~" && !new RegExp(`^(?:${value})$`).test(actual)) return false;
  }
  const rest = q.trim().slice(selector[0].length);
  for (const m of rest.matchAll(/\|=\s*("(?:[^"\\]|\\.)*")/g)) {
    if (!e.line.includes(JSON.parse(m[1] as string) as string)) return false;
  }
  return true;
}

const notFound: MockResponse = { status: 404, body: "404 page not found\n" };

/** Starts a fake Loki that stores JSON pushes and answers query_range/labels/series. */
export async function startFakeLoki(options: FakeLokiOptions = {}): Promise<FakeLoki> {
  const entries: StoredEntry[] = [];
  const server = await startServer((req, index) => {
    const override = options.override?.(req, index);
    if (override !== undefined) return override;
    const path = req.url.split("?")[0] ?? "";
    if (path === "/ready") return { status: 200, body: "ready\n" };
    if (path === "/loki/api/v1/status/buildinfo") return fixtureResponse("buildInfo.ok");
    const tenantHeader = req.headers["x-scope-orgid"];
    const tenant = typeof tenantHeader === "string" ? tenantHeader : "fake";
    if (options.tenantRequired === true && tenantHeader === undefined) {
      return { status: 401, body: "no org id\n" };
    }
    if (path === "/loki/api/v1/push" && req.method === "POST") {
      if (req.headers["content-type"] !== "application/json") return { status: 204 };
      const raw = req.headers["content-encoding"] === "gzip" ? gunzipSync(req.rawBody) : req.rawBody;
      const body = JSON.parse(raw.toString("utf8")) as {
        streams: { stream: Record<string, string>; values: [string, string, Record<string, string>?][] }[];
      };
      for (const s of body.streams) {
        for (const [ts, line, md] of s.values) {
          entries.push({ tenant, labels: { ...s.stream }, ts, line, metadata: md ?? {} });
        }
      }
      return { status: 204 };
    }
    if (path === "/otlp/v1/logs" && req.method === "POST") return { status: 204 };
    const params = parseQuery(req.url);
    const mine = entries.filter((e) => e.tenant === tenant);
    if (path === "/loki/api/v1/query_range") {
      const q = params.get("query") ?? "";
      const limit = Number(params.get("limit") ?? "100");
      const forward = params.get("direction") === "forward";
      const start = BigInt(params.get("start") ?? "0");
      const end = params.has("end") ? BigInt(params.get("end") as string) : 1n << 80n;
      const categorize = req.headers["x-loki-response-encoding-flags"] === "categorize-labels";
      const hits = mine
        .filter((e) => matches(q, e) && BigInt(e.ts) >= start && BigInt(e.ts) < end)
        .sort((a, b) => (BigInt(a.ts) < BigInt(b.ts) ? -1 : BigInt(a.ts) > BigInt(b.ts) ? 1 : 0) * (forward ? 1 : -1))
        .slice(0, limit);
      const streams = new Map<string, { stream: Record<string, string>; values: unknown[] }>();
      for (const e of hits) {
        const key = JSON.stringify(Object.entries(e.labels).sort());
        let s = streams.get(key);
        if (s === undefined) {
          s = { stream: categorize ? e.labels : { ...e.labels, ...e.metadata }, values: [] };
          streams.set(key, s);
        }
        s.values.push(
          categorize ? [e.ts, e.line, { structuredMetadata: e.metadata }] : [e.ts, e.line],
        );
      }
      return {
        status: 200,
        body: {
          status: "success",
          data: {
            resultType: "streams",
            result: [...streams.values()],
            ...(categorize ? { encodingFlags: ["categorize-labels"] } : {}),
            stats: {},
          },
        },
      };
    }
    if (path === "/loki/api/v1/labels") {
      const names = [...new Set(mine.flatMap((e) => Object.keys(e.labels)))].sort();
      return { status: 200, body: names.length === 0 ? { status: "success" } : { status: "success", data: names } };
    }
    if (path === "/loki/api/v1/series") {
      const seen = new Map<string, Record<string, string>>();
      for (const e of mine) seen.set(JSON.stringify(Object.entries(e.labels).sort()), e.labels);
      return { status: 200, body: { status: "success", data: [...seen.values()] } };
    }
    return notFound;
  });
  return Object.assign(server, { entries });
}

/** Minimal protobuf decoder for assertions: field number → list of raw values. */
export function decodeProto(bytes: Uint8Array): Map<number, (bigint | Uint8Array)[]> {
  const out = new Map<number, (bigint | Uint8Array)[]>();
  let pos = 0;
  const varint = (): bigint => {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      const b = bytes[pos++] as number;
      result |= BigInt(b & 0x7f) << shift;
      if ((b & 0x80) === 0) return result;
      shift += 7n;
    }
  };
  while (pos < bytes.length) {
    const tag = Number(varint());
    const field = tag >> 3;
    const wire = tag & 7;
    let value: bigint | Uint8Array;
    if (wire === 0) value = varint();
    else if (wire === 2) {
      const len = Number(varint());
      value = bytes.subarray(pos, pos + len);
      pos += len;
    } else throw new Error(`unsupported wire type ${wire}`);
    const list = out.get(field) ?? [];
    list.push(value);
    out.set(field, list);
  }
  return out;
}
