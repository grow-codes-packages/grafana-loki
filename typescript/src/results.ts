// Service-owned: typed results of the Loki query API and their decoders.

import { UnavailableError } from "./core/index.js";
import { nanosToDate, secondsToDate } from "./time.js";

/** One log entry returned by a query or the tail stream. */
export interface LokiEntryResult {
  /** Unix nanoseconds as a decimal string (lossless). */
  timestampNs: string;
  /** The same time as a `Date` (millisecond precision). */
  timestamp: Date;
  /** The log line. */
  line: string;
  /** Structured metadata of the entry (includes Loki's `detected_level`). */
  structuredMetadata: Record<string, string>;
  /** Labels extracted by parser stages (`| json`, `| logfmt`, …). */
  parsed: Record<string, string>;
}

/** One stream of a `streams` result. */
export interface LokiStreamResult {
  /** Index labels of the stream. */
  labels: Record<string, string>;
  /** Entries in the requested direction. */
  entries: LokiEntryResult[];
}

/** One metric sample. */
export interface LokiSample {
  timestamp: Date;
  /** Sample value (`NaN`/`±Infinity` kept). */
  value: number;
}

/** One series of a `vector` result. */
export interface LokiVectorSample extends LokiSample {
  metric: Record<string, string>;
}

/** One series of a `matrix` result. */
export interface LokiMatrixSeries {
  metric: Record<string, string>;
  samples: LokiSample[];
}

/** Result type reported by Loki. */
export type LokiResultType = "streams" | "vector" | "matrix" | "scalar";

/** Result of `query`, `queryRange`, `volume` and `volumeRange`. */
export interface LokiQueryResult {
  resultType: LokiResultType;
  /** Filled for `streams` results. */
  streams: LokiStreamResult[];
  /** Filled for `vector` results. */
  vector: LokiVectorSample[];
  /** Filled for `matrix` results. */
  matrix: LokiMatrixSeries[];
  /** Filled for `scalar` results. */
  scalar?: LokiSample;
  /** Raw query statistics (`data.stats`), `{}` when absent. */
  stats: Record<string, unknown>;
}

/** Result of `indexStats` (approximate, from the TSDB index only). */
export interface LokiIndexStats {
  streams: number;
  chunks: number;
  bytes: number;
  entries: number;
}

/** Build information of the server (`/loki/api/v1/status/buildinfo`). */
export interface LokiBuildInfo {
  version: string;
  revision: string;
  branch: string;
  buildUser: string;
  buildDate: string;
  goVersion: string;
}

/** Labels of one dropped entry reported by the tail stream. */
export interface LokiDroppedEntry {
  labels: Record<string, string>;
  timestampNs: string;
}

/** One frame of the tail WebSocket. */
export interface LokiTailFrame {
  streams: LokiStreamResult[];
  droppedEntries: LokiDroppedEntry[];
}

type Json = Record<string, unknown>;

function isObject(v: unknown): v is Json {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function bad(what: string): never {
  throw new UnavailableError(`unexpected response from Loki: ${what}`, { retryable: false });
}

/** Converts a JSON object of string values to a string map (other values are stringified). */
export function stringMap(v: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  if (!isObject(v)) return out;
  for (const [k, val] of Object.entries(v)) {
    if (val === null || val === undefined) continue;
    out[k] = typeof val === "string" ? val : JSON.stringify(val);
  }
  return out;
}

function parseValue(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v !== "string") return Number.NaN;
  if (v === "+Inf" || v === "Inf") return Number.POSITIVE_INFINITY;
  if (v === "-Inf") return Number.NEGATIVE_INFINITY;
  return Number(v);
}

function decodeSample(v: unknown): LokiSample {
  if (!Array.isArray(v) || v.length < 2 || typeof v[0] !== "number") bad("malformed sample");
  return { timestamp: secondsToDate(v[0]), value: parseValue(v[1]) };
}

/** Decodes one `[ns, line, {structuredMetadata, parsed}?]` value. */
export function decodeEntry(v: unknown): LokiEntryResult {
  if (!Array.isArray(v) || typeof v[0] !== "string" || typeof v[1] !== "string") {
    bad("malformed stream value");
  }
  const ns = v[0];
  if (!/^\d+$/.test(ns)) bad("malformed timestamp");
  const extra: unknown = v[2];
  return {
    timestampNs: ns,
    timestamp: nanosToDate(ns),
    line: v[1],
    structuredMetadata: isObject(extra) ? stringMap(extra.structuredMetadata) : {},
    parsed: isObject(extra) ? stringMap(extra.parsed) : {},
  };
}

/** Decodes a list of `{stream, values}` objects. */
export function decodeStreams(v: unknown): LokiStreamResult[] {
  if (v === undefined || v === null) return [];
  if (!Array.isArray(v)) bad("streams is not a list");
  return v.map((s: unknown) => {
    if (!isObject(s)) bad("malformed stream");
    const values = Array.isArray(s.values) ? (s.values as unknown[]) : [];
    return { labels: stringMap(s.stream), entries: values.map(decodeEntry) };
  });
}

/** Decodes the `{status, data: {resultType, result, stats}}` envelope. */
export function decodeQueryResult(body: unknown): LokiQueryResult {
  if (!isObject(body) || !isObject(body.data)) bad("missing data");
  const data = body.data;
  const resultType = data.resultType;
  const out: LokiQueryResult = {
    resultType: "streams",
    streams: [],
    vector: [],
    matrix: [],
    stats: isObject(data.stats) ? data.stats : {},
  };
  const result: unknown = data.result;
  switch (resultType) {
    case "streams":
      out.streams = decodeStreams(result);
      break;
    case "vector":
      out.resultType = "vector";
      out.vector = (Array.isArray(result) ? (result as unknown[]) : []).map((r) => {
        if (!isObject(r)) bad("malformed vector sample");
        return { metric: stringMap(r.metric), ...decodeSample(r.value) };
      });
      break;
    case "matrix":
      out.resultType = "matrix";
      out.matrix = (Array.isArray(result) ? (result as unknown[]) : []).map((r) => {
        if (!isObject(r)) bad("malformed matrix series");
        const values = Array.isArray(r.values) ? (r.values as unknown[]) : [];
        return { metric: stringMap(r.metric), samples: values.map(decodeSample) };
      });
      break;
    case "scalar":
      out.resultType = "scalar";
      out.scalar = decodeSample(result);
      break;
    default:
      bad(`unknown resultType ${String(resultType)}`);
  }
  return out;
}

/** Decodes `{status, data: [...]}`; a missing `data` (empty result) is `[]`. */
export function decodeStringList(body: unknown): string[] {
  if (!isObject(body)) bad("missing body");
  const data = body.data;
  if (data === undefined || data === null) return [];
  if (!Array.isArray(data)) bad("data is not a list");
  return data.filter((x): x is string => typeof x === "string");
}

/** Decodes the series response (list of label maps). */
export function decodeSeries(body: unknown): Record<string, string>[] {
  if (!isObject(body)) bad("missing body");
  const data = body.data;
  if (data === undefined || data === null) return [];
  if (!Array.isArray(data)) bad("data is not a list");
  return data.map(stringMap);
}

/** Decodes the index stats response (no envelope). */
export function decodeIndexStats(body: unknown): LokiIndexStats {
  if (!isObject(body)) bad("missing body");
  const num = (k: string): number => (typeof body[k] === "number" ? body[k] : 0);
  return {
    streams: num("streams"),
    chunks: num("chunks"),
    bytes: num("bytes"),
    entries: num("entries"),
  };
}

/** Decodes the build info response. */
export function decodeBuildInfo(body: unknown): LokiBuildInfo {
  if (!isObject(body)) bad("missing body");
  const str = (k: string): string => (typeof body[k] === "string" ? body[k] : "");
  return {
    version: str("version"),
    revision: str("revision"),
    branch: str("branch"),
    buildUser: str("buildUser"),
    buildDate: str("buildDate"),
    goVersion: str("goVersion"),
  };
}

/** Decodes one tail WebSocket frame. */
export function decodeTailFrame(body: unknown): LokiTailFrame {
  if (!isObject(body)) bad("malformed tail frame");
  const dropped = Array.isArray(body.dropped_entries) ? (body.dropped_entries as unknown[]) : [];
  return {
    streams: decodeStreams(body.streams),
    droppedEntries: dropped.filter(isObject).map((d) => ({
      labels: stringMap(d.labels),
      timestampNs: typeof d.timestamp === "string" ? d.timestamp : String(d.timestamp),
    })),
  };
}
