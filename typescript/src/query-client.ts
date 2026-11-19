// Service-owned: LogQueryClient (logging category) on top of the Loki query API.

import { ValidationError } from "./core/index.js";
import {
  BaseLogQueryClient,
  Severity,
  parseSeverity,
  severityText,
  type Attributes,
  type LogRecord,
  type LogSearchOptions,
  type LogSearchResult,
  type TailHandle,
  type TailOptions,
} from "./logging/index.js";
import { formatLabelSet } from "./labels.js";
import { quoteLogQL } from "./logql.js";
import type {
  LokiEntryResult,
  LokiQueryResult,
  LokiStreamResult,
  LokiTailFrame,
} from "./results.js";
import type { LokiTail, TailCallbacks } from "./tail.js";
import { nanosToDate, type LokiTime } from "./time.js";

/** Largest `limit` Loki accepts per query by default (`max_entries_limit_per_query`). */
export const MAX_QUERY_LIMIT = 5000;

/** Selector used for free-text searches and `listStreams()`. */
export const ALL_STREAMS_SELECTOR = '{service_name=~".+"}';

/** Stream labels mapped back to OpenTelemetry resource keys. */
const RESOURCE_KEYS: Readonly<Record<string, string>> = {
  service_name: "service.name",
  service_namespace: "service.namespace",
  service_version: "service.version",
  service_instance_id: "service.instance.id",
  deployment_environment: "deployment.environment",
  host_name: "host.name",
};

/** What the query client needs from {@link LokiClient}. @internal */
export interface LokiQuerySource {
  queryRange(
    logql: string,
    options?: {
      start?: LokiTime;
      end?: LokiTime;
      limit?: number;
      direction?: "forward" | "backward";
      signal?: AbortSignal;
    },
  ): Promise<LokiQueryResult>;
  labels(options?: { signal?: AbortSignal }): Promise<string[]>;
  series(matchers: string | readonly string[]): Promise<Record<string, string>[]>;
  tail(
    logql: string,
    onEntries: (streams: LokiStreamResult[], frame: LokiTailFrame) => void,
    options?: TailCallbacks & { start?: LokiTime },
  ): Promise<LokiTail>;
}

/**
 * Turns a search text into LogQL: text starting with `{` is used as LogQL as is; anything else
 * is a case-sensitive substring search over every stream (`{service_name=~".+"} |= "<text>"`).
 */
export function toSearchLogQL(query: string): string {
  const trimmed = query.trim();
  if (trimmed.startsWith("{")) return trimmed;
  if (trimmed === "") return ALL_STREAMS_SELECTOR;
  return `${ALL_STREAMS_SELECTOR} |= ${quoteLogQL(trimmed)}`;
}

/** Encodes a search cursor (`<ns>:<entries at ns already returned>`, base64). */
export function encodeCursor(ns: string, skip: number): string {
  return Buffer.from(`${ns}:${skip}`, "utf8").toString("base64url");
}

/** Decodes a search cursor; throws `ValidationError` when it is malformed. */
export function decodeCursor(cursor: string): { ns: string; skip: number } {
  const text = Buffer.from(cursor, "base64url").toString("utf8");
  const match = /^(\d+):(\d+)$/.exec(text);
  if (match === null) throw new ValidationError("logging.search: invalid cursor");
  return { ns: match[1] as string, skip: Number(match[2]) };
}

const CONSUMED_METADATA = new Set([
  "severity_text",
  "severity_number",
  "trace_id",
  "span_id",
  "trace_flags",
  "observed_timestamp",
]);

function severityFrom(
  md: Record<string, string>,
  labels: Record<string, string>,
): { severity: Severity; text: string } {
  const fromNumber = md.severity_number === undefined ? null : parseSeverity(md.severity_number);
  const fromText = md.severity_text === undefined ? null : parseSeverity(md.severity_text);
  const severity =
    fromNumber ??
    fromText ??
    (md.detected_level === undefined ? null : parseSeverity(md.detected_level)) ??
    (labels.level === undefined ? null : parseSeverity(labels.level)) ??
    Severity.Info;
  return { severity, text: md.severity_text ?? severityText(severity) };
}

/**
 * Converts one Loki entry to a normalized {@link LogRecord}: the line becomes `body`; structured
 * metadata and parsed labels become `attributes` (the SDK's own keys are mapped back to
 * `severity`, `traceId`, `spanId`, `traceFlags`, `observedTimestamp`); stream labels become
 * `resource` (`service_name` → `service.name`, `deployment_environment` →
 * `deployment.environment`, …).
 */
export function entryToLogRecord(
  labels: Record<string, string>,
  entry: LokiEntryResult,
): LogRecord {
  const md = entry.structuredMetadata;
  const attributes: Attributes = {};
  for (const [k, v] of Object.entries(md)) if (!CONSUMED_METADATA.has(k)) attributes[k] = v;
  for (const [k, v] of Object.entries(entry.parsed)) attributes[k] = v;
  const resource: Attributes = {};
  for (const [k, v] of Object.entries(labels)) resource[RESOURCE_KEYS[k] ?? k] = v;
  const { severity, text } = severityFrom(md, labels);
  const record: LogRecord = {
    timestamp: entry.timestamp,
    severity,
    severityText: text,
    body: entry.line,
    attributes,
    resource,
  };
  if (md.observed_timestamp !== undefined && /^\d+$/.test(md.observed_timestamp)) {
    record.observedTimestamp = nanosToDate(md.observed_timestamp);
  }
  const traceId = md.trace_id?.toLowerCase();
  if (traceId !== undefined && /^[0-9a-f]{32}$/.test(traceId)) {
    record.traceId = traceId;
    const spanId = md.span_id?.toLowerCase();
    if (spanId !== undefined && /^[0-9a-f]{16}$/.test(spanId)) record.spanId = spanId;
    const flagsText = md.trace_flags ?? md.flags;
    if (flagsText !== undefined && /^\d+$/.test(flagsText)) {
      record.traceFlags = Number(flagsText);
      if (md.trace_flags === undefined) delete attributes.flags;
    }
  }
  return record;
}

/** One entry of the merged, sorted search result. */
export interface MergedEntry {
  labels: Record<string, string>;
  labelKey: string;
  entry: LokiEntryResult;
}

function compareNs(a: string, b: string): number {
  if (a.length !== b.length) return a.length - b.length;
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Merge-sorts the entries of all streams by time (then labels, then line for stable ties). */
export function mergeStreams(
  streams: readonly LokiStreamResult[],
  order: "asc" | "desc",
): MergedEntry[] {
  const flat: MergedEntry[] = [];
  for (const s of streams) {
    const labelKey = formatLabelSet(s.labels);
    for (const entry of s.entries) flat.push({ labels: s.labels, labelKey, entry });
  }
  const dir = order === "asc" ? 1 : -1;
  flat.sort((a, b) => {
    const t = compareNs(a.entry.timestampNs, b.entry.timestampNs);
    if (t !== 0) return t * dir;
    if (a.labelKey !== b.labelKey) return a.labelKey < b.labelKey ? -1 : 1;
    return a.entry.line < b.entry.line ? -1 : a.entry.line > b.entry.line ? 1 : 0;
  });
  return flat;
}

/**
 * {@link LogQueryClient} for Loki: `search` (query_range, merge-sorted, cursor pagination),
 * `tail` (WebSocket), `listStreams` (series), `listLabels` (labels) and `waitForLog`.
 */
export class LokiLogQueryClient extends BaseLogQueryClient {
  readonly #source: LokiQuerySource;

  constructor(source: LokiQuerySource) {
    super();
    this.#source = source;
  }

  /**
   * Searches with `query_range`. `query` is LogQL (starting with `{`) or free text. `order`
   * `desc` (default) is newest first. Pass `nextCursor` back as `cursor` for the next page
   * (the cursor is `<timestamp>:<entries at that timestamp already returned>`, so pages never
   * skip or repeat entries sharing a nanosecond). `total` is not available from Loki.
   */
  async search(query: string, opts: LogSearchOptions = {}): Promise<LogSearchResult> {
    const limit = opts.limit ?? 100;
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_QUERY_LIMIT) {
      throw new ValidationError(`logging.search: limit must be an integer 1..${MAX_QUERY_LIMIT}`);
    }
    const order = opts.order ?? "desc";
    let start: LokiTime | undefined = opts.start;
    let end: LokiTime | undefined = opts.end;
    let cursor: { ns: string; skip: number } | undefined;
    if (opts.cursor !== undefined) {
      cursor = decodeCursor(opts.cursor);
      if (order === "desc") end = BigInt(cursor.ns) + 1n;
      else start = BigInt(cursor.ns);
    }
    const fetch = Math.min(limit + (cursor?.skip ?? 0), MAX_QUERY_LIMIT);
    const logql = toSearchLogQL(query);
    const direction = order === "asc" ? "forward" : "backward";
    const signal = opts.signal === undefined ? {} : { signal: opts.signal };
    const result = await this.#source.queryRange(logql, {
      ...(start === undefined ? {} : { start }),
      ...(end === undefined ? {} : { end }),
      limit: fetch,
      direction,
      ...signal,
    });
    let merged = mergeStreams(result.streams, order);
    const truncated = merged.length >= fetch;
    const boundary = merged[merged.length - 1]?.entry.timestampNs;
    if (truncated && boundary !== undefined) {
      // Loki cut the result inside the boundary nanosecond in its own order: fetch that
      // nanosecond completely so the page (and the cursor's skip count) are deterministic.
      const exact = await this.#source.queryRange(logql, {
        start: BigInt(boundary),
        end: BigInt(boundary) + 1n,
        limit: MAX_QUERY_LIMIT,
        direction,
        ...signal,
      });
      merged = mergeStreams(
        [
          ...result.streams.map((s) => ({
            labels: s.labels,
            entries: s.entries.filter((e) => e.timestampNs !== boundary),
          })),
          ...exact.streams,
        ],
        order,
      );
    }
    let offset = 0;
    if (cursor !== undefined) {
      while (
        offset < cursor.skip &&
        offset < merged.length &&
        merged[offset]?.entry.timestampNs === cursor.ns
      ) {
        offset++;
      }
    }
    const remaining = merged.slice(offset);
    const page = remaining.slice(0, limit);
    const out: LogSearchResult = {
      records: page.map((f) => entryToLogRecord(f.labels, f.entry)),
    };
    const more = truncated || remaining.length > limit;
    const last = page[page.length - 1];
    if (more && last !== undefined) {
      const lastNs = last.entry.timestampNs;
      let atLast = page.filter((f) => f.entry.timestampNs === lastNs).length;
      if (cursor !== undefined && cursor.ns === lastNs) atLast += cursor.skip;
      out.nextCursor = encodeCursor(lastNs, atLast);
    }
    return out;
  }

  /**
   * Live tail over the WebSocket API (`start` defaults to now, so only new records arrive).
   * Connection errors and abnormal closes go to `opts.onError`.
   */
  override tail(
    query: string,
    onRecord: (record: LogRecord) => void,
    opts: TailOptions = {},
  ): TailHandle {
    let stopped = false;
    let tail: LokiTail | undefined;
    const report = (error: unknown): void => {
      if (!stopped) opts.onError?.(error);
    };
    this.#source
      .tail(
        toSearchLogQL(query),
        (streams) => {
          for (const f of mergeStreams(streams, "asc")) {
            if (stopped) return;
            onRecord(entryToLogRecord(f.labels, f.entry));
          }
        },
        { start: opts.start ?? new Date(), onError: report },
      )
      .then((t) => {
        tail = t;
        if (stopped) t.stop();
      }, report);
    return {
      stop(): void {
        stopped = true;
        tail?.stop();
      },
    };
  }

  /** Every stream of the default window as a label-set string (`{a="b", c="d"}`), sorted. */
  override async listStreams(): Promise<string[]> {
    const series = await this.#source.series([ALL_STREAMS_SELECTOR]);
    return [...new Set(series.map(formatLabelSet))].sort();
  }

  /** Label names of the default window. */
  override listLabels(): Promise<string[]> {
    return this.#source.labels();
  }
}
