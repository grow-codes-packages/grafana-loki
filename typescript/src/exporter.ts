// Service-owned: LogExporter implementations (Loki push API and OTLP) for the logging category.

import { LokiError } from "./core/index.js";
import type {
  AttrValue,
  Attributes,
  ExportOptions,
  ExportResult,
  LogExporter,
  LogRecord,
} from "./logging/index.js";
import { recordsToStreams, type CardinalityGuardOptions, type LokiStream } from "./labels.js";
import { dateToNanos } from "./time.js";

/** Wire format of `push`. */
export type PushFormat = "json" | "protobuf";

/** Options of a push call. */
export interface PushOptions {
  /** `json` (default) or `protobuf` (Snappy-compressed `PushRequest`). */
  format?: PushFormat;
  /** Gzip the JSON body (ignored for protobuf, which is Snappy-compressed). Default false. */
  gzip?: boolean;
  signal?: AbortSignal;
}

/** Options of an OTLP push call. */
export interface OtlpPushOptions {
  /** Gzip the body. Default false. */
  gzip?: boolean;
  signal?: AbortSignal;
}

/** What the exporters need from the client. @internal */
export interface LokiPusher {
  push(streams: readonly LokiStream[], options?: PushOptions): Promise<void>;
  pushOtlp(records: readonly LogRecord[], options?: OtlpPushOptions): Promise<void>;
}

async function exportWith(records: LogRecord[], send: () => Promise<void>): Promise<ExportResult> {
  if (records.length === 0) return { accepted: 0, rejected: 0 };
  try {
    await send();
    return { accepted: records.length, rejected: 0 };
  } catch (error) {
    if (error instanceof LokiError) return { accepted: 0, rejected: records.length, error };
    throw error;
  }
}

/** Options of {@link LokiLogExporter}. */
export interface LokiLogExporterOptions extends CardinalityGuardOptions {
  /** Wire format. Default `json`. */
  format?: PushFormat;
}

/**
 * {@link LogExporter} that writes records with `POST /loki/api/v1/push`. Records are grouped into
 * streams by the cardinality guard ({@link recordsToStreams}). Loki accepts or rejects a whole
 * push, so a result is either all accepted or all rejected (with the error).
 */
export class LokiLogExporter implements LogExporter {
  readonly #pusher: LokiPusher;
  readonly #options: LokiLogExporterOptions;

  constructor(pusher: LokiPusher, options: LokiLogExporterOptions = {}) {
    this.#pusher = pusher;
    this.#options = options;
  }

  export(records: LogRecord[], opts: ExportOptions): Promise<ExportResult> {
    return exportWith(records, () =>
      this.#pusher.push(recordsToStreams(records, this.#options), {
        format: this.#options.format ?? "json",
        gzip: opts.gzip,
        ...(opts.signal === undefined ? {} : { signal: opts.signal }),
      }),
    );
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

/**
 * {@link LogExporter} that writes OTLP/JSON to `POST /otlp/v1/logs`. Loki then decides which
 * resource attributes become labels (its OTLP defaults) and stores the rest as structured
 * metadata.
 */
export class LokiOtlpExporter implements LogExporter {
  readonly #pusher: LokiPusher;

  constructor(pusher: LokiPusher) {
    this.#pusher = pusher;
  }

  export(records: LogRecord[], opts: ExportOptions): Promise<ExportResult> {
    return exportWith(records, () =>
      this.#pusher.pushOtlp(records, {
        gzip: opts.gzip,
        ...(opts.signal === undefined ? {} : { signal: opts.signal }),
      }),
    );
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

/** Instrumentation scope name used in OTLP payloads. */
export const OTLP_SCOPE_NAME = "grow-codes-grafana-loki";

/** OTLP/JSON `AnyValue`. */
export type OtlpAnyValue =
  | { stringValue: string }
  | { boolValue: boolean }
  | { intValue: string }
  | { doubleValue: number }
  | { arrayValue: { values: OtlpAnyValue[] } }
  | { kvlistValue: { values: OtlpKeyValue[] } };

/** OTLP/JSON `KeyValue`. */
export interface OtlpKeyValue {
  key: string;
  value: OtlpAnyValue;
}

/** Converts an attribute value to an OTLP/JSON `AnyValue`. */
export function toOtlpValue(value: AttrValue): OtlpAnyValue {
  if (typeof value === "string") return { stringValue: value };
  if (typeof value === "boolean") return { boolValue: value };
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? { intValue: String(value) } : { doubleValue: value };
  }
  if (Array.isArray(value)) return { arrayValue: { values: value.map(toOtlpValue) } };
  return { kvlistValue: { values: toOtlpAttributes(value) } };
}

function toOtlpAttributes(attrs: Attributes): OtlpKeyValue[] {
  return Object.entries(attrs).map(([key, value]) => ({ key, value: toOtlpValue(value) }));
}

/**
 * Encodes records as an OTLP/JSON `ExportLogsServiceRequest` (one `resourceLogs` entry per
 * distinct resource).
 */
export function recordsToOtlpJson(records: readonly LogRecord[]): Record<string, unknown> {
  const byResource = new Map<string, { resource: Attributes; logRecords: unknown[] }>();
  for (const r of records) {
    const key = JSON.stringify(r.resource);
    let group = byResource.get(key);
    if (group === undefined) {
      group = { resource: r.resource, logRecords: [] };
      byResource.set(key, group);
    }
    const lr: Record<string, unknown> = {
      timeUnixNano: dateToNanos(r.timestamp),
      observedTimeUnixNano: dateToNanos(r.observedTimestamp ?? r.timestamp),
      severityNumber: r.severity,
      severityText: r.severityText,
      body: { stringValue: r.body },
      attributes: toOtlpAttributes(r.attributes),
    };
    if (r.traceId !== undefined) lr.traceId = r.traceId;
    if (r.spanId !== undefined) lr.spanId = r.spanId;
    if (r.traceFlags !== undefined) lr.flags = r.traceFlags;
    group.logRecords.push(lr);
  }
  return {
    resourceLogs: [...byResource.values()].map((g) => ({
      resource: { attributes: toOtlpAttributes(g.resource) },
      scopeLogs: [{ scope: { name: OTLP_SCOPE_NAME }, logRecords: g.logRecords }],
    })),
  };
}
