// Service-owned: label validation, the cardinality guard and LogRecord → Loki stream mapping.

import { ValidationError } from "./core/index.js";
import { severityText, type AttrValue, type LogRecord } from "./logging/index.js";
import { dateToNanos } from "./time.js";

/** Label names allowed by Loki (Prometheus rules). */
export const LABEL_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * Keys the cardinality guard lets become index labels by default (coordinator decision 3).
 * Every other attribute or resource key goes to structured metadata.
 */
export const DEFAULT_ALLOWED_LABELS: readonly string[] = Object.freeze([
  "service_name",
  "service_namespace",
  "deployment_environment",
  "host_name",
  "level",
  "job",
  "instance",
  "namespace",
  "pod",
  "container",
]);

/** Metadata keys the SDK writes itself (and reads back into `LogRecord` fields). */
export const RESERVED_METADATA_KEYS: readonly string[] = Object.freeze([
  "severity_text",
  "severity_number",
  "trace_id",
  "span_id",
  "trace_flags",
]);

/** True when `name` is a valid, non-reserved Loki label name. */
export function isValidLabelName(name: string): boolean {
  return LABEL_NAME_PATTERN.test(name) && !name.startsWith("__");
}

/** Throws `ValidationError` unless `name` is a valid, non-reserved label name. */
export function validateLabelName(name: string, what = "label name"): void {
  if (!LABEL_NAME_PATTERN.test(name)) {
    throw new ValidationError(
      `invalid ${what} ${JSON.stringify(name)}: must match [a-zA-Z_][a-zA-Z0-9_]*`,
    );
  }
  if (name.startsWith("__")) {
    throw new ValidationError(`invalid ${what} ${JSON.stringify(name)}: "__" names are reserved`);
  }
}

/** Validates a label set: valid names and non-empty values. */
export function validateLabels(labels: Readonly<Record<string, string>>): void {
  for (const [name, value] of Object.entries(labels)) {
    validateLabelName(name);
    if (typeof value !== "string" || value === "") {
      throw new ValidationError(`label ${name}: value must be a non-empty string`);
    }
  }
}

/**
 * Normalizes an attribute key to a label/metadata name the way Loki does for OTLP attributes:
 * every character outside `[a-zA-Z0-9_]` becomes `_` (`service.name` → `service_name`), and a
 * leading digit gets a `_` prefix.
 */
export function normalizeLabelName(key: string): string {
  const replaced = key.replace(/[^a-zA-Z0-9_]/g, "_");
  if (replaced === "") return "_";
  return /^[0-9]/.test(replaced) ? `_${replaced}` : replaced;
}

/**
 * Renders a label set in Prometheus syntax, `{a="b", c="d"}`, with sorted names and
 * Go-compatible quoting (the form protobuf push and `listStreams()` use).
 */
export function formatLabelSet(labels: Readonly<Record<string, string>>): string {
  const parts = Object.keys(labels)
    .sort()
    .map((k) => `${k}=${JSON.stringify(labels[k])}`);
  return `{${parts.join(", ")}}`;
}

/** Stringifies an attribute value for structured metadata (objects and arrays as JSON). */
export function attrToString(value: AttrValue): string {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

/** One log line pushed to Loki. */
export interface LokiEntry {
  /** Entry time: `Date`, bigint nanoseconds or a decimal nanosecond string (unset = now). */
  timestamp?: Date | bigint | string;
  /** The log line. */
  line: string;
  /** Structured metadata (string → string), stored next to the line but not indexed. */
  structuredMetadata?: Readonly<Record<string, string>>;
}

/** A stream: one label set and its entries (`push` input). */
export interface LokiStream {
  /** Index labels. Keep them few and low-cardinality. */
  labels: Readonly<Record<string, string>>;
  /** Entries of the stream (any order inside the ingest window). */
  entries: readonly LokiEntry[];
}

/**
 * Keeps Loki's index small: only allowlisted label names become index labels, every other
 * stream label is moved to the structured metadata of each entry (an entry's own metadata wins).
 * `push` applies it unless `LokiConfig.disableLabelGuard` is set.
 */
export class CardinalityGuard {
  readonly #allowed: ReadonlySet<string>;

  /** A guard for `allowed` (default {@link DEFAULT_ALLOWED_LABELS}). */
  constructor(allowed: readonly string[] = DEFAULT_ALLOWED_LABELS) {
    this.#allowed = new Set(allowed);
  }

  /** The allowlist, sorted. */
  get allowed(): string[] {
    return [...this.#allowed].sort();
  }

  /** Whether `name` may be an index label. */
  isAllowed(name: string): boolean {
    return this.#allowed.has(name);
  }

  /** A copy of `stream` whose labels outside the allowlist are moved to every entry's metadata. */
  apply(stream: LokiStream): LokiStream {
    const labels: Record<string, string> = {};
    const moved: Record<string, string> = {};
    for (const [k, v] of Object.entries(stream.labels)) {
      if (this.#allowed.has(k)) labels[k] = v;
      else moved[k] = v;
    }
    if (Object.keys(moved).length === 0) return { labels, entries: stream.entries };
    return {
      labels,
      entries: stream.entries.map((e) => ({
        ...e,
        structuredMetadata: { ...moved, ...(e.structuredMetadata ?? {}) },
      })),
    };
  }
}

/** Options of the cardinality guard ({@link recordsToStreams}). */
export interface CardinalityGuardOptions {
  /** Normalized keys allowed as index labels. Default {@link DEFAULT_ALLOWED_LABELS}. */
  allowedLabels?: readonly string[];
}

function isScalar(value: AttrValue): value is string | number | boolean {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

/**
 * Maps log records to Loki streams with the cardinality guard: resource and attribute keys are
 * normalized (`service.name` → `service_name`); keys in `allowedLabels` with scalar, non-empty
 * values become index labels, everything else becomes structured metadata. `level` (the
 * lower-case severity text) is added as a label when allowed. Severity, trace id, span id and
 * trace flags travel as metadata (`severity_text`, `severity_number`, `trace_id`, `span_id`,
 * `trace_flags`) and are never labels. Records with the same final label set share a stream.
 */
export function recordsToStreams(
  records: readonly LogRecord[],
  options: CardinalityGuardOptions = {},
): LokiStream[] {
  const allowed = new Set(options.allowedLabels ?? DEFAULT_ALLOWED_LABELS);
  const streams = new Map<string, { labels: Record<string, string>; entries: LokiEntry[] }>();
  for (const record of records) {
    const labels: Record<string, string> = {};
    const metadata: Record<string, string> = {};
    const place = (key: string, value: AttrValue): void => {
      const name = normalizeLabelName(key);
      if (allowed.has(name) && isValidLabelName(name) && isScalar(value) && String(value) !== "") {
        labels[name] = String(value);
      } else {
        metadata[name] = attrToString(value);
      }
    };
    for (const [k, v] of Object.entries(record.resource)) place(k, v);
    for (const [k, v] of Object.entries(record.attributes)) place(k, v);
    const text = record.severityText !== "" ? record.severityText : severityText(record.severity);
    if (allowed.has("level") && labels.level === undefined) labels.level = text.toLowerCase();
    metadata.severity_text = text;
    metadata.severity_number = String(record.severity);
    if (record.traceId !== undefined) {
      metadata.trace_id = record.traceId;
      if (record.spanId !== undefined) metadata.span_id = record.spanId;
      if (record.traceFlags !== undefined) metadata.trace_flags = String(record.traceFlags);
    }
    const key = formatLabelSet(labels);
    let stream = streams.get(key);
    if (stream === undefined) {
      stream = { labels, entries: [] };
      streams.set(key, stream);
    }
    stream.entries.push({
      timestamp: dateToNanos(record.timestamp),
      line: record.body,
      structuredMetadata: metadata,
    });
  }
  return [...streams.values()];
}
