// Service-owned: the Grafana Loki client.

import { gzipSync } from "node:zlib";
import {
  ConfigError,
  type HealthStatus,
  HttpTransport,
  LokiError,
  type RequestOptions,
  type TransportOptions,
  type TransportResponse,
  UnavailableError,
  UnsupportedError,
  ValidationError,
  healthStatus,
  probeHealth,
} from "./core/index.js";
import { LokiConfig, type LokiConfigOptions, tenantIdProblem } from "./config.js";
import {
  LokiLogExporter,
  LokiOtlpExporter,
  recordsToOtlpJson,
  type LokiPusher,
  type OtlpPushOptions,
  type PushOptions,
} from "./exporter.js";
import {
  type CardinalityGuardOptions,
  type LokiStream,
  validateLabelName,
  validateLabels,
} from "./labels.js";
import {
  BatchingLogClient,
  type BatchingLogClientOptions,
  type LogRecord,
} from "./logging/index.js";
import { decodeRpcStatusMessage, encodePushRequest, snappyCompress } from "./protobuf.js";
import { LokiLogQueryClient, MAX_QUERY_LIMIT } from "./query-client.js";
import {
  decodeBuildInfo,
  decodeIndexStats,
  decodeQueryResult,
  decodeSeries,
  decodeStringList,
  type LokiBuildInfo,
  type LokiIndexStats,
  type LokiQueryResult,
  type LokiStreamResult,
  type LokiTailFrame,
} from "./results.js";
import { openTail, type LokiTail, type TailCallbacks } from "./tail.js";
import { msToDuration, optionalNanos, toNanos, type LokiTime } from "./time.js";

/** Per-call options. */
export interface CallOptions {
  /** Cancels the call (`CancelledError`). */
  signal?: AbortSignal;
}

/** Test seams (undici `MockAgent`, fake sleep). Not part of the stable API. */
export type ClientInternals = Pick<TransportOptions, "dispatcher" | "sleep" | "now">;

/** Sort order of log results. */
export type Direction = "forward" | "backward";

/** Options of {@link LokiClient.query}. */
export interface QueryOptions extends CallOptions {
  /** Evaluation time. Default: now. */
  time?: LokiTime;
  /** Kept for API parity; instant queries only return metrics. */
  limit?: number;
  /** Kept for API parity. */
  direction?: Direction;
}

/** Options of {@link LokiClient.queryRange}. */
export interface QueryRangeOptions extends CallOptions {
  /** Inclusive start. Default: one hour before `end`. */
  start?: LokiTime;
  /** Exclusive end. Default: now. */
  end?: LokiTime;
  /** Metric query resolution in milliseconds. */
  stepMs?: number;
  /** Stream sampling interval in milliseconds (log queries). */
  intervalMs?: number;
  /** Maximum entries across all streams (default 100, server max 5000). */
  limit?: number;
  /** `backward` (default, newest first) or `forward`. */
  direction?: Direction;
}

/** Time window options. */
export interface TimeRangeOptions extends CallOptions {
  start?: LokiTime;
  end?: LokiTime;
}

/** Options of {@link LokiClient.labels} and {@link LokiClient.labelValues}. */
export interface LabelsOptions extends TimeRangeOptions {
  /** Stream selector that scopes the result. */
  query?: string;
}

/** Options of {@link LokiClient.volume} and {@link LokiClient.volumeRange}. */
export interface VolumeOptions extends TimeRangeOptions {
  /** Maximum series. */
  limit?: number;
  /** Labels to aggregate on. */
  targetLabels?: readonly string[];
  /** `series` (default) or `labels`. */
  aggregateBy?: "series" | "labels";
}

/** Options of {@link LokiClient.volumeRange}. */
export interface VolumeRangeOptions extends VolumeOptions {
  /** Resolution in milliseconds. */
  stepMs?: number;
}

/** Options of {@link LokiClient.tail}. */
export interface TailStreamOptions extends TailCallbacks, CallOptions {
  /** Replay entries from this time. Default: Loki replays the last hour (up to `limit`). */
  start?: LokiTime;
  /** Maximum replayed entries (Loki default 100). */
  limit?: number;
  /** Server-side delay in milliseconds, 0–5000 (sent as whole seconds). */
  delayForMs?: number;
}

/** Options of {@link LokiClient.logClient}. */
export interface LokiLogClientOptions extends BatchingLogClientOptions, CardinalityGuardOptions {
  /** `json` (default) or `protobuf` push, or `otlp` (`/otlp/v1/logs`). */
  protocol?: "json" | "protobuf" | "otlp";
}

/** Header that switches query responses to categorized labels. */
export const ENCODING_FLAGS_HEADER = "x-loki-response-encoding-flags";
/** Tenant header. */
export const TENANT_HEADER = "x-scope-orgid";

const SHUTTING_DOWN = /ingester is shutting down/i;

/** Maps Loki-specific error cases onto the SPEC classes (decision 6 + API.md §0). */
function mapLokiError(err: unknown): unknown {
  if (!(err instanceof LokiError) || err.httpStatus !== 500) return err;
  const details = {
    httpStatus: 500,
    ...(err.requestId === undefined ? {} : { requestId: err.requestId }),
    ...(err.cause === undefined ? {} : { cause: err.cause }),
  };
  // WAL disk-full throttling is transient; any other 500 from Loki is not.
  return new UnavailableError(err.message, {
    ...details,
    retryable: SHUTTING_DOWN.test(err.message),
  });
}

/** Turns a `vector` result (single point per series) into the equivalent `matrix`. */
function vectorToMatrix(result: LokiQueryResult): LokiQueryResult {
  if (result.resultType !== "vector") return result;
  return {
    ...result,
    resultType: "matrix",
    matrix: result.vector.map(({ metric, timestamp, value }) => ({
      metric,
      samples: [{ timestamp, value }],
    })),
    vector: [],
  };
}

/** Replaces a protobuf `google.rpc.Status` body in an OTLP error message by its text. */
function cleanRpcStatus(err: unknown): unknown {
  if (!(err instanceof LokiError)) return err;
  const match = /^(.*?: )([\s\S]*)( \(HTTP \d+\))$/.exec(err.message);
  if (match === null) return err;
  const raw = match[2] as string;
  const decoded = decodeRpcStatusMessage(new TextEncoder().encode(raw)) ?? raw;
  // eslint-disable-next-line no-control-regex
  const text = decoded.replace(/[\u0000-\u001f\u007f]/g, "").trim();
  err.message = `${match[1] as string}${text}${match[3] as string}`;
  return err;
}

function checkLimit(limit: number | undefined, op: string, max = Number.MAX_SAFE_INTEGER): void {
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1 || limit > max)) {
    throw new ValidationError(`${op}: limit must be an integer 1..${max}`);
  }
}

function checkQuery(logql: string, op: string): void {
  if (typeof logql !== "string" || logql.trim() === "") {
    throw new ValidationError(`${op}: query must not be empty`);
  }
}

/** Shared state between a client and its {@link LokiClient.withTenant} children. @internal */
export interface LokiClientShared {
  transport: HttpTransport;
}

/**
 * Client for Grafana Loki: push (JSON, protobuf + Snappy, OTLP), LogQL queries, labels,
 * series, index stats and volume, WebSocket tail, status, multi-tenancy and the logging
 * category (`logClient()`, `logQueryClient()`).
 */
export class LokiClient implements LokiPusher {
  /** The immutable configuration. */
  readonly config: LokiConfig;
  readonly #transport: HttpTransport;
  readonly #ownsTransport: boolean;
  readonly #tails = new Set<LokiTail>();
  readonly #logClients = new Set<BatchingLogClient>();
  #closed = false;
  #closing: Promise<void> | undefined;

  /** Accepts a {@link LokiConfig} or plain options. */
  constructor(
    config: LokiConfigOptions = {},
    internals: ClientInternals = {},
    shared?: LokiClientShared,
  ) {
    const cfg = LokiConfig.from(config);
    this.config = cfg;
    if (shared !== undefined) {
      this.#transport = shared.transport;
      this.#ownsTransport = false;
    } else {
      this.#transport = new HttpTransport({
        ...internals,
        config: cfg,
        defaultHeaders: (): Record<string, string> => {
          const auth = cfg.authorizationHeader();
          return auth === undefined ? {} : { authorization: auth };
        },
        knownSecrets: () => cfg.knownSecrets(),
      });
      this.#ownsTransport = true;
    }
  }

  /** Builds a client from `GROW_GRAFANA_LOKI_*` (and `LOKI_*`) environment variables. */
  static fromEnv(overrides: LokiConfigOptions = {}): LokiClient {
    return new LokiClient(LokiConfig.fromEnv(overrides));
  }

  /** The shared HTTP transport (for service modules). @internal */
  get transport(): HttpTransport {
    return this.#transport;
  }

  /** The tenant this client sends as `X-Scope-OrgID`, if any. */
  get tenantId(): string | undefined {
    return this.config.tenantId;
  }

  /**
   * A client for another tenant: shares this client's connections, sends `X-Scope-OrgID: id`
   * on every request and on the tail handshake. This client is unchanged. Closing the child
   * does not close the shared connections. Throws `ValidationError` for an invalid id.
   */
  withTenant(id: string): LokiClient {
    const problem = tenantIdProblem(id);
    if (problem !== undefined) throw new ValidationError(`withTenant: ${problem}`);
    return new LokiClient(this.config.with({ tenantId: id }), {}, { transport: this.#transport });
  }

  #tenantHeaders(): Record<string, string> {
    return this.config.tenantId === undefined ? {} : { [TENANT_HEADER]: this.config.tenantId };
  }

  async #request<T>(options: RequestOptions): Promise<TransportResponse<T>> {
    if (this.#closed) {
      throw new ConfigError(`${options.operation ?? options.path}: client is closed`);
    }
    try {
      return await this.#transport.request<T>({
        ...options,
        headers: { ...this.#tenantHeaders(), ...options.headers },
      });
    } catch (err) {
      throw mapLokiError(err);
    }
  }

  async #getJson(
    path: string,
    operation: string,
    query: RequestOptions["query"],
    options: CallOptions,
    categorize = false,
  ): Promise<unknown> {
    const res = await this.#request<unknown>({
      method: "GET",
      path,
      operation,
      query,
      ...(categorize ? { headers: { [ENCODING_FLAGS_HEADER]: "categorize-labels" } } : {}),
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    return res.data;
  }

  // ---------------------------------------------------------------- status

  /**
   * Readiness: `GET /ready`. `200 ready` → healthy `ok`; `503` → unhealthy with Loki's text
   * (e.g. `Ingester not ready: waiting for 15s after being ready`); network errors →
   * `unreachable`. Never throws except for `CANCELLED` and `CONFIG`.
   */
  health(options: CallOptions = {}): Promise<HealthStatus> {
    return probeHealth(async () => {
      try {
        await this.#request({
          method: "GET",
          path: "/ready",
          operation: "health",
          responseType: "text",
          idempotent: false, // report 503 at once instead of retrying it
          ...options,
        });
        return healthStatus("ok", {});
      } catch (err) {
        if (err instanceof LokiError && err.httpStatus === 503) {
          const text = /^health: (.*) \(HTTP 503\)$/.exec(err.message)?.[1] ?? "unhealthy";
          return healthStatus(text, { httpStatus: 503 }, false);
        }
        throw err;
      }
    });
  }

  /** `true` when `GET /ready` answers 200 (same probe as {@link health}). */
  async ready(options: CallOptions = {}): Promise<boolean> {
    return (await this.health(options)).healthy;
  }

  /**
   * Authenticated round trip: `GET /loki/api/v1/labels?since=5m` with auth and tenant headers.
   * Returns the latency in milliseconds; `AuthenticationError` without a tenant (multi-tenant
   * Loki) or with wrong gateway credentials.
   */
  async ping(options: CallOptions = {}): Promise<number> {
    const start = performance.now();
    await this.#request({
      method: "GET",
      path: "/loki/api/v1/labels",
      query: { since: "5m" },
      operation: "ping",
      responseType: "none",
      ...options,
    });
    return performance.now() - start;
  }

  /** Build information: `GET /loki/api/v1/status/buildinfo`. */
  async buildInfo(options: CallOptions = {}): Promise<LokiBuildInfo> {
    return decodeBuildInfo(
      await this.#getJson("/loki/api/v1/status/buildinfo", "buildInfo", undefined, options),
    );
  }

  /** Server version (`buildInfo().version`, e.g. `"3.7.10"`). */
  async serverVersion(options: CallOptions = {}): Promise<string> {
    const info = await this.buildInfo(options);
    if (info.version === "") {
      throw new UnsupportedError("serverVersion: the service does not report a version");
    }
    return info.version;
  }

  // ------------------------------------------------------------------ push

  /**
   * Pushes streams with `POST /loki/api/v1/push`. JSON by default (optionally gzip);
   * `format: "protobuf"` sends a Snappy-compressed `PushRequest`. Labels are validated client
   * side. Retrying is safe: Loki drops entries identical to stored ones (same labels,
   * timestamp and line), so the push is marked idempotent and retried on 5xx and network
   * errors. Status 260 (ingestion blocked by the operator) throws a non-retryable error.
   */
  async push(streams: readonly LokiStream[], options: PushOptions = {}): Promise<void> {
    const op = "push";
    const live = streams.filter((s) => s.entries.length > 0);
    if (live.length === 0) return;
    const normalized: LokiStream[] = live.map((s) => {
      validateLabels(s.labels);
      return {
        labels: s.labels,
        entries: s.entries.map((e) => {
          if (typeof e.line !== "string") throw new ValidationError(`${op}: line must be a string`);
          for (const [k, v] of Object.entries(e.metadata ?? {})) {
            validateLabelName(k, "structured metadata key");
            if (typeof v !== "string") {
              throw new ValidationError(`${op}: structured metadata ${k} must be a string`);
            }
          }
          return { ...e, timestamp: toNanos(e.timestamp, "timestamp") };
        }),
      };
    });
    const format: string = options.format ?? "json";
    if (format !== "json" && format !== "protobuf") {
      throw new ValidationError(`${op}: unknown format ${format}`);
    }
    let body: Uint8Array | string;
    const headers: Record<string, string> = {};
    if (format === "protobuf") {
      body = snappyCompress(encodePushRequest(normalized));
      headers["content-type"] = "application/x-protobuf";
    } else {
      body = JSON.stringify({
        streams: normalized.map((s) => ({
          stream: s.labels,
          values: s.entries.map((e) =>
            e.metadata !== undefined && Object.keys(e.metadata).length > 0
              ? [e.timestamp, e.line, e.metadata]
              : [e.timestamp, e.line],
          ),
        })),
      });
      headers["content-type"] = "application/json";
      if (options.gzip === true) {
        body = gzipSync(body);
        headers["content-encoding"] = "gzip";
      }
    }
    const res = await this.#request({
      method: "POST",
      path: "/loki/api/v1/push",
      operation: op,
      body,
      headers,
      idempotent: true,
      responseType: "none",
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    if (res.status === 260) {
      throw new UnavailableError(`${op}: ingestion is blocked for this tenant (HTTP 260)`, {
        httpStatus: 260,
        retryable: false,
      });
    }
  }

  /**
   * Pushes records as OTLP/JSON to `POST /otlp/v1/logs` (Loki's OTLP endpoint). Loki promotes
   * its default resource attributes (`service.name`, `deployment.environment`, …) to labels and
   * stores everything else as structured metadata.
   */
  async pushOtlp(records: readonly LogRecord[], options: OtlpPushOptions = {}): Promise<void> {
    if (records.length === 0) return;
    let body: string | Uint8Array = JSON.stringify(recordsToOtlpJson(records));
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (options.gzip === true) {
      body = gzipSync(body);
      headers["content-encoding"] = "gzip";
    }
    try {
      await this.#request({
        method: "POST",
        path: "/otlp/v1/logs",
        operation: "pushOtlp",
        body,
        headers,
        idempotent: true,
        responseType: "none",
        ...(options.signal === undefined ? {} : { signal: options.signal }),
      });
    } catch (err) {
      throw cleanRpcStatus(err);
    }
  }

  // ----------------------------------------------------------------- query

  /**
   * Instant metric query: `GET /loki/api/v1/query`. A plain log selector (`{...}` with or
   * without a pipeline) is rejected with `ValidationError`, as Loki does not run log queries
   * as instant queries; use {@link queryRange} instead.
   */
  async query(logql: string, options: QueryOptions = {}): Promise<LokiQueryResult> {
    checkQuery(logql, "query");
    if (logql.trim().startsWith("{")) {
      throw new ValidationError(
        "query: log queries cannot run as instant queries; use queryRange or search",
      );
    }
    checkLimit(options.limit, "query");
    return decodeQueryResult(
      await this.#getJson(
        "/loki/api/v1/query",
        "query",
        {
          query: logql,
          time: optionalNanos(options.time, "time"),
          limit: options.limit,
          direction: options.direction,
        },
        options,
        true,
      ),
    );
  }

  /** Range query (logs or metrics): `GET /loki/api/v1/query_range`. */
  async queryRange(logql: string, options: QueryRangeOptions = {}): Promise<LokiQueryResult> {
    checkQuery(logql, "queryRange");
    checkLimit(options.limit, "queryRange", MAX_QUERY_LIMIT);
    return decodeQueryResult(
      await this.#getJson(
        "/loki/api/v1/query_range",
        "queryRange",
        {
          query: logql,
          start: optionalNanos(options.start, "start"),
          end: optionalNanos(options.end, "end"),
          step: options.stepMs === undefined ? undefined : msToDuration(options.stepMs, "stepMs"),
          interval:
            options.intervalMs === undefined
              ? undefined
              : msToDuration(options.intervalMs, "intervalMs"),
          limit: options.limit,
          direction: options.direction,
        },
        options,
        true,
      ),
    );
  }

  /** Label names: `GET /loki/api/v1/labels` (an empty result is `[]`). */
  async labels(options: LabelsOptions = {}): Promise<string[]> {
    return decodeStringList(
      await this.#getJson(
        "/loki/api/v1/labels",
        "labels",
        {
          start: optionalNanos(options.start, "start"),
          end: optionalNanos(options.end, "end"),
          query: options.query,
        },
        options,
      ),
    );
  }

  /**
   * Values of one label: `GET /loki/api/v1/label/{name}/values`. The name is validated client
   * side (Loki answers unknown and invalid names with an empty result).
   */
  async labelValues(name: string, options: LabelsOptions = {}): Promise<string[]> {
    validateLabelName(name);
    return decodeStringList(
      await this.#getJson(
        `/loki/api/v1/label/${encodeURIComponent(name)}/values`,
        "labelValues",
        {
          start: optionalNanos(options.start, "start"),
          end: optionalNanos(options.end, "end"),
          query: options.query,
        },
        options,
      ),
    );
  }

  /** Series matching any of the selectors: `GET /loki/api/v1/series` (at least one matcher). */
  async series(
    matchers: string | readonly string[],
    options: TimeRangeOptions = {},
  ): Promise<Record<string, string>[]> {
    const list = typeof matchers === "string" ? [matchers] : [...matchers];
    if (list.length === 0 || list.some((m) => m.trim() === "")) {
      throw new ValidationError("series: at least one non-empty matcher is required");
    }
    return decodeSeries(
      await this.#getJson(
        "/loki/api/v1/series",
        "series",
        {
          "match[]": list,
          start: optionalNanos(options.start, "start"),
          end: optionalNanos(options.end, "end"),
        },
        options,
      ),
    );
  }

  /**
   * Approximate index statistics: `GET /loki/api/v1/index/stats`. Reads the TSDB index only,
   * so data still in the ingester counts after the next chunk flush.
   */
  async indexStats(query: string, options: TimeRangeOptions = {}): Promise<LokiIndexStats> {
    checkQuery(query, "indexStats");
    return decodeIndexStats(
      await this.#getJson(
        "/loki/api/v1/index/stats",
        "indexStats",
        {
          query,
          start: optionalNanos(options.start, "start"),
          end: optionalNanos(options.end, "end"),
        },
        options,
      ),
    );
  }

  #volumeQuery(query: string, options: VolumeOptions): RequestOptions["query"] {
    checkLimit(options.limit, "volume");
    return {
      query,
      start: optionalNanos(options.start, "start"),
      end: optionalNanos(options.end, "end"),
      limit: options.limit,
      targetLabels:
        options.targetLabels === undefined || options.targetLabels.length === 0
          ? undefined
          : options.targetLabels.join(","),
      aggregateBy: options.aggregateBy,
    };
  }

  /**
   * Ingested bytes per series or label: `GET /loki/api/v1/index/volume` (vector result).
   * Needs `limits_config.volume_enabled: true`; otherwise Loki returns an empty vector.
   */
  async volume(query: string, options: VolumeOptions = {}): Promise<LokiQueryResult> {
    checkQuery(query, "volume");
    return decodeQueryResult(
      await this.#getJson(
        "/loki/api/v1/index/volume",
        "volume",
        this.#volumeQuery(query, options),
        options,
      ),
    );
  }

  /**
   * Volume over time: `GET /loki/api/v1/index/volume_range`. Always a matrix: Loki answers
   * with a `vector` when every series has a single point, which is converted to one-sample
   * matrix series.
   */
  async volumeRange(query: string, options: VolumeRangeOptions = {}): Promise<LokiQueryResult> {
    checkQuery(query, "volumeRange");
    return vectorToMatrix(
      decodeQueryResult(
        await this.#getJson(
          "/loki/api/v1/index/volume_range",
          "volumeRange",
          {
            ...this.#volumeQuery(query, options),
            step: options.stepMs === undefined ? undefined : msToDuration(options.stepMs, "stepMs"),
          },
          options,
        ),
      ),
    );
  }

  /** Normalizes a LogQL query on the server (`GET /loki/api/v1/format_query`). */
  async formatQuery(logql: string, options: CallOptions = {}): Promise<string> {
    checkQuery(logql, "formatQuery");
    const body = await this.#getJson(
      "/loki/api/v1/format_query",
      "formatQuery",
      { query: logql },
      options,
    );
    const data = (body as { data?: unknown } | undefined)?.data;
    if (typeof data !== "string") {
      throw new UnavailableError("formatQuery: unexpected response", { retryable: false });
    }
    return data;
  }

  /**
   * Live tail over WebSocket (`/loki/api/v1/tail`). Resolves once connected; handshake errors
   * reject (`AuthenticationError`, `ValidationError`, …). `onEntries` receives the streams of
   * every frame; dropped entries go to `onDropped`, later errors to `onError`. Only log queries
   * (starting with `{`) are allowed. Stop with `handle.stop()` or `close()`.
   * Proxies are not used for the WebSocket.
   */
  async tail(
    logql: string,
    onEntries: (streams: LokiStreamResult[], frame: LokiTailFrame) => void,
    options: TailStreamOptions = {},
  ): Promise<LokiTail> {
    const op = "tail";
    checkQuery(logql, op);
    if (!logql.trim().startsWith("{")) {
      throw new ValidationError(`${op}: only log queries (starting with a selector) can be tailed`);
    }
    checkLimit(options.limit, op);
    let delayFor: number | undefined;
    if (options.delayForMs !== undefined) {
      delayFor = Math.round(options.delayForMs / 1000);
      if (!Number.isFinite(delayFor) || delayFor < 0 || delayFor > 5) {
        throw new ValidationError(`${op}: delayForMs must be between 0 and 5000`);
      }
    }
    if (this.#closed || this.#transport.closed) {
      throw new ConfigError(`${op}: client is closed`);
    }
    const url = this.#transport.buildUrl("/loki/api/v1/tail", {
      query: logql,
      start: optionalNanos(options.start, "start"),
      limit: options.limit,
      delay_for: delayFor,
    });
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const headers: Record<string, string> = {};
    const auth = this.config.authorizationHeader();
    for (const [k, v] of Object.entries({
      ...this.config.headers,
      ...(auth === undefined ? {} : { authorization: auth }),
      ...this.#tenantHeaders(),
      [ENCODING_FLAGS_HEADER]: "categorize-labels",
      "user-agent": this.#transport.userAgent,
    })) {
      headers[k.toLowerCase()] = v;
    }
    const handle = await openTail(
      {
        url,
        headers,
        tls: this.config.tls,
        connectTimeoutMs: this.config.connectTimeoutMs,
        ...(options.signal === undefined ? {} : { signal: options.signal }),
        redact: (t) => this.#transport.redact(t),
      },
      (frame) => {
        onEntries(frame.streams, frame);
      },
      {
        ...(options.onDropped === undefined ? {} : { onDropped: options.onDropped }),
        ...(options.onError === undefined ? {} : { onError: options.onError }),
      },
    );
    this.#tails.add(handle);
    void handle.closed.then(() => this.#tails.delete(handle));
    return handle;
  }

  // --------------------------------------------------------------- logging

  /**
   * A batching {@link LogClient} that writes to this Loki (and tenant). Records go through
   * redaction and the cardinality guard; `protocol: "otlp"` uses `/otlp/v1/logs` instead.
   * {@link close} flushes and closes every log client created here.
   */
  logClient(options: LokiLogClientOptions = {}): BatchingLogClient {
    const { protocol, allowedLabels, ...batching } = options;
    const exporter =
      protocol === "otlp"
        ? new LokiOtlpExporter(this)
        : new LokiLogExporter(this, {
            format: protocol ?? "json",
            ...(allowedLabels === undefined ? {} : { allowedLabels }),
          });
    const client = new BatchingLogClient(exporter, batching);
    this.#logClients.add(client);
    return client;
  }

  /** A {@link LogQueryClient} (search, tail, listStreams, listLabels, waitForLog) on this Loki. */
  logQueryClient(): LokiLogQueryClient {
    return new LokiLogQueryClient(this);
  }

  /**
   * Flushes and closes log clients created by {@link logClient}, stops open tails and releases
   * connections (children from {@link withTenant} leave the shared connections open).
   * Idempotent.
   */
  close(): Promise<void> {
    this.#closing ??= this.#shutdown();
    return this.#closing;
  }

  async #shutdown(): Promise<void> {
    // Log clients flush through this client, so they close before requests are refused.
    await Promise.allSettled([...this.#logClients].map((c) => c.close()));
    this.#logClients.clear();
    this.#closed = true;
    for (const t of this.#tails) t.stop();
    this.#tails.clear();
    if (this.#ownsTransport) await this.#transport.close();
  }
}
