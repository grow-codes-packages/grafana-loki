// Service-owned: generated once by sdk-kit, then maintained in this repository.

import {
  type CommonConfig,
  type CommonConfigOptions,
  type CommonVendorEnv,
  ConfigError,
  type EnvMap,
  EnvReader,
  type Hooks,
  type ClientLogger,
  REDACTED,
  type RetryPolicy,
  Secret,
  type TlsOptions,
  describeCommonConfig,
  mergeOptions,
  readCommonEnv,
  resolveCommonConfig,
  secretValues,
  validateCommonConfig,
} from "./core/index.js";

const INSPECT = Symbol.for("nodejs.util.inspect.custom");

/** Port text from the stack catalog; the default URL uses its first number. */
const CATALOG_PORT = "3100";

/** Longest tenant id Loki accepts (bytes). */
export const MAX_TENANT_ID_LENGTH = 150;

const TENANT_CHARS = /^[A-Za-z0-9!\-_.*'()]+$/;

/**
 * Returns the problem with a tenant id (`X-Scope-OrgID`), or `undefined` when it is valid.
 * Loki rules: non-empty, at most 150 bytes, not `.`/`..`, only `[A-Za-z0-9!-_.*'()]`. A `|`
 * (multi-tenant query) is rejected because Loki disables multi-tenant queries by default.
 */
export function tenantIdProblem(id: string): string | undefined {
  if (id === "") return "tenant id must not be empty";
  if (new TextEncoder().encode(id).length > MAX_TENANT_ID_LENGTH) {
    return `tenant id is too long (max ${MAX_TENANT_ID_LENGTH} bytes)`;
  }
  if (id === "." || id === "..") return "tenant id must not be '.' or '..'";
  if (id.includes("|")) return "tenant id must not contain '|' (multi-tenant queries)";
  if (!TENANT_CHARS.test(id)) {
    return "tenant id may only contain letters, digits and !-_.*'()";
  }
  return undefined;
}

/** Options accepted by {@link LokiConfig} (all optional). */
export interface LokiConfigOptions extends CommonConfigOptions {
  /** Tenant sent as `X-Scope-OrgID` (env `GROW_GRAFANA_LOKI_TENANT_ID`, `LOKI_TENANT_ID`). */
  tenantId?: string;
  /** Basic-auth user for gateways / Grafana Cloud (env `GROW_GRAFANA_LOKI_USERNAME`, `LOKI_USERNAME`). */
  username?: string;
  /** Basic-auth password or access token (env `GROW_GRAFANA_LOKI_PASSWORD`, `LOKI_PASSWORD`). */
  password?: string | Secret;
  /** Bearer token for gateways (env `GROW_GRAFANA_LOKI_BEARER_TOKEN`, `LOKI_BEARER_TOKEN`). */
  bearerToken?: string | Secret;
}

/** Vendor environment variables read after `GROW_GRAFANA_LOKI_*`, in order. */
export const VENDOR_ENV: CommonVendorEnv & {
  tenantId: readonly string[];
  username: readonly string[];
  password: readonly string[];
  bearerToken: readonly string[];
} = {
  url: ["LOKI_URL"],
  tenantId: ["LOKI_TENANT_ID"],
  username: ["LOKI_USERNAME", "LOKI_BASIC_AUTH_USER"],
  password: ["LOKI_PASSWORD", "LOKI_BASIC_AUTH_PASSWORD"],
  bearerToken: ["LOKI_BEARER_TOKEN"],
};

/** Immutable configuration of {@link LokiClient}. */
export class LokiConfig implements CommonConfig {
  /** Default endpoint. */
  static readonly DEFAULT_URL = `http://127.0.0.1:${/\d+/.exec(CATALOG_PORT)?.[0] ?? "8080"}`;
  /** Prefix of every environment variable. */
  static readonly ENV_PREFIX = "GROW_GRAFANA_LOKI_";

  readonly url: string;
  readonly connectTimeoutMs: number;
  readonly requestTimeoutMs: number;
  readonly totalTimeoutMs: number;
  readonly retry: RetryPolicy;
  readonly tls: Readonly<TlsOptions>;
  readonly proxy?: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly userAgentSuffix?: string;
  readonly hooks?: Hooks;
  readonly logger?: ClientLogger;
  /** Tenant (`X-Scope-OrgID`); not sent when unset. */
  readonly tenantId?: string;
  /** Basic-auth user. */
  readonly username?: string;
  /** Basic-auth password. */
  readonly password?: Secret;
  /** Bearer token. */
  readonly bearerToken?: Secret;

  /** Applies defaults, validates (throws {@link ConfigError}) and freezes. */
  constructor(options: LokiConfigOptions = {}) {
    const common = resolveCommonConfig(options, LokiConfig.DEFAULT_URL);
    this.url = common.url;
    this.connectTimeoutMs = common.connectTimeoutMs;
    this.requestTimeoutMs = common.requestTimeoutMs;
    this.totalTimeoutMs = common.totalTimeoutMs;
    this.retry = common.retry;
    this.tls = common.tls;
    this.headers = common.headers;
    if (common.proxy !== undefined) this.proxy = common.proxy;
    if (common.userAgentSuffix !== undefined) this.userAgentSuffix = common.userAgentSuffix;
    if (common.hooks !== undefined) this.hooks = common.hooks;
    if (common.logger !== undefined) this.logger = common.logger;
    if (options.tenantId !== undefined && options.tenantId !== "") this.tenantId = options.tenantId;
    if (options.username !== undefined && options.username !== "") this.username = options.username;
    const password = Secret.from(options.password);
    if (password !== undefined) this.password = password;
    const bearer = Secret.from(options.bearerToken);
    if (bearer !== undefined) this.bearerToken = bearer;
    this.validate();
    Object.freeze(this);
  }

  /** Returns `options` when it already is a config, otherwise builds one. */
  static from(options: LokiConfigOptions = {}): LokiConfig {
    if (options instanceof LokiConfig) return options;
    return new LokiConfig(options);
  }

  /**
   * Builds a config from the environment. Precedence: explicit `overrides` >
   * `GROW_GRAFANA_LOKI_<NAME>` > vendor variables ({@link VENDOR_ENV}) > defaults.
   */
  static fromEnv(overrides: LokiConfigOptions = {}, env: EnvMap = process.env): LokiConfig {
    const reader = new EnvReader(LokiConfig.ENV_PREFIX, env);
    const fromEnv: LokiConfigOptions = readCommonEnv(reader, VENDOR_ENV);
    const tenantId = reader.string("TENANT_ID", VENDOR_ENV.tenantId);
    if (tenantId !== undefined) fromEnv.tenantId = tenantId;
    const username = reader.string("USERNAME", VENDOR_ENV.username);
    if (username !== undefined) fromEnv.username = username;
    const password = reader.secret("PASSWORD", VENDOR_ENV.password);
    if (password !== undefined) fromEnv.password = password;
    const bearer = reader.secret("BEARER_TOKEN", VENDOR_ENV.bearerToken);
    if (bearer !== undefined) fromEnv.bearerToken = bearer;
    const merged = mergeOptions<LokiConfigOptions>(fromEnv, overrides);
    return new LokiConfig(merged);
  }

  /** A copy of this config with some fields replaced (validated again). */
  with(overrides: LokiConfigOptions): LokiConfig {
    const current: LokiConfigOptions = {
      url: this.url,
      connectTimeoutMs: this.connectTimeoutMs,
      requestTimeoutMs: this.requestTimeoutMs,
      totalTimeoutMs: this.totalTimeoutMs,
      retry: this.retry,
      tls: { ...this.tls },
      headers: { ...this.headers },
      ...(this.proxy === undefined ? {} : { proxy: this.proxy }),
      ...(this.userAgentSuffix === undefined ? {} : { userAgentSuffix: this.userAgentSuffix }),
      ...(this.hooks === undefined ? {} : { hooks: this.hooks }),
      ...(this.logger === undefined ? {} : { logger: this.logger }),
      ...(this.tenantId === undefined ? {} : { tenantId: this.tenantId }),
      ...(this.username === undefined ? {} : { username: this.username }),
      ...(this.password === undefined ? {} : { password: this.password }),
      ...(this.bearerToken === undefined ? {} : { bearerToken: this.bearerToken }),
    };
    return new LokiConfig({ ...current, ...overrides });
  }

  /** Throws {@link ConfigError} when a setting is invalid. */
  validate(): void {
    validateCommonConfig(this);
    const problems: string[] = [];
    if (this.tenantId !== undefined) {
      const problem = tenantIdProblem(this.tenantId);
      if (problem !== undefined) problems.push(problem);
    }
    if ((this.username === undefined) !== (this.password === undefined)) {
      problems.push("username and password must be given together");
    }
    if (this.password !== undefined && this.bearerToken !== undefined) {
      problems.push("basic auth (username/password) and bearerToken are mutually exclusive");
    }
    if (this.bearerToken?.reveal().trim() === "") problems.push("bearerToken must not be blank");
    if (problems.length > 0) {
      throw new ConfigError(`invalid configuration: ${problems.join("; ")}`);
    }
  }

  /** The `Authorization` header value, when credentials are configured. */
  authorizationHeader(): string | undefined {
    if (this.bearerToken !== undefined) return `Bearer ${this.bearerToken.reveal()}`;
    if (this.username !== undefined && this.password !== undefined) {
      return `Basic ${basicCredentials(this.username, this.password.reveal())}`;
    }
    return undefined;
  }

  /** Credential values to redact from error messages and logs. */
  knownSecrets(): string[] {
    const out = secretValues(this.password, this.bearerToken);
    if (this.username !== undefined && this.password !== undefined) {
      out.push(basicCredentials(this.username, this.password.reveal()));
    }
    return out;
  }

  /** Debug form with every credential redacted. */
  toJSON(): Record<string, unknown> {
    return {
      ...describeCommonConfig(this),
      tenantId: this.tenantId ?? null,
      username: this.username ?? null,
      password: this.password === undefined ? null : REDACTED,
      bearerToken: this.bearerToken === undefined ? null : REDACTED,
    };
  }

  toString(): string {
    return `LokiConfig(${JSON.stringify(this.toJSON())})`;
  }

  [INSPECT](): string {
    return this.toString();
  }
}

function basicCredentials(username: string, password: string): string {
  return Buffer.from(`${username}:${password}`, "utf8").toString("base64");
}
