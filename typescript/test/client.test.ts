import { inspect } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import {
  ConfigError,
  LokiClient,
  LokiConfig,
  LokiError,
  PACKAGE_KEY,
  REDACTED,
  RetryPolicy,
  Secret,
  VENDOR_ENV,
  tenantIdProblem,
} from "../src/index.js";
import { fixtureResponse, startFakeLoki } from "./helpers/loki.js";
import { type MockServer, closedPort, startServer } from "./helpers/server.js";

const servers: MockServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function track<T extends MockServer>(p: Promise<T>): Promise<T> {
  const s = await p;
  servers.push(s);
  return s;
}

const fast = { retry: { maxRetries: 1, initialBackoffMs: 1 } };

describe("LokiConfig", () => {
  it("uses defaults and freezes", () => {
    const c = new LokiConfig();
    expect(c.url).toBe("http://127.0.0.1:3100");
    expect(c.requestTimeoutMs).toBe(30_000);
    expect(c.retry).toBeInstanceOf(RetryPolicy);
    expect(c.tenantId).toBeUndefined();
    expect(c.authorizationHeader()).toBeUndefined();
    expect(c.knownSecrets()).toEqual([]);
    expect(Object.isFrozen(c)).toBe(true);
    expect(LokiConfig.from(c)).toBe(c);
    expect(LokiConfig.from().url).toBe(c.url);
    expect(LokiConfig.ENV_PREFIX).toBe(`GROW_${PACKAGE_KEY.toUpperCase().replaceAll("-", "_")}_`);
  });

  it("keeps optional settings and builds auth headers", () => {
    const hooks = {};
    const logger = { debug() {}, info() {}, warn() {}, error() {} };
    const basic = new LokiConfig({
      proxy: "http://p:1",
      userAgentSuffix: "s",
      hooks,
      logger,
      tenantId: "team-a",
      username: "123456",
      password: new Secret("glc_secret"),
    });
    expect(basic.proxy).toBe("http://p:1");
    expect(basic.userAgentSuffix).toBe("s");
    expect(basic.hooks).toBe(hooks);
    expect(basic.logger).toBe(logger);
    expect(basic.tenantId).toBe("team-a");
    const encoded = Buffer.from("123456:glc_secret").toString("base64");
    expect(basic.authorizationHeader()).toBe(`Basic ${encoded}`);
    expect(basic.knownSecrets()).toEqual(["glc_secret", encoded]);
    const bearer = new LokiConfig({ bearerToken: "bearer-abc" });
    expect(bearer.authorizationHeader()).toBe("Bearer bearer-abc");
    expect(bearer.knownSecrets()).toEqual(["bearer-abc"]);
    const copy = basic.with({ tenantId: "team-b" });
    expect(copy.tenantId).toBe("team-b");
    expect(copy.username).toBe("123456");
    expect(copy.proxy).toBe("http://p:1");
    expect(new LokiConfig().with({}).url).toBe("http://127.0.0.1:3100");
    expect(new LokiConfig({ tenantId: "", username: "" }).tenantId).toBeUndefined();
  });

  it("validate() raises ConfigError", () => {
    expect(() => new LokiConfig({ url: "ftp://x" })).toThrow(ConfigError);
    expect(() => new LokiConfig({ bearerToken: "   " })).toThrow(/bearerToken/);
    expect(() => new LokiConfig({ username: "u" })).toThrow(/together/);
    expect(() => new LokiConfig({ password: "p" })).toThrow(/together/);
    expect(() => new LokiConfig({ username: "u", password: "p", bearerToken: "b" })).toThrow(
      /mutually exclusive/,
    );
    expect(() => new LokiConfig({ tenantId: "a|b" })).toThrow(ConfigError);
  });

  it("tenant id rules", () => {
    expect(tenantIdProblem("sdk-tenant_a.1!*'()")).toBeUndefined();
    expect(tenantIdProblem("")).toMatch(/empty/);
    expect(tenantIdProblem("x".repeat(151))).toMatch(/too long/);
    expect(tenantIdProblem("x".repeat(150))).toBeUndefined();
    expect(tenantIdProblem(".")).toMatch(/'\.'/);
    expect(tenantIdProblem("..")).toMatch(/'\.'/);
    expect(tenantIdProblem("a|b")).toMatch(/\|/);
    expect(tenantIdProblem("bad/tenant")).toMatch(/only contain/);
  });

  it("core.redaction.config: toString/JSON/inspect hide credentials", () => {
    const c = new LokiConfig({
      username: "user",
      password: "super-secret-pass",
      headers: { "x-api-key": "hdr-secret" },
    });
    const b = new LokiConfig({ bearerToken: "super-secret-bearer" });
    for (const form of [String(c), JSON.stringify(c), inspect(c), String(b), inspect(b)]) {
      expect(form).not.toContain("super-secret-pass");
      expect(form).not.toContain("super-secret-bearer");
      expect(form).not.toContain("hdr-secret");
    }
    expect(String(c)).toContain(REDACTED);
    expect(String(c)).toMatch(/LokiConfig\(/);
    expect(new LokiConfig().toJSON()).toMatchObject({
      tenantId: null,
      username: null,
      password: null,
      bearerToken: null,
    });
  });

  it("core.config.env-precedence: explicit > GROW_* > vendor > default", () => {
    const prefix = LokiConfig.ENV_PREFIX;
    expect(VENDOR_ENV.url).toEqual(["LOKI_URL"]);
    const env = {
      [`${prefix}URL`]: "http://grow:1",
      LOKI_URL: "http://vendor:2",
      LOKI_TENANT_ID: "vendor-tenant",
      LOKI_BASIC_AUTH_USER: "platform-user",
      LOKI_BASIC_AUTH_PASSWORD: "platform-pass",
      [`${prefix}REQUEST_TIMEOUT`]: "5s",
      [`${prefix}MAX_RETRIES`]: "7",
    };
    const fromEnv = LokiConfig.fromEnv({}, env);
    expect(fromEnv.url).toBe("http://grow:1");
    expect(fromEnv.tenantId).toBe("vendor-tenant");
    expect(fromEnv.username).toBe("platform-user");
    expect(fromEnv.password?.reveal()).toBe("platform-pass");
    expect(fromEnv.requestTimeoutMs).toBe(5000);
    expect(fromEnv.retry.maxRetries).toBe(7);

    const vendorFirst = LokiConfig.fromEnv(
      {},
      {
        LOKI_URL: "http://vendor:2",
        LOKI_USERNAME: "u1",
        LOKI_BASIC_AUTH_USER: "u2",
        LOKI_PASSWORD: "p1",
        LOKI_BASIC_AUTH_PASSWORD: "p2",
      },
    );
    expect(vendorFirst.url).toBe("http://vendor:2");
    expect(vendorFirst.username).toBe("u1");
    expect(vendorFirst.password?.reveal()).toBe("p1");

    const grow = LokiConfig.fromEnv(
      {},
      {
        [`${prefix}TENANT_ID`]: "grow-tenant",
        LOKI_TENANT_ID: "vendor-tenant",
        [`${prefix}BEARER_TOKEN`]: "grow-bearer",
        LOKI_BEARER_TOKEN: "vendor-bearer",
      },
    );
    expect(grow.tenantId).toBe("grow-tenant");
    expect(grow.bearerToken?.reveal()).toBe("grow-bearer");
    expect(LokiConfig.fromEnv({}, { LOKI_BEARER_TOKEN: "vb" }).bearerToken?.reveal()).toBe("vb");

    const explicit = LokiConfig.fromEnv(
      { url: "http://explicit:3", tenantId: "explicit", retry: { initialBackoffMs: 1 } },
      env,
    );
    expect(explicit.url).toBe("http://explicit:3");
    expect(explicit.tenantId).toBe("explicit");
    expect(explicit.retry.maxRetries).toBe(7);
    expect(explicit.retry.initialBackoffMs).toBe(1);

    expect(LokiConfig.fromEnv({}, {}).url).toBe(LokiConfig.DEFAULT_URL);
    expect(LokiConfig.fromEnv().url).toBeTypeOf("string");

    let err: unknown;
    try {
      LokiConfig.fromEnv({}, { [`${prefix}TOTAL_TIMEOUT`]: "forever-and-secret" });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(ConfigError);
    expect((err as Error).message).toContain(`${prefix}TOTAL_TIMEOUT`);
    expect((err as Error).message).not.toContain("forever-and-secret");
  });
});

describe("LokiClient status", () => {
  it("health, ready, ping, buildInfo and serverVersion against fixtures", async () => {
    const s = await track(startFakeLoki());
    const client = new LokiClient({ url: s.url, ...fast });
    expect(await client.health()).toEqual({ healthy: true, status: "ok", details: {} });
    expect(await client.ready()).toBe(true);
    expect(await client.ping()).toBeGreaterThanOrEqual(0);
    const info = await client.buildInfo();
    expect(info).toMatchObject({ version: "3.7.10", revision: "c792f585", goVersion: "" });
    expect(await client.serverVersion()).toBe("3.7.10");
    expect(client.transport.userAgent).toMatch(/^grow-codes-grafana-loki\/\S+ \(typescript\)$/);
    const ping = s.requests.find((r) => r.url.startsWith("/loki/api/v1/labels"));
    expect(ping?.url).toBe("/loki/api/v1/labels?since=5m");
    expect(ping?.headers["x-scope-orgid"]).toBeUndefined();
    await client.close();
  });

  it("health reports 503 text without retrying, and 500 as unhealthy", async () => {
    const s = await track(
      startServer([
        { status: 503, body: "Ingester not ready: waiting for 15s after being ready\n" },
        { status: 500, body: "boom" },
      ]),
    );
    const client = new LokiClient({ url: s.url, ...fast });
    expect(await client.health()).toEqual({
      healthy: false,
      status: "Ingester not ready: waiting for 15s after being ready",
      details: { httpStatus: "503" },
    });
    expect(s.requests).toHaveLength(1);
    expect(await client.ready()).toBe(false);
    await client.close();
  });

  it("core.health.unreachable: closed port -> healthy=false, status=unreachable", async () => {
    const port = await closedPort();
    const client = new LokiClient({ url: `http://127.0.0.1:${port}`, ...fast });
    expect(await client.health()).toEqual({
      healthy: false,
      status: "unreachable",
      details: { error: "UNAVAILABLE" },
    });
    await client.close();
  });

  it("health rethrows cancellation", async () => {
    const s = await track(startFakeLoki());
    const client = new LokiClient({ url: s.url });
    await expect(client.health({ signal: AbortSignal.abort() })).rejects.toMatchObject({
      code: "CANCELLED",
    });
    await client.close();
  });

  it("ping maps 401 (no tenant / bad gateway credentials) to AuthenticationError", async () => {
    const s = await track(startServer([fixtureResponse("ping.gateway-bad-credentials")]));
    const client = new LokiClient({
      url: s.url,
      username: "u",
      password: "wrong-pass-xyz",
      ...fast,
    });
    const err = await client.ping().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(LokiError);
    expect((err as LokiError).code).toBe("AUTHENTICATION");
    expect((err as LokiError).httpStatus).toBe(401);
    expect((err as Error).message).not.toContain("wrong-pass-xyz");
    expect(s.requests[0]?.headers.authorization).toMatch(/^Basic /);
    await client.close();

    const mt = await track(startServer([fixtureResponse("ping.no-tenant")]));
    const c2 = new LokiClient({ url: mt.url, ...fast });
    await expect(c2.ping()).rejects.toMatchObject({
      code: "AUTHENTICATION",
      message: "ping: no org id (HTTP 401)",
    });
    await c2.close();
  });

  it("serverVersion raises UnsupportedError when the version is empty", async () => {
    const s = await track(startServer([{ status: 200, body: { version: "" } }]));
    const client = new LokiClient({ url: s.url, ...fast });
    await expect(client.serverVersion()).rejects.toMatchObject({ code: "UNSUPPORTED" });
    await client.close();
  });

  it("buildInfo rejects a malformed body", async () => {
    const s = await track(
      startServer([{ status: 200, body: "[1]", headers: { "content-type": "application/json" } }]),
    );
    const client = new LokiClient({ url: s.url, ...fast });
    await expect(client.buildInfo()).rejects.toMatchObject({
      code: "UNAVAILABLE",
      retryable: false,
    });
    await client.close();
  });

  it("withTenant sends X-Scope-OrgID, shares the transport and validates the id", async () => {
    const s = await track(startFakeLoki({ tenantRequired: true }));
    const root = new LokiClient({ url: s.url, ...fast });
    await expect(root.ping()).rejects.toMatchObject({ code: "AUTHENTICATION" });
    const a = root.withTenant("sdk-tenant-a");
    expect(a.tenantId).toBe("sdk-tenant-a");
    expect(root.tenantId).toBeUndefined();
    expect(a.transport).toBe(root.transport);
    expect(await a.ping()).toBeGreaterThanOrEqual(0);
    expect(s.requests.at(-1)?.headers["x-scope-orgid"]).toBe("sdk-tenant-a");
    expect(() => root.withTenant("a|b")).toThrow(expect.objectContaining({ code: "VALIDATION" }));
    expect(() => root.withTenant("bad/tenant")).toThrow(/withTenant/);
    await a.close();
    await expect(a.ping()).rejects.toMatchObject({ code: "CONFIG" });
    // the shared transport stays open for the parent
    await expect(root.withTenant("b").ping()).resolves.toBeGreaterThanOrEqual(0);
    await root.close();
  });

  it("core.close.idempotent: close twice is safe", async () => {
    const s = await track(startFakeLoki());
    const client = new LokiClient({ url: s.url });
    await client.health();
    await client.close();
    await client.close();
    await expect(client.ping()).rejects.toMatchObject({ code: "CONFIG" });
  });

  it("fromEnv builds from GROW_* variables", async () => {
    const s = await track(startFakeLoki());
    const key = `${LokiConfig.ENV_PREFIX}URL`;
    process.env[key] = s.url;
    try {
      const client = LokiClient.fromEnv({ tenantId: "t1" });
      expect(client.config.url).toBe(s.url);
      expect(await client.ping()).toBeGreaterThanOrEqual(0);
      expect(s.requests.at(-1)?.headers["x-scope-orgid"]).toBe("t1");
      await client.close();
    } finally {
      delete process.env[key];
    }
  });

  it("maps 500 to a non-retryable UnavailableError except 'Ingester is shutting down'", async () => {
    const s = await track(
      startServer((req) =>
        req.url.startsWith("/loki/api/v1/labels")
          ? { status: 500, body: "rpc error: code = Unknown desc = Ingester is shutting down\n" }
          : { status: 500, body: "multiple org IDs present\n" },
      ),
    );
    const client = new LokiClient({ url: s.url, ...fast });
    await expect(client.labels()).rejects.toMatchObject({
      code: "UNAVAILABLE",
      retryable: true,
      httpStatus: 500,
    });
    await expect(client.buildInfo()).rejects.toMatchObject({
      code: "UNAVAILABLE",
      retryable: false,
      httpStatus: 500,
    });
    await client.close();
  });
});
