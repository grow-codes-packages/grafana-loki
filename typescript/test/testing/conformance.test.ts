import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { LokiClient } from "../../src/client.js";
import {
  type ConformanceOptions,
  conformanceCases,
  formatReport,
  runConformance,
} from "../../src/testing/index.js";
import { type FakeLoki, startFakeLoki } from "../helpers/loki.js";

const scenariosDir = fileURLToPath(new URL("../../../conformance/scenarios/", import.meta.url));
let server: FakeLoki;
const clients: LokiClient[] = [];

beforeAll(async () => {
  server = await startFakeLoki();
});

afterAll(async () => {
  await Promise.all(clients.map((c) => c.close()));
  await server.close();
});

function lokiClient(): LokiClient {
  const c = new LokiClient({ url: server.url, retry: { maxRetries: 0 } });
  clients.push(c);
  return c;
}

function options(): ConformanceOptions {
  return {
    scenariosDir,
    categories: ["core", "logging"],
    factories: {
      core: () => lokiClient(),
      logging: (ctx) => {
        const loki = lokiClient();
        return {
          client: loki.logClient({
            batch: { flushIntervalMs: 50, ...(ctx.batchOptions ?? {}) },
            resource: { "service.name": "conformance" },
          }),
          query: loki.logQueryClient(),
        };
      },
    },
    capabilities: { core: ["version"], logging: ["query", "tail", "labels", "streams"] },
  };
}

describe("conformance (core + logging) against a fake Loki", () => {
  it("every scenario passes", async () => {
    const report = await runConformance(options());
    expect(report.failed, formatReport(report)).toBe(0);
    expect(report.skipped).toBe(0);
    expect(report.results.map((r) => r.id)).toEqual(
      expect.arrayContaining(["core.health", "logging.roundtrip", "logging.batch", "logging.redaction"]),
    );
  }, 30_000);

  for (const c of conformanceCases(options())) {
    it.skipIf(c.skip !== undefined)(`case ${c.id}`, async () => {
      expect((await c.run()).status).toBe("passed");
    }, 30_000);
  }
});
