import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket } from "ws";
import { LokiClient, type LokiError, type LokiStreamResult, type LogRecord } from "../src/index.js";
import { fixture } from "./helpers/loki.js";

interface WsFake {
  url: string;
  requests: IncomingMessage[];
  sockets: WebSocket[];
  close(): Promise<void>;
}

type Behaviour = (req: IncomingMessage, socket: Socket, head: Buffer, wss: WebSocketServer) => void;

const fakes: WsFake[] = [];
afterEach(async () => {
  await Promise.all(fakes.splice(0).map((f) => f.close()));
});

async function startWs(behaviour: Behaviour): Promise<WsFake> {
  const requests: IncomingMessage[] = [];
  const sockets: WebSocket[] = [];
  const raw = new Set<Socket>();
  const wss = new WebSocketServer({ noServer: true });
  wss.on("connection", (ws) => sockets.push(ws));
  const server: Server = createServer((_req, res) => {
    res.writeHead(404);
    res.end();
  });
  server.on("connection", (s) => raw.add(s));
  server.on("upgrade", (req, socket, head) => {
    requests.push(req);
    behaviour(req, socket as unknown as Socket, head, wss);
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as AddressInfo).port;
  const fake: WsFake = {
    url: `http://127.0.0.1:${port}`,
    requests,
    sockets,
    close: () =>
      new Promise<void>((r) => {
        for (const ws of wss.clients) ws.terminate();
        for (const s of raw) s.destroy();
        wss.close();
        server.close(() => r());
      }),
  };
  fakes.push(fake);
  return fake;
}

const accept: Behaviour = (req, socket, head, wss) => {
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
};

function reject(status: number, body: string): Behaviour {
  return (_req, socket) => {
    socket.end(
      `HTTP/1.1 ${status} X\r\ncontent-type: text/plain\r\ncontent-length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
    );
  };
}

function waitFor(cond: () => boolean, ms = 3000): Promise<void> {
  const where = new Error("waitFor").stack;
  const start = Date.now();
  return new Promise((resolve, rej) => {
    const tick = (): void => {
      if (cond()) resolve();
      else if (Date.now() - start > ms) rej(new Error(`timeout ${where}`));
      else setTimeout(tick, 10);
    };
    tick();
  });
}

describe("tail (WebSocket)", () => {
  it("sends the handshake headers and delivers recorded frames", async () => {
    const f = await startWs(accept);
    const client = new LokiClient({
      url: f.url,
      tenantId: "sdk-tenant-a",
      bearerToken: "tail-bearer-123",
      headers: { "X-Extra": "1" },
    });
    const got: LokiStreamResult[][] = [];
    const dropped: unknown[] = [];
    const errors: LokiError[] = [];
    const handle = await client.tail('{run="x"}', (streams) => got.push(streams), {
      start: 5n,
      limit: 10,
      delayForMs: 2000,
      onDropped: (d) => dropped.push(...d),
      onError: (e) => errors.push(e),
    });
    const req = f.requests[0]!;
    const url = new URL(req.url!, "http://x");
    expect(url.pathname).toBe("/loki/api/v1/tail");
    expect(url.searchParams.get("query")).toBe('{run="x"}');
    expect(url.searchParams.get("start")).toBe("5");
    expect(url.searchParams.get("limit")).toBe("10");
    expect(url.searchParams.get("delay_for")).toBe("2");
    expect(req.headers["x-scope-orgid"]).toBe("sdk-tenant-a");
    expect(req.headers.authorization).toBe("Bearer tail-bearer-123");
    expect(req.headers["x-loki-response-encoding-flags"]).toBe("categorize-labels");
    expect(req.headers["x-extra"]).toBe("1");
    expect(req.headers["user-agent"]).toMatch(/^grow-codes-grafana-loki\//);

    await waitFor(() => f.sockets.length === 1);
    const ws = f.sockets[0]!;
    ws.send(JSON.stringify(fixture("tail.categorized-labels").response.frame));
    ws.send(
      JSON.stringify({ streams: [], dropped_entries: [{ labels: { a: "b" }, timestamp: "1" }] }),
    );
    ws.send("not json");
    await waitFor(() => got.length === 1 && dropped.length === 1 && errors.length === 1);
    expect(got[0]![0]!.entries[0]).toMatchObject({
      line: "tail marker c023892b83",
      structuredMetadata: { request_id: "tail-c023892b83" },
    });
    expect(errors[0]!.message).toMatch(/malformed frame/);
    ws.send(JSON.stringify({ streams: {} }));
    await waitFor(() => errors.length === 2);
    expect(errors[1]!.message).toMatch(/unexpected response/);

    handle.stop();
    handle.stop();
    await handle.closed;
    await client.close();
  });

  it("maps handshake errors (401, 400) with Loki's text", async () => {
    const f401 = await startWs(reject(401, "no org id\n"));
    const c1 = new LokiClient({ url: f401.url });
    await expect(c1.tail('{a="b"}', () => {})).rejects.toMatchObject({
      code: "AUTHENTICATION",
      httpStatus: 401,
      message: "tail: no org id (HTTP 401)",
    });
    await c1.close();
    const f400 = await startWs(reject(400, "delay_for can't be greater than 5\n"));
    const c2 = new LokiClient({ url: f400.url });
    await expect(c2.tail('{a="b"}', () => {})).rejects.toMatchObject({ code: "VALIDATION" });
    await c2.close();
    const fEmpty = await startWs(reject(403, ""));
    const c3 = new LokiClient({ url: fEmpty.url });
    await expect(c3.tail('{a="b"}', () => {})).rejects.toMatchObject({
      code: "PERMISSION_DENIED",
      message: "tail: permission denied (HTTP 403)",
    });
    await c3.close();
  });

  it("closes the kept-alive connection after a rejected handshake", async () => {
    let closed = false;
    const f = await startWs((_req, socket) => {
      socket.on("end", () => (closed = true));
      socket.resume();
      socket.write(
        "HTTP/1.1 401 Unauthorized\r\nconnection: keep-alive\r\ncontent-length: 10\r\n\r\nno org id\n",
      );
    });
    const client = new LokiClient({ url: f.url });
    await expect(client.tail('{a="b"}', () => {})).rejects.toMatchObject({
      code: "AUTHENTICATION",
    });
    await waitFor(() => closed, 2000);
    await client.close();
  });

  it("validates input client side", async () => {
    const client = new LokiClient({ url: "http://127.0.0.1:1" });
    await expect(client.tail('count_over_time({a="b"}[1m])', () => {})).rejects.toThrow(
      /log queries/,
    );
    await expect(client.tail("", () => {})).rejects.toThrow(/empty/);
    await expect(client.tail('{a="b"}', () => {}, { delayForMs: 6000 })).rejects.toThrow(
      /delayForMs/,
    );
    await expect(client.tail('{a="b"}', () => {}, { limit: 0 })).rejects.toThrow(/limit/);
    await client.close();
    await expect(client.tail('{a="b"}', () => {})).rejects.toMatchObject({ code: "CONFIG" });
  });

  it("reports connection failures, cancellation and abnormal closes", async () => {
    const refused = new LokiClient({ url: "http://127.0.0.1:1" });
    await expect(refused.tail('{a="b"}', () => {})).rejects.toMatchObject({ code: "UNAVAILABLE" });
    await expect(
      refused.tail('{a="b"}', () => {}, { signal: AbortSignal.abort() }),
    ).rejects.toMatchObject({
      code: "CANCELLED",
    });
    await refused.close();

    const hang = await startWs(() => {
      /* never answer */
    });
    const slow = new LokiClient({ url: hang.url, connectTimeoutMs: 100 });
    await expect(slow.tail('{a="b"}', () => {})).rejects.toMatchObject({ code: "TIMEOUT" });
    const controller = new AbortController();
    const pending = slow.tail('{a="b"}', () => {}, { signal: controller.signal });
    setTimeout(() => controller.abort(), 20);
    await expect(pending).rejects.toMatchObject({ code: "CANCELLED" });
    await slow.close();

    const dropper = await startWs((_req, socket) => socket.destroy());
    const c = new LokiClient({ url: dropper.url });
    await expect(c.tail('{a="b"}', () => {})).rejects.toMatchObject({ code: "UNAVAILABLE" });
    await c.close();

    const f = await startWs(accept);
    const client = new LokiClient({ url: f.url });
    const errors: LokiError[] = [];
    const ctl = new AbortController();
    const handle = await client.tail('{a="b"}', () => {}, {
      onError: (e) => errors.push(e),
      signal: ctl.signal,
    });
    await waitFor(() => f.sockets.length === 1);
    f.sockets[0]!.close(1011, "only log selector is supported");
    await handle.closed;
    expect(errors[0]).toMatchObject({
      code: "UNAVAILABLE",
      retryable: false,
      message: "tail: connection closed (code 1011): only log selector is supported",
    });
    ctl.abort();

    const second = await client
      .tail('{a="b"}', () => {}, {
        onError: () => {
          throw new Error("ignored");
        },
        signal: ctl.signal,
      })
      .catch((e: unknown) => e);
    expect(second).toMatchObject({ code: "CANCELLED" });

    const ctl2 = new AbortController();
    const h2 = await client.tail('{a="b"}', () => {}, {
      onError: () => {
        throw new Error("ignored");
      },
      onDropped: () => {
        throw new Error("ignored");
      },
      signal: ctl2.signal,
    });
    await waitFor(() => f.sockets.length === 2);
    f.sockets[1]!.send(
      JSON.stringify({ streams: [], dropped_entries: [{ labels: {}, timestamp: "1" }] }),
    );
    f.sockets[1]!.terminate();
    await h2.closed;
    ctl2.abort();

    const h3 = await client.tail('{a="b"}', () => {});
    await waitFor(() => f.sockets.length === 3);
    f.sockets[2]!.close(1001);
    await h3.closed;
    const h4 = await client.tail('{a="b"}', () => {});
    await client.close(); // stops open tails
    await h4.closed;
  });

  it("LokiLogQueryClient.tail streams normalized records and reports errors", async () => {
    const f = await startWs(accept);
    const client = new LokiClient({ url: f.url });
    const q = client.logQueryClient();
    const records: LogRecord[] = [];
    const handle = q.tail("tail marker", (r) => records.push(r), { onError: () => {} });
    await waitFor(() => f.sockets.length === 1);
    const url = new URL(f.requests[0]!.url!, "http://x");
    expect(url.searchParams.get("query")).toBe('{service_name=~".+"} |= "tail marker"');
    expect(url.searchParams.get("start")).toMatch(/^\d+$/);
    f.sockets[0]!.send(JSON.stringify(fixture("tail.categorized-labels").response.frame));
    await waitFor(() => records.length === 1);
    expect(records[0]).toMatchObject({
      body: "tail marker c023892b83",
      resource: { "service.name": "sdk-verify" },
      attributes: { request_id: "tail-c023892b83" },
    });
    handle.stop();

    const stoppedEarly = q.tail('{a="b"}', () => {});
    stoppedEarly.stop();
    await waitFor(() => f.sockets.length === 2);
    const errors: unknown[] = [];
    const bad = new LokiClient({ url: "http://127.0.0.1:1" }).logQueryClient();
    bad.tail('{a="b"}', () => {}, { onError: (e) => errors.push(e) });
    await waitFor(() => errors.length === 1);
    const silent = bad.tail('{a="b"}', () => {});
    silent.stop();
    await client.close();
  });
});
