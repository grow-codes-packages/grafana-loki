// Service-owned: WebSocket client for `GET /loki/api/v1/tail`.

import { readFileSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import WebSocket from "ws";
import {
  CancelledError,
  type LokiError,
  MAX_VENDOR_TEXT,
  TimeoutError,
  UnavailableError,
  errorForStatus,
  type TlsOptions,
} from "./core/index.js";
import { decodeTailFrame, type LokiDroppedEntry, type LokiTailFrame } from "./results.js";

/** Handle of an open tail stream. */
export interface LokiTail {
  /** Closes the WebSocket (close frame 1000). Idempotent. */
  stop(): void;
  /** Resolves once the socket is closed (after {@link stop} or when the server closes it). */
  readonly closed: Promise<void>;
}

/** Callbacks of a tail stream besides the frame handler. */
export interface TailCallbacks {
  /** Entries the server dropped because the client fell behind. */
  onDropped?: (entries: LokiDroppedEntry[]) => void;
  /** Errors after the connection was established (bad frame, abnormal close). */
  onError?: (error: LokiError) => void;
}

/** Connection parameters (built by the client). @internal */
export interface TailConnectParams {
  url: URL;
  headers: Record<string, string>;
  tls: Readonly<TlsOptions>;
  connectTimeoutMs: number;
  signal?: AbortSignal;
  redact: (text: string) => string;
}

function vendorText(text: string, redact: (t: string) => string): string | undefined {
  const trimmed = text.trim().replace(/\s+/g, " ");
  if (trimmed === "") return undefined;
  const red = redact(trimmed);
  return red.length > MAX_VENDOR_TEXT ? red.slice(0, MAX_VENDOR_TEXT - 1) + "…" : red;
}

function tlsOptions(tls: Readonly<TlsOptions>): WebSocket.ClientOptions {
  const ca = [
    tls.caCertPem,
    tls.caCertPath === undefined ? undefined : readFileSync(tls.caCertPath, "utf8"),
  ].filter((v): v is string => v !== undefined);
  return {
    rejectUnauthorized: tls.insecureSkipVerify !== true,
    ...(ca.length > 0 ? { ca } : {}),
    ...(tls.clientCertPath === undefined ? {} : { cert: readFileSync(tls.clientCertPath, "utf8") }),
    ...(tls.clientKeyPath === undefined ? {} : { key: readFileSync(tls.clientKeyPath, "utf8") }),
  };
}

/**
 * Opens the tail WebSocket. Resolves when the server accepted the upgrade; handshake failures
 * reject with the mapped SDK error (401 → `AuthenticationError`, 400 → `ValidationError`, …).
 * @internal
 */
export function openTail(
  params: TailConnectParams,
  onFrame: (frame: LokiTailFrame) => void,
  callbacks: TailCallbacks = {},
): Promise<LokiTail> {
  const op = "tail";
  return new Promise<LokiTail>((resolve, reject) => {
    if (params.signal?.aborted === true) {
      reject(new CancelledError(`${op}: cancelled`));
      return;
    }
    let settled = false;
    let opened = false;
    let stopped = false;
    let resolveClosed!: () => void;
    const closed = new Promise<void>((r) => {
      resolveClosed = r;
    });
    const ws = new WebSocket(params.url, {
      headers: params.headers,
      handshakeTimeout: params.connectTimeoutMs,
      followRedirects: false,
      perMessageDeflate: false,
      ...tlsOptions(params.tls),
    });
    const fail = (error: LokiError): void => {
      if (settled) return;
      settled = true;
      params.signal?.removeEventListener("abort", onAbort);
      reject(error);
    };
    const report = (error: LokiError): void => {
      try {
        callbacks.onError?.(error);
      } catch {
        // callbacks must not break the stream
      }
    };
    const handle: LokiTail = {
      stop(): void {
        if (stopped) return;
        stopped = true;
        params.signal?.removeEventListener("abort", onAbort);
        if (ws.readyState === WebSocket.OPEN) ws.close(1000);
        else ws.terminate();
      },
      closed,
    };
    function onAbort(): void {
      if (!settled) {
        fail(new CancelledError(`${op}: cancelled`));
        ws.terminate();
      } else {
        handle.stop();
      }
    }
    params.signal?.addEventListener("abort", onAbort, { once: true });

    ws.on("unexpected-response", (req, res: IncomingMessage) => {
      const socket = res.socket;
      const chunks: Buffer[] = [];
      res.on("data", (c: Buffer) => chunks.push(c));
      res.on("end", () => {
        const status = res.statusCode ?? 0;
        const reason = vendorText(Buffer.concat(chunks).toString("utf8"), params.redact);
        fail(errorForStatus(op, status, reason === undefined ? {} : { reason }));
        // Loki keeps the connection alive after a rejected upgrade; close it so the socket does
        // not hold the process open until the server's idle timeout.
        socket.destroy();
        req.destroy();
        resolveClosed();
      });
    });
    ws.on("open", () => {
      if (settled) return;
      settled = true;
      opened = true;
      resolve(handle);
    });
    ws.on("message", (data: WebSocket.RawData) => {
      let frame: LokiTailFrame;
      try {
        frame = decodeTailFrame(
          JSON.parse(Buffer.from(data as Buffer).toString("utf8")) as unknown,
        );
      } catch (err) {
        report(
          err instanceof UnavailableError
            ? err
            : new UnavailableError(`${op}: malformed frame`, { retryable: false }),
        );
        return;
      }
      if (frame.droppedEntries.length > 0) {
        try {
          callbacks.onDropped?.(frame.droppedEntries);
        } catch {
          // ignore
        }
      }
      if (frame.streams.length > 0) onFrame(frame);
    });
    ws.on("error", (err: Error) => {
      if (!settled) {
        fail(
          /timed out/i.test(err.message)
            ? new TimeoutError(`${op}: connect timeout after ${params.connectTimeoutMs} ms`)
            : new UnavailableError(`${op}: ${params.redact(err.message)}`),
        );
      } else if (opened && !stopped) {
        report(new UnavailableError(`${op}: ${params.redact(err.message)}`));
      }
    });
    ws.on("close", (code: number, reasonBuf: Buffer) => {
      params.signal?.removeEventListener("abort", onAbort);
      const reason = vendorText(reasonBuf.toString("utf8"), params.redact);
      if (!settled) {
        fail(new UnavailableError(`${op}: connection closed before the upgrade (code ${code})`));
      } else if (opened && !stopped && code !== 1000) {
        report(
          new UnavailableError(
            `${op}: connection closed (code ${code})${reason === undefined ? "" : `: ${reason}`}`,
            { retryable: code === 1001 || code === 1006 },
          ),
        );
      }
      stopped = true;
      resolveClosed();
    });
  });
}
