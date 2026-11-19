// Service-owned: hand-written protobuf encoder for Loki's PushRequest.
//
// Messages and field numbers follow pkg/push/push.proto of Grafana Loki v3.7.10 (Apache-2.0,
// see NOTICE); google.protobuf.Timestamp follows protobuf's timestamp.proto. No Loki code is
// copied or imported; only the wire layout is reproduced:
//
//   PushRequest      { repeated StreamAdapter streams = 1; }
//   StreamAdapter    { string labels = 1; repeated EntryAdapter entries = 2; uint64 hash = 3; }
//   EntryAdapter     { Timestamp timestamp = 1; string line = 2;
//                      repeated LabelPairAdapter structuredMetadata = 3; }
//   LabelPairAdapter { string name = 1; string value = 2; }
//   Timestamp        { int64 seconds = 1; int32 nanos = 2; }

import { compress } from "snappyjs";
import { formatLabelSet, type LokiStream } from "./labels.js";
import { toNanos } from "./time.js";

const encoder = new TextEncoder();

/** Growable byte buffer with protobuf primitives. */
class Writer {
  #chunks: Uint8Array[] = [];
  #length = 0;

  get length(): number {
    return this.#length;
  }

  bytes(b: Uint8Array): void {
    this.#chunks.push(b);
    this.#length += b.length;
  }

  varint(value: bigint): void {
    const out: number[] = [];
    let v = value;
    while (v > 0x7fn) {
      out.push(Number(v & 0x7fn) | 0x80);
      v >>= 7n;
    }
    out.push(Number(v));
    this.bytes(Uint8Array.from(out));
  }

  tag(field: number, wireType: number): void {
    this.varint(BigInt((field << 3) | wireType));
  }

  /** Length-delimited field (wire type 2). */
  lengthDelimited(field: number, payload: Uint8Array): void {
    this.tag(field, 2);
    this.varint(BigInt(payload.length));
    this.bytes(payload);
  }

  string(field: number, value: string): void {
    this.lengthDelimited(field, encoder.encode(value));
  }

  /** Varint field (wire type 0); zero values are omitted (proto3 default). */
  uint(field: number, value: bigint): void {
    if (value === 0n) return;
    this.tag(field, 0);
    this.varint(value);
  }

  finish(): Uint8Array {
    const out = new Uint8Array(this.#length);
    let offset = 0;
    for (const c of this.#chunks) {
      out.set(c, offset);
      offset += c.length;
    }
    return out;
  }
}

function encodeTimestamp(ns: bigint): Uint8Array {
  const w = new Writer();
  w.uint(1, ns / 1_000_000_000n);
  w.uint(2, ns % 1_000_000_000n);
  return w.finish();
}

/** Encodes streams as an (uncompressed) Loki `PushRequest` protobuf message. */
export function encodePushRequest(streams: readonly LokiStream[]): Uint8Array {
  const req = new Writer();
  for (const stream of streams) {
    const s = new Writer();
    s.string(1, formatLabelSet(stream.labels));
    for (const entry of stream.entries) {
      const e = new Writer();
      e.lengthDelimited(
        1,
        encodeTimestamp(BigInt(toNanos(entry.timestamp ?? new Date(), "timestamp"))),
      );
      e.string(2, entry.line);
      for (const [name, value] of Object.entries(entry.structuredMetadata ?? {})) {
        const pair = new Writer();
        pair.string(1, name);
        pair.string(2, value);
        e.lengthDelimited(3, pair.finish());
      }
      s.lengthDelimited(2, e.finish());
    }
    req.lengthDelimited(1, s.finish());
  }
  return req.finish();
}

/** Snappy block compression (not the framed format), as `/loki/api/v1/push` expects. */
export function snappyCompress(data: Uint8Array): Uint8Array {
  return compress(data);
}

/**
 * Extracts the message (field 2) of a protobuf `google.rpc.Status` body, which Loki returns
 * for OTLP errors. Returns `undefined` when the bytes do not look like one.
 */
export function decodeRpcStatusMessage(bytes: Uint8Array): string | undefined {
  let pos = 0;
  const readVarint = (): number | undefined => {
    let result = 0;
    let shift = 0;
    while (pos < bytes.length && shift < 35) {
      const b = bytes[pos++] as number;
      result += (b & 0x7f) * 2 ** shift;
      if ((b & 0x80) === 0) return result;
      shift += 7;
    }
    return undefined;
  };
  let message: string | undefined;
  while (pos < bytes.length) {
    const tag = readVarint();
    if (tag === undefined) return undefined;
    const field = tag >> 3;
    const wire = tag & 7;
    if (wire === 0) {
      if (readVarint() === undefined) return undefined;
    } else if (wire === 2) {
      const len = readVarint();
      if (len === undefined || pos + len > bytes.length) return undefined;
      if (field === 2) message = new TextDecoder().decode(bytes.subarray(pos, pos + len));
      pos += len;
    } else {
      return undefined;
    }
  }
  return message;
}
