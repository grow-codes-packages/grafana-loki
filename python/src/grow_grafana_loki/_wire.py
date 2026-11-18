"""Wire encoders: JSON and protobuf push bodies, Snappy, OTLP/JSON logs, ``google.rpc.Status``.

Protobuf messages are hand-encoded from the field numbers of Grafana Loki's Apache-2.0
``pkg/push/push.proto`` (v3.7.10) and protobuf's ``timestamp.proto``; no Loki code is copied or
imported (see NOTICE)::

    PushRequest      { repeated StreamAdapter streams = 1; }
    StreamAdapter    { string labels = 1; repeated EntryAdapter entries = 2; uint64 hash = 3; }
    EntryAdapter     { google.protobuf.Timestamp timestamp = 1; string line = 2;
                       repeated LabelPairAdapter structuredMetadata = 3; }
    LabelPairAdapter { string name = 1; string value = 2; }
    Timestamp        { int64 seconds = 1; int32 nanos = 2; }
"""

from __future__ import annotations

import gzip as _gzip
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from ._labels import LokiStream, label_set_string
from ._logging import LogRecord
from ._time import datetime_to_ns

__all__ = [
    "decode_rpc_status",
    "encode_otlp_logs_json",
    "encode_push_json",
    "encode_push_protobuf",
    "gzip_bytes",
    "snappy_compress",
]

#: Prepared stream: (labels, [(timestamp_ns, line, structured_metadata)]).
PreparedStream = tuple[Mapping[str, str], Sequence[tuple[int, str, Mapping[str, str]]]]


def encode_push_json(streams: Sequence[PreparedStream]) -> bytes:
    """JSON push body: ``{"streams":[{"stream":{...},"values":[["<ns>","<line>",{meta}]]}]}``."""
    out = []
    for labels, entries in streams:
        values: list[list[Any]] = []
        for ts, line, meta in entries:
            value: list[Any] = [str(ts), line]
            if meta:
                value.append(dict(meta))
            values.append(value)
        out.append({"stream": dict(labels), "values": values})
    return json.dumps({"streams": out}, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def gzip_bytes(data: bytes) -> bytes:
    """Gzip ``data`` (``Content-Encoding: gzip``)."""
    return _gzip.compress(data, compresslevel=6, mtime=0)


# --------------------------------------------------------------------------- protobuf


def _varint(value: int) -> bytes:
    out = bytearray()
    value &= (1 << 64) - 1
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _tag(field: int, wire: int) -> bytes:
    return _varint(field << 3 | wire)


def _ld(field: int, payload: bytes) -> bytes:
    return _tag(field, 2) + _varint(len(payload)) + payload


def _str(field: int, text: str) -> bytes:
    return _ld(field, text.encode("utf-8"))


def _entry_protobuf(ts: int, line: str, meta: Mapping[str, str]) -> bytes:
    seconds, nanos = divmod(ts, 1_000_000_000)
    stamp = b""
    if seconds:
        stamp += _tag(1, 0) + _varint(seconds)
    if nanos:
        stamp += _tag(2, 0) + _varint(nanos)
    out = _ld(1, stamp) + _str(2, line)
    for key in sorted(meta):
        out += _ld(3, _str(1, key) + _str(2, meta[key]))
    return out


def encode_push_protobuf(streams: Sequence[PreparedStream]) -> bytes:
    """Uncompressed ``logproto.PushRequest`` (labels in Prometheus label-set syntax)."""
    out = bytearray()
    for labels, entries in streams:
        body = _str(1, label_set_string(labels))
        for ts, line, meta in entries:
            body += _ld(2, _entry_protobuf(ts, line, meta))
        out += _ld(1, body)
    return bytes(out)


# --------------------------------------------------------------------------- snappy


def _snappy_literal_block(data: bytes) -> bytes:
    """Snappy block format with literal elements only (valid, uncompressed)."""
    out = bytearray(_varint(len(data)))
    pos = 0
    while pos < len(data):
        chunk = data[pos : pos + 65536]
        n = len(chunk) - 1
        if n < 60:
            out.append(n << 2)
        elif n < 0x100:
            out += bytes((60 << 2, n))
        else:
            out += bytes((61 << 2, n & 0xFF, n >> 8))
        out += chunk
        pos += len(chunk)
    return bytes(out)


def snappy_compress(data: bytes) -> bytes:
    """Snappy **block** format (what Loki's protobuf push expects).

    Uses ``cramjam`` (MIT, install the ``grow-grafana-loki[snappy]`` extra) when it is
    available for real compression; otherwise writes a valid literal-only block in pure Python
    (no size reduction, same result on the server).
    """
    try:
        import cramjam  # noqa: PLC0415 - optional dependency
    except ImportError:
        return _snappy_literal_block(data)
    return bytes(cramjam.snappy.compress_raw(data))


# --------------------------------------------------------------------------- OTLP/JSON


def _any_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        if math.isfinite(value):
            return {"doubleValue": value}
        return {"stringValue": repr(value)}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, Mapping):
        return {"kvlistValue": {"values": _key_values(value)}}
    if isinstance(value, (list, tuple)):
        return {"arrayValue": {"values": [_any_value(v) for v in value]}}
    return {"stringValue": str(value)}


def _key_values(attrs: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [{"key": str(k), "value": _any_value(attrs[k])} for k in sorted(attrs)]


def encode_otlp_logs_json(records: Sequence[LogRecord], scope_name: str) -> bytes:
    """OTLP/JSON ``ExportLogsServiceRequest`` (records grouped by resource)."""
    groups: dict[str, tuple[Mapping[str, Any], list[dict[str, Any]]]] = {}
    for record in records:
        key = json.dumps(_key_values(record.resource), sort_keys=True, default=str)
        ts = str(datetime_to_ns(record.timestamp))
        item: dict[str, Any] = {
            "timeUnixNano": ts,
            "observedTimeUnixNano": (
                str(datetime_to_ns(record.observed_timestamp))
                if record.observed_timestamp is not None
                else ts
            ),
            "severityNumber": int(record.severity),
            "severityText": record.severity_text,
            "body": {"stringValue": record.body},
            "attributes": _key_values(record.attributes),
        }
        if record.trace_id:
            item["traceId"] = record.trace_id
        if record.span_id:
            item["spanId"] = record.span_id
        if record.trace_flags is not None:
            item["flags"] = record.trace_flags
        groups.setdefault(key, (record.resource, []))[1].append(item)
    body = {
        "resourceLogs": [
            {
                "resource": {"attributes": _key_values(resource)},
                "scopeLogs": [{"scope": {"name": scope_name}, "logRecords": items}],
            }
            for resource, items in groups.values()
        ]
    }
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False, default=str).encode()


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    value = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7


def decode_rpc_status(data: bytes) -> str | None:
    """The message (field 2) of a protobuf ``google.rpc.Status`` body, or ``None``.

    Loki's OTLP endpoint answers errors with such a body even for JSON requests.
    """
    pos = 0
    message: str | None = None
    try:
        while pos < len(data):
            key, pos = _read_varint(data, pos)
            field, wire = key >> 3, key & 7
            if wire == 0:
                _, pos = _read_varint(data, pos)
            elif wire == 2:
                length, pos = _read_varint(data, pos)
                chunk = data[pos : pos + length]
                if len(chunk) != length:
                    return None
                pos += length
                if field == 2:
                    message = chunk.decode("utf-8", errors="replace")
            else:
                return None
    except IndexError:
        return None
    return message.strip() or None if message is not None else None


def encode_streams(streams: Sequence[LokiStream], now_ns: int) -> list[PreparedStream]:
    """Resolve entry timestamps (unset = ``now_ns``) into the prepared form."""
    return [
        (
            dict(s.labels),
            [(e.timestamp_ns(now_ns), e.line, dict(e.structured_metadata)) for e in s.entries],
        )
        for s in streams
    ]
