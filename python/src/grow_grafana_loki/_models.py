"""Result models of the Loki query, status and tail APIs, and their decoders."""

from __future__ import annotations

import dataclasses
import datetime as _dt
import math
from collections.abc import Mapping
from typing import Any, Literal

from ._core import ValidationError
from ._time import ns_to_datetime

__all__ = [
    "BuildInfo",
    "Direction",
    "DroppedEntry",
    "IndexStats",
    "Point",
    "QueryResult",
    "Sample",
    "SampleSeries",
    "StreamEntry",
    "StreamResult",
    "TailFrame",
]

Direction = Literal["backward", "forward"]
"""Sort order of log query results (``backward`` = newest first, Loki's default)."""

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


@dataclasses.dataclass(frozen=True)
class StreamEntry:
    """One log line returned by Loki.

    Attributes:
        timestamp_ns: Unix nanoseconds (lossless).
        line: the log line.
        structured_metadata: metadata of the entry (categorized responses, the SDK default).
        parsed: labels extracted by parsers such as ``| json`` (categorized responses).
    """

    timestamp_ns: int
    line: str
    structured_metadata: Mapping[str, str] = dataclasses.field(default_factory=dict)
    parsed: Mapping[str, str] = dataclasses.field(default_factory=dict)

    @property
    def timestamp(self) -> _dt.datetime:
        """The timestamp as an aware UTC ``datetime`` (microsecond precision)."""
        return ns_to_datetime(self.timestamp_ns)


@dataclasses.dataclass(frozen=True)
class StreamResult:
    """One stream of a log query result or tail frame.

    ``labels`` are index labels only (the SDK asks Loki to categorize labels); structured
    metadata and parsed labels come per entry.
    """

    labels: Mapping[str, str]
    entries: list[StreamEntry]


@dataclasses.dataclass(frozen=True)
class Point:
    """One metric sample value (``timestamp`` from Loki's float seconds)."""

    timestamp: _dt.datetime
    value: float


@dataclasses.dataclass(frozen=True)
class Sample:
    """One series of an instant vector."""

    metric: Mapping[str, str]
    value: Point


@dataclasses.dataclass(frozen=True)
class SampleSeries:
    """One series of a range matrix."""

    metric: Mapping[str, str]
    values: list[Point]


@dataclasses.dataclass(frozen=True)
class QueryResult:
    """Decoded ``data`` of a query response.

    Exactly one of ``streams`` (log queries), ``vector`` (instant metric queries, volume),
    ``matrix`` (range metric queries, volume range) or ``scalar`` is filled, per
    ``result_type``. ``stats`` is Loki's raw statistics object (may be empty).
    """

    result_type: str = ""
    streams: list[StreamResult] = dataclasses.field(default_factory=list)
    vector: list[Sample] = dataclasses.field(default_factory=list)
    matrix: list[SampleSeries] = dataclasses.field(default_factory=list)
    scalar: Point | None = None
    encoding_flags: list[str] = dataclasses.field(default_factory=list)
    stats: Mapping[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class IndexStats:
    """Index statistics (TSDB index only: zero until the ingester flushed its chunks; bytes
    are tracked per chunk in KB, so small test chunks report 0)."""

    streams: int = 0
    chunks: int = 0
    bytes: int = 0
    entries: int = 0


@dataclasses.dataclass(frozen=True)
class BuildInfo:
    """Loki build information (``/loki/api/v1/status/buildinfo``)."""

    version: str = ""
    revision: str = ""
    branch: str = ""
    build_user: str = ""
    build_date: str = ""
    go_version: str = ""


@dataclasses.dataclass(frozen=True)
class DroppedEntry:
    """An entry Loki dropped because the tailer fell behind."""

    labels: Mapping[str, str]
    timestamp: _dt.datetime


@dataclasses.dataclass(frozen=True)
class TailFrame:
    """One message of a live tail."""

    streams: list[StreamResult] = dataclasses.field(default_factory=list)
    dropped_entries: list[DroppedEntry] = dataclasses.field(default_factory=list)


# --------------------------------------------------------------------------- decoders


def _invalid(what: str) -> ValidationError:
    return ValidationError(f"invalid Loki response: {what}")


def string_map(value: Any) -> dict[str, str]:
    """A string → string mapping from JSON (non-string values are stringified)."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise _invalid("expected an object of strings")
    return {str(k): v if isinstance(v, str) else str(v) for k, v in value.items()}


def decode_entry(value: Any) -> StreamEntry:
    """``["<ns>", "<line>"]`` or ``["<ns>", "<line>", {"structuredMetadata":…, "parsed":…}]``."""
    if not isinstance(value, list) or len(value) < 2:
        raise _invalid("stream value must be [ts, line, ...]")
    try:
        ts = int(value[0])
    except (TypeError, ValueError):
        raise _invalid("stream timestamp is not an integer") from None
    line = value[1] if isinstance(value[1], str) else str(value[1])
    meta: dict[str, str] = {}
    parsed: dict[str, str] = {}
    if len(value) > 2 and isinstance(value[2], Mapping):
        meta = string_map(value[2].get("structuredMetadata"))
        parsed = string_map(value[2].get("parsed"))
    return StreamEntry(ts, line, meta, parsed)


def decode_streams(value: Any) -> list[StreamResult]:
    """Decode a ``streams`` result list."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise _invalid("streams must be a list")
    out = []
    for item in value:
        if not isinstance(item, Mapping):
            raise _invalid("stream must be an object")
        values = item.get("values") or []
        if not isinstance(values, list):
            raise _invalid("stream values must be a list")
        out.append(StreamResult(string_map(item.get("stream")), [decode_entry(v) for v in values]))
    return out


def decode_point(value: Any) -> Point:
    """``[<float seconds>, "<value>"]``."""
    if not isinstance(value, list) or len(value) != 2:
        raise _invalid("sample must be [time, value]")
    try:
        seconds = float(value[0])
        number = float(value[1])
    except (TypeError, ValueError):
        raise _invalid("sample time/value is not a number") from None
    if not math.isfinite(seconds):
        raise _invalid("sample time is not finite")
    stamp = _EPOCH + _dt.timedelta(microseconds=round(seconds * 1_000_000))
    return Point(stamp, number)


def decode_query_data(data: Any) -> QueryResult:
    """Decode the ``data`` object of a query / volume response (missing = empty result)."""
    if data is None:
        return QueryResult()
    if not isinstance(data, Mapping):
        raise _invalid("data must be an object")
    result_type = str(data.get("resultType") or "")
    flags = [str(f) for f in data.get("encodingFlags") or []]
    stats = data.get("stats")
    stats = dict(stats) if isinstance(stats, Mapping) else {}
    result = data.get("result")
    kwargs: dict[str, Any] = {}
    if result is not None:
        if result_type == "streams":
            kwargs["streams"] = decode_streams(result)
        elif result_type == "vector":
            if not isinstance(result, list):
                raise _invalid("vector must be a list")
            kwargs["vector"] = [
                Sample(string_map(_obj(i).get("metric")), decode_point(_obj(i).get("value")))
                for i in result
            ]
        elif result_type == "matrix":
            if not isinstance(result, list):
                raise _invalid("matrix must be a list")
            kwargs["matrix"] = [
                SampleSeries(
                    string_map(_obj(i).get("metric")),
                    [decode_point(p) for p in _obj(i).get("values") or []],
                )
                for i in result
            ]
        elif result_type == "scalar":
            kwargs["scalar"] = decode_point(result)
    return QueryResult(result_type, encoding_flags=flags, stats=stats, **kwargs)


def _obj(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid("expected an object")
    return value


def decode_string_list(body: Any) -> list[str]:
    """``{"status":"success","data":[...]}``; a missing ``data`` means an empty list."""
    data = _obj(body).get("data")
    if data is None:
        return []
    if not isinstance(data, list):
        raise _invalid("data must be a list")
    return [str(v) for v in data]


def decode_series(body: Any) -> list[dict[str, str]]:
    """Series response: a list of label sets (missing ``data`` = empty)."""
    data = _obj(body).get("data")
    if data is None:
        return []
    if not isinstance(data, list):
        raise _invalid("data must be a list")
    return [string_map(item) for item in data]


def decode_index_stats(body: Any) -> IndexStats:
    """``{"streams":n,"chunks":n,"bytes":n,"entries":n}`` (no envelope)."""
    doc = _obj(body)
    try:
        return IndexStats(
            int(doc.get("streams") or 0),
            int(doc.get("chunks") or 0),
            int(doc.get("bytes") or 0),
            int(doc.get("entries") or 0),
        )
    except (TypeError, ValueError):
        raise _invalid("index stats must be numbers") from None


def decode_build_info(body: Any) -> BuildInfo:
    """Build info object (missing fields become ``""``)."""
    doc = _obj(body)

    def text(key: str) -> str:
        value = doc.get(key)
        return value if isinstance(value, str) else ""

    return BuildInfo(
        text("version"),
        text("revision"),
        text("branch"),
        text("buildUser"),
        text("buildDate"),
        text("goVersion"),
    )


def decode_tail_frame(body: Any) -> TailFrame:
    """One tail frame: ``{"streams":[…],"dropped_entries":[{"labels":…,"timestamp":"<ns>"}]}``."""
    doc = _obj(body)
    dropped = []
    for item in doc.get("dropped_entries") or []:
        obj = _obj(item)
        try:
            stamp = ns_to_datetime(int(obj.get("timestamp") or 0))
        except (TypeError, ValueError):
            raise _invalid("dropped entry timestamp is not an integer") from None
        dropped.append(DroppedEntry(string_map(obj.get("labels")), stamp))
    return TailFrame(decode_streams(doc.get("streams")), dropped)
