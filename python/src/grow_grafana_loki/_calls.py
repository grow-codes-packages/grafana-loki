"""Request builders and response decoders shared by the sync and async clients.

Each builder validates its input client-side and returns a :class:`Call`: the transport
:class:`Request`, a decoder for the response and an optional error translator. The clients
only send calls; this keeps both clients behaviourally identical.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Generic, Literal, TypeVar
from urllib.parse import quote, urlencode

from ._core import (
    HealthStatus,
    LokiError,
    NotFoundError,
    Request,
    Response,
    ServiceError,
    UnavailableError,
    UnsupportedError,
    ValidationError,
    error_from_status,
    redact_text,
    truncate,
)
from ._labels import CardinalityGuard, LokiStream, is_valid_label_name, validate_label
from ._logging import LogRecord
from ._models import (
    BuildInfo,
    Direction,
    IndexStats,
    QueryResult,
    SampleSeries,
    decode_build_info,
    decode_index_stats,
    decode_query_data,
    decode_series,
    decode_string_list,
)
from ._time import LokiTime, datetime_to_ns, duration_param, optional_ns
from ._wire import (
    decode_rpc_status,
    encode_otlp_logs_json,
    encode_push_json,
    encode_push_protobuf,
    encode_streams,
    gzip_bytes,
    snappy_compress,
)
from .config import LokiConfig

__all__ = [
    "Call",
    "HEADER_ENCODING_FLAGS",
    "HEADER_TENANT",
    "INGESTER_SHUTTING_DOWN",
    "OTLP_SCOPE_NAME",
    "PushFormat",
    "STATUS_INGESTION_BLOCKED",
    "is_log_query",
    "map_loki_error",
]

T = TypeVar("T")

PushFormat = Literal["json", "protobuf"]
"""Wire format of ``push``: ``json`` (default) or ``protobuf`` (Snappy-compressed)."""

#: Header that carries the tenant id (Loki multi-tenancy).
HEADER_TENANT = "X-Scope-OrgID"
#: Header that asks Loki to categorize labels in query responses and tail frames.
HEADER_ENCODING_FLAGS = "X-Loki-Response-Encoding-Flags"
#: Loki's push status when ingestion is administratively blocked.
STATUS_INGESTION_BLOCKED = 260
#: Loki's text when the WAL disk is >= 90 % full or the ingester is stopping.
INGESTER_SHUTTING_DOWN = "Ingester is shutting down"
#: Instrumentation scope name used in OTLP payloads.
OTLP_SCOPE_NAME = "grow-codes-grafana-loki"

PATH_READY = "/ready"
PATH_BUILD_INFO = "/loki/api/v1/status/buildinfo"
PATH_PUSH = "/loki/api/v1/push"
PATH_QUERY = "/loki/api/v1/query"
PATH_QUERY_RANGE = "/loki/api/v1/query_range"
PATH_LABELS = "/loki/api/v1/labels"
PATH_SERIES = "/loki/api/v1/series"
PATH_INDEX_STATS = "/loki/api/v1/index/stats"
PATH_VOLUME = "/loki/api/v1/index/volume"
PATH_VOLUME_RANGE = "/loki/api/v1/index/volume_range"
PATH_FORMAT_QUERY = "/loki/api/v1/format_query"
PATH_OTLP_LOGS = "/otlp/v1/logs"

_CATEGORIZE = {HEADER_ENCODING_FLAGS: "categorize-labels"}
_DIRECTIONS = ("backward", "forward")


@dataclasses.dataclass(frozen=True)
class Call(Generic[T]):
    """One prepared API call: request, decoder and error translator."""

    request: Request
    decode: Callable[[Response], T]
    translate: Callable[[ServiceError], ServiceError] | None = None

    def fail(self, err: ServiceError) -> ServiceError:
        """The error to raise for ``err`` (Loki mappings + call specific translation)."""
        err = map_loki_error(err)
        return self.translate(err) if self.translate is not None else err


def map_loki_error(err: ServiceError) -> ServiceError:
    """Loki-specific mapping: "Ingester is shutting down" (WAL disk >= 90 % full or ingester
    stopping, HTTP 500/503) is a retryable :class:`UnavailableError`."""
    if INGESTER_SHUTTING_DOWN in err.message:
        message = err.message
        if "WAL" not in message:
            message += " (the Loki ingester is stopping or its WAL disk is at least 90 % full)"
        mapped = UnavailableError(
            message,
            http_status=err.http_status,
            request_id=err.request_id,
            retryable=True,
            retry_after=err.retry_after,
        )
        mapped.__cause__ = err.__cause__
        return mapped
    return err


def is_log_query(logql: str) -> bool:
    """Whether ``logql`` is a log query (starts with a ``{...}`` stream selector)."""
    return logql.strip().startswith("{")


def _json(resp: Response) -> Any:
    return resp.json()


def _check_direction(op: str, direction: Direction | None) -> None:
    if direction is not None and direction not in _DIRECTIONS:
        raise ValidationError(f"{op}: direction must be 'backward' or 'forward'")


def _check_limit(op: str, limit: int | None) -> None:
    if limit is not None and (isinstance(limit, bool) or limit < 1):
        raise ValidationError(f"{op}: limit must be a positive integer")


def _check_query(op: str, query: str) -> None:
    if not isinstance(query, str) or not query.strip():
        raise ValidationError(f"{op}: query must not be empty")


def _window(
    op: str,
    params: dict[str, Any],
    start: LokiTime | None,
    end: LokiTime | None,
    since_s: float | _dt.timedelta | str | None = None,
) -> None:
    start_ns = optional_ns(start, "start")
    end_ns = optional_ns(end, "end")
    if start_ns is not None and end_ns is not None and int(end_ns) <= int(start_ns):
        raise ValidationError(f"{op}: end must be after start")
    params["start"] = start_ns
    params["end"] = end_ns
    if start_ns is None and since_s is not None:
        params["since"] = duration_param(since_s, "since")


def _result_call(op: str, path: str, params: dict[str, Any]) -> Call[QueryResult]:
    def decode(resp: Response) -> QueryResult:
        body = resp.json()
        if not isinstance(body, Mapping):
            raise ValidationError(f"{op}: invalid response (HTTP {resp.status})")
        return decode_query_data(body.get("data"))

    return Call(Request("GET", path, operation=op, params=params, headers=_CATEGORIZE), decode)


# --------------------------------------------------------------------------- status


def health_call(config: LokiConfig, known_secrets: list[str]) -> Call[HealthStatus]:
    """``GET /ready``: 200 → healthy ``ok``; otherwise Loki's text (e.g. ``Ingester not ready``)."""

    def decode(resp: Response) -> HealthStatus:
        text = truncate(redact_text(_first_line(resp.text), known_secrets), 300)
        details = {"httpStatus": str(resp.status)}
        if text:
            details["message"] = text
        if resp.status == 200:
            return HealthStatus(True, "ok", details)
        return HealthStatus(False, text or "unhealthy", details)

    req = Request(
        "GET",
        PATH_READY,
        operation="health",
        headers=config.auth_headers(),
        raise_for_status=False,
        max_retries=0,
    )
    return Call(req, decode)


def ready_call(config: LokiConfig, known_secrets: list[str]) -> Call[bool]:
    """``GET /ready``: True for 200, False for 503 (starting), error otherwise."""

    def decode(resp: Response) -> bool:
        if resp.status == 200:
            return True
        if resp.status == 503:
            return False
        raise map_loki_error(
            error_from_status(
                resp.status,
                "ready",
                content=resp.content,
                headers=resp.headers,
                known_secrets=known_secrets,
            )
        )

    req = Request(
        "GET",
        PATH_READY,
        operation="ready",
        headers=config.auth_headers(),
        raise_for_status=False,
        max_retries=0,
    )
    return Call(req, decode)


def ping_call(config: LokiConfig) -> Call[None]:
    """``GET /loki/api/v1/labels?since=5m`` (cheap, authenticated, touches the querier)."""
    req = Request(
        "GET", PATH_LABELS, operation="ping", params={"since": "5m"}, headers=config.auth_headers()
    )
    return Call(req, lambda resp: None)


def build_info_call(config: LokiConfig) -> Call[BuildInfo]:
    """``GET /loki/api/v1/status/buildinfo``; 404 → :class:`UnsupportedError`."""

    def translate(err: ServiceError) -> ServiceError:
        if isinstance(err, NotFoundError):
            return UnsupportedError(
                "build_info: the server exposes no build information",
                http_status=err.http_status,
                request_id=err.request_id,
            )
        return err

    req = Request("GET", PATH_BUILD_INFO, operation="build_info", headers=config.auth_headers())
    return Call(req, lambda resp: decode_build_info(resp.json()), translate)


# --------------------------------------------------------------------------- push


def _validate_stream(index: int, stream: LokiStream) -> None:
    try:
        for name, value in stream.labels.items():
            validate_label(name, value)
    except ValidationError as err:
        raise ValidationError(f"push: stream {index}: {err.message}") from None
    for j, entry in enumerate(stream.entries):
        if not isinstance(entry.line, str):
            raise ValidationError(f"push: stream {index}: entry {j}: line must be a string")
        for key, value in entry.structured_metadata.items():
            if not isinstance(key, str) or not key:
                raise ValidationError(
                    f"push: stream {index}: entry {j}: structured metadata names must be "
                    "non-empty strings"
                )
            if not isinstance(value, str):
                raise ValidationError(
                    f"push: stream {index}: entry {j}: structured metadata value of {key!r} "
                    "must be a string"
                )


def push_call(
    config: LokiConfig,
    guard: CardinalityGuard,
    streams: Sequence[LokiStream],
    *,
    format: PushFormat = "json",  # noqa: A002 - mirrors the option name of the other packages
    gzip: bool = False,
    timeout_s: float | None = None,
) -> Call[None] | None:
    """Validate, guard and encode ``streams``; ``None`` when there is nothing to send."""
    if not streams:
        return None
    if format not in ("json", "protobuf"):
        raise ValidationError(f"push: unknown push format {format!r}")
    prepared_streams = []
    for i, original in enumerate(streams):
        stream = original if config.disable_label_guard else guard.apply(original)
        _validate_stream(i, stream)
        prepared_streams.append(stream)
    now_ns = datetime_to_ns(_dt.datetime.now(_dt.timezone.utc))
    prepared = encode_streams(prepared_streams, now_ns)
    headers = config.auth_headers()
    if format == "protobuf":
        body = snappy_compress(encode_push_protobuf(prepared))
        headers["Content-Type"] = "application/x-protobuf"
    else:
        body = encode_push_json(prepared)
        headers["Content-Type"] = "application/json"
        if gzip:
            body = gzip_bytes(body)
            headers["Content-Encoding"] = "gzip"
    req = Request(
        "POST",
        PATH_PUSH,
        operation="push",
        content=body,
        headers=headers,
        idempotent=True,  # Loki drops exact duplicates, so replaying the payload is safe
        timeout=timeout_s,
    )
    return Call(req, _push_decode)


def _push_decode(resp: Response) -> None:
    if resp.status == STATUS_INGESTION_BLOCKED:
        raise LokiError(
            "push: ingestion is blocked by the Loki administrator (HTTP 260)",
            http_status=resp.status,
            request_id=resp.request_id,
            retryable=False,
        )


def push_otlp_call(
    config: LokiConfig,
    records: Sequence[LogRecord],
    known_secrets: list[str],
    *,
    gzip: bool = False,
    timeout_s: float | None = None,
) -> Call[None] | None:
    """OTLP/JSON logs to ``POST /otlp/v1/logs``; ``None`` when there is nothing to send."""
    if not records:
        return None
    body = encode_otlp_logs_json(records, OTLP_SCOPE_NAME)
    headers = config.auth_headers()
    headers["Content-Type"] = "application/json"
    if gzip:
        body = gzip_bytes(body)
        headers["Content-Encoding"] = "gzip"

    def decode(resp: Response) -> None:
        if resp.status < 400:
            return
        content = resp.content
        ctype = resp.headers.get("content-type", "")
        if "protobuf" in ctype or "octet-stream" in ctype:
            text = decode_rpc_status(content)
            content, ctype = (text or "").encode(), "text/plain"
        headers_out = resp.headers.copy()
        headers_out["content-type"] = ctype
        raise map_loki_error(
            error_from_status(
                resp.status,
                "push_otlp",
                content=content,
                headers=headers_out,
                known_secrets=known_secrets,
            )
        )

    req = Request(
        "POST",
        PATH_OTLP_LOGS,
        operation="push_otlp",
        content=body,
        headers=headers,
        idempotent=True,
        timeout=timeout_s,
        raise_for_status=False,
    )
    return Call(req, decode)


# --------------------------------------------------------------------------- query


def query_call(
    config: LokiConfig,
    logql: str,
    *,
    time: LokiTime | None = None,
    limit: int | None = None,
    direction: Direction | None = None,
) -> Call[QueryResult]:
    """Instant query; a log selector is rejected (Loki answers it only as a range query)."""
    _check_query("query", logql)
    if is_log_query(logql):
        raise ValidationError(
            "query: log queries are not supported as instant queries; use query_range"
        )
    _check_limit("query", limit)
    _check_direction("query", direction)
    params = {
        "query": logql,
        "time": optional_ns(time, "time"),
        "limit": limit,
        "direction": direction,
    }
    call = _result_call("query", PATH_QUERY, params)
    return _with_auth(call, config)


def query_range_call(
    config: LokiConfig,
    logql: str,
    *,
    start: LokiTime | None = None,
    end: LokiTime | None = None,
    since_s: float | _dt.timedelta | str | None = None,
    step_s: float | _dt.timedelta | str | None = None,
    interval_s: float | _dt.timedelta | str | None = None,
    limit: int | None = None,
    direction: Direction | None = None,
) -> Call[QueryResult]:
    """Range query (log queries → streams, metric queries → matrix)."""
    _check_query("query_range", logql)
    _check_limit("query_range", limit)
    _check_direction("query_range", direction)
    params: dict[str, Any] = {"query": logql}
    _window("query_range", params, start, end, since_s)
    if step_s is not None:
        params["step"] = duration_param(step_s, "step")
    if interval_s is not None:
        params["interval"] = duration_param(interval_s, "interval")
    params["limit"] = limit
    params["direction"] = direction
    return _with_auth(_result_call("query_range", PATH_QUERY_RANGE, params), config)


def _with_auth(call: Call[T], config: LokiConfig) -> Call[T]:
    headers = {**config.auth_headers(), **(call.request.headers or {})}
    return dataclasses.replace(call, request=dataclasses.replace(call.request, headers=headers))


def labels_call(
    config: LokiConfig,
    *,
    start: LokiTime | None = None,
    end: LokiTime | None = None,
    since_s: float | _dt.timedelta | str | None = None,
    query: str | None = None,
) -> Call[list[str]]:
    """Label names (structured metadata keys are not labels)."""
    params: dict[str, Any] = {}
    _window("labels", params, start, end, since_s)
    params["query"] = query or None
    req = Request("GET", PATH_LABELS, operation="labels", params=params)
    return _with_auth(Call(req, lambda resp: decode_string_list(resp.json())), config)


def label_values_call(
    config: LokiConfig,
    name: str,
    *,
    start: LokiTime | None = None,
    end: LokiTime | None = None,
    since_s: float | _dt.timedelta | str | None = None,
    query: str | None = None,
) -> Call[list[str]]:
    """Values of one label; the name is validated client-side (Loki answers bad names with [])."""
    if not isinstance(name, str) or not is_valid_label_name(name):
        raise ValidationError(f"label_values: invalid label name {name!r}")
    params: dict[str, Any] = {}
    _window("label_values", params, start, end, since_s)
    params["query"] = query or None
    path = f"/loki/api/v1/label/{quote(name, safe='')}/values"
    req = Request("GET", path, operation=f"label_values {name}", params=params)
    return _with_auth(Call(req, lambda resp: decode_string_list(resp.json())), config)


def series_call(
    config: LokiConfig,
    matchers: Sequence[str] | str,
    *,
    start: LokiTime | None = None,
    end: LokiTime | None = None,
    since_s: float | _dt.timedelta | str | None = None,
) -> Call[list[dict[str, str]]]:
    """Label sets of the streams matching any selector (POST form with ``match[]``)."""
    items = [matchers] if isinstance(matchers, str) else list(matchers)
    if not items or any(not isinstance(m, str) or not m.strip() for m in items):
        raise ValidationError("series: at least one non-empty matcher is required")
    params: dict[str, Any] = {}
    _window("series", params, start, end, since_s)
    form: list[tuple[str, str]] = [("match[]", m) for m in items]
    form += [(k, str(v)) for k, v in params.items() if v is not None]
    headers = {**config.auth_headers(), "Content-Type": "application/x-www-form-urlencoded"}
    req = Request(
        "POST",
        PATH_SERIES,
        operation="series",
        content=urlencode(form).encode("ascii"),
        headers=headers,
        idempotent=True,
    )
    return Call(req, lambda resp: decode_series(resp.json()))


def index_stats_call(
    config: LokiConfig,
    query: str,
    *,
    start: LokiTime | None = None,
    end: LokiTime | None = None,
) -> Call[IndexStats]:
    """Index statistics for a selector (zero until chunks are flushed)."""
    _check_query("index_stats", query)
    params: dict[str, Any] = {"query": query}
    _window("index_stats", params, start, end)
    req = Request("GET", PATH_INDEX_STATS, operation="index_stats", params=params)
    return _with_auth(Call(req, lambda resp: decode_index_stats(resp.json())), config)


def volume_call(
    config: LokiConfig,
    query: str,
    *,
    ranged: bool,
    start: LokiTime | None = None,
    end: LokiTime | None = None,
    step_s: float | _dt.timedelta | str | None = None,
    limit: int | None = None,
    target_labels: Sequence[str] | None = None,
    aggregate_by: Literal["series", "labels"] | None = None,
) -> Call[QueryResult]:
    """Volume (vector) or volume range (matrix) of ingested bytes."""
    op = "volume_range" if ranged else "volume"
    _check_query(op, query)
    _check_limit(op, limit)
    if aggregate_by is not None and aggregate_by not in ("series", "labels"):
        raise ValidationError(f"{op}: aggregate_by must be 'series' or 'labels'")
    params: dict[str, Any] = {"query": query}
    _window(op, params, start, end)
    if ranged and step_s is not None:
        params["step"] = duration_param(step_s, "step")
    params["limit"] = limit
    if target_labels:
        params["targetLabels"] = ",".join(target_labels)
    params["aggregateBy"] = aggregate_by
    path = PATH_VOLUME_RANGE if ranged else PATH_VOLUME
    call = _result_call(op, path, params)
    if ranged:
        call = dataclasses.replace(call, decode=_as_matrix(call.decode))
    return _with_auth(call, config)


def _as_matrix(decode: Callable[[Response], QueryResult]) -> Callable[[Response], QueryResult]:
    """Loki answers ``volume_range`` with a vector when every point falls into one step;
    normalize that to a one-point matrix so callers always get ``matrix``."""

    def wrapped(resp: Response) -> QueryResult:
        result = decode(resp)
        if result.result_type != "vector":
            return result
        matrix = [SampleSeries(s.metric, [s.value]) for s in result.vector]
        return dataclasses.replace(result, result_type="matrix", vector=[], matrix=matrix)

    return wrapped


def format_query_call(config: LokiConfig, logql: str) -> Call[str]:
    """Server-side parse + pretty-print of a LogQL query."""
    _check_query("format_query", logql)

    def decode(resp: Response) -> str:
        body = resp.json()
        data = body.get("data") if isinstance(body, Mapping) else None
        if not isinstance(data, str):
            raise ValidationError(f"format_query: invalid response (HTTP {resp.status})")
        return data

    req = Request("GET", PATH_FORMAT_QUERY, operation="format_query", params={"query": logql})
    return _with_auth(Call(req, decode), config)


def _first_line(text: str) -> str:
    return text.strip().split("\n", 1)[0].strip()
