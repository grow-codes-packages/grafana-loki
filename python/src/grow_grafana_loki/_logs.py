"""Logging category on top of Loki: exporters, the query client and record normalization."""

from __future__ import annotations

import base64
import binascii
import datetime as _dt
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ._core import ServiceError, ValidationError
from ._labels import RESERVED_METADATA_KEYS, CardinalityGuard, label_set_string, records_to_streams
from ._logging import (
    AsyncBaseLogQueryClient,
    BaseLogQueryClient,
    ExportResult,
    LogRecord,
    LogSearchResult,
    SearchOrder,
    Severity,
)
from ._models import QueryResult, StreamEntry, TailFrame
from ._time import LokiTime
from .logql import quote_logql

if TYPE_CHECKING:
    from ._calls import PushFormat
    from ._tail import AsyncTailSession, TailSession
    from .client import AsyncLokiClient, LokiClient

__all__ = [
    "AsyncLokiLogExporter",
    "AsyncLokiLogQueryClient",
    "AsyncLokiOtlpExporter",
    "LokiLogExporter",
    "LokiLogQueryClient",
    "LokiOtlpExporter",
    "STREAM_SELECTOR_ALL",
    "decode_cursor",
    "encode_cursor",
    "entry_to_record",
    "to_logql",
]

#: Selector matching every stream (Loki adds ``service_name`` to every stream).
STREAM_SELECTOR_ALL = '{service_name=~".+"}'

#: Stream label → OTel resource key for the well-known keys.
_RESOURCE_KEYS = {
    "service_name": "service.name",
    "service_namespace": "service.namespace",
    "service_version": "service.version",
    "deployment_environment": "deployment.environment",
    "host_name": "host.name",
}


def to_logql(query: str) -> str:
    """``query`` when it is LogQL (starts with ``{``), else a line filter over every stream:
    ``{service_name=~".+"} |= "<text>"``."""
    text = query.strip()
    if not text:
        raise ValidationError("logs.search: query must not be empty")
    if text.startswith("{"):
        return text
    return f"{STREAM_SELECTOR_ALL} |= {quote_logql(text)}"


def encode_cursor(timestamp_ns: int, skip: int) -> str:
    """Opaque cursor ``base64("<last ts ns>:<entries already returned at that ts>")``."""
    raw = f"{timestamp_ns}:{skip}".encode("ascii")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> tuple[int, int]:
    """Inverse of :func:`encode_cursor`; :class:`ValidationError` for a foreign cursor."""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        ts_text, _, skip_text = base64.urlsafe_b64decode(padded).decode("ascii").partition(":")
        ts, skip = int(ts_text), int(skip_text)
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise ValidationError("logs.search: invalid cursor") from None
    if ts < 0 or skip < 0:
        raise ValidationError("logs.search: invalid cursor")
    return ts, skip


def _severity(meta: Mapping[str, str], labels: Mapping[str, str]) -> tuple[Severity, str]:
    text = meta.get("severity_text", "")
    number = meta.get("severity_number")
    if number:
        try:
            return Severity.parse(int(number)), text or Severity.parse(int(number)).name
        except (ValueError, ValidationError):
            pass
    for candidate in (text, meta.get("detected_level", ""), labels.get("level", "")):
        if candidate:
            try:
                return Severity.parse(candidate), text or candidate.upper()
            except ValidationError:
                continue
    return Severity.INFO, text or "INFO"


def entry_to_record(labels: Mapping[str, str], entry: StreamEntry) -> LogRecord:
    """Normalize one Loki entry into a :class:`LogRecord`.

    ``body`` = line; ``attributes`` = parsed labels + structured metadata (minus the SDK's own
    keys); ``resource`` = stream labels with well-known keys mapped back to OTel names
    (``service_name`` → ``service.name``); severity from ``severity_number`` /
    ``severity_text`` / ``detected_level`` / ``level``; trace ids from ``trace_id`` /
    ``span_id`` / ``trace_flags``.
    """
    meta = entry.structured_metadata
    attributes: dict[str, Any] = dict(entry.parsed)
    attributes.update({k: v for k, v in meta.items() if k not in RESERVED_METADATA_KEYS})
    resource = {_RESOURCE_KEYS.get(k, k): v for k, v in labels.items()}
    severity, severity_text = _severity(meta, labels)
    flags: int | None = None
    if meta.get("trace_flags", "").isdigit():
        flags = int(meta["trace_flags"])
    return LogRecord(
        body=entry.line,
        severity=severity,
        severity_text=severity_text,
        timestamp=entry.timestamp,
        attributes=attributes,
        resource=resource,
        trace_id=meta.get("trace_id") or None,
        span_id=meta.get("span_id") or None,
        trace_flags=flags,
    )


def frame_records(frame: TailFrame) -> list[LogRecord]:
    """Records of a tail frame, oldest first."""
    items = [(e.timestamp_ns, s.labels, e) for s in frame.streams for e in s.entries]
    items.sort(key=lambda item: (item[0], label_set_string(item[1]), item[2].line))
    return [entry_to_record(labels, entry) for _, labels, entry in items]


class _SearchPlan:
    """Pagination over ``query_range`` with the composite ``<ts>:<skip>`` cursor."""

    def __init__(
        self,
        query: str,
        start: LokiTime | None,
        end: LokiTime | None,
        limit: int,
        order: SearchOrder,
        cursor: str | None,
    ) -> None:
        if isinstance(limit, bool) or limit < 1:
            raise ValidationError("logs.search: limit must be a positive integer")
        if order not in ("asc", "desc"):
            raise ValidationError("logs.search: order must be 'asc' or 'desc'")
        self.logql = to_logql(query)
        self.limit = limit
        self.order = order
        self.cursor_ts: int | None = None
        self.skip = 0
        self.start = start
        self.end = end
        if cursor:
            self.cursor_ts, self.skip = decode_cursor(cursor)
            if order == "desc":
                self.end = self.cursor_ts + 1  # end is exclusive
            else:
                self.start = self.cursor_ts

    def kwargs(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "end": self.end,
            "limit": self.limit + self.skip,
            "direction": "forward" if self.order == "asc" else "backward",
        }

    def page(self, result: QueryResult) -> LogSearchResult:
        items = [(e.timestamp_ns, s.labels, e) for s in result.streams for e in s.entries]
        sign = 1 if self.order == "asc" else -1
        # time in the requested order; ties by label set, then line (ascending, as everywhere)
        items.sort(key=lambda item: (sign * item[0], label_set_string(item[1]), item[2].line))
        raw_count = len(items)
        if self.cursor_ts is not None and self.skip:
            skipped = 0
            kept = []
            for item in items:
                if item[0] == self.cursor_ts and skipped < self.skip:
                    skipped += 1
                    continue
                kept.append(item)
            items = kept
        page = items[: self.limit]
        next_cursor = None
        if page and raw_count >= self.limit + self.skip:
            last_ts = page[-1][0]
            at_last = sum(1 for item in page if item[0] == last_ts)
            if last_ts == self.cursor_ts:
                at_last += self.skip
            next_cursor = encode_cursor(last_ts, at_last)
        records = [entry_to_record(labels, entry) for _, labels, entry in page]
        return LogSearchResult(records=records, next_cursor=next_cursor)


def _series_streams(series: Sequence[Mapping[str, str]]) -> list[str]:
    return sorted({label_set_string(s) for s in series})


class LokiLogQueryClient(BaseLogQueryClient):
    """:class:`~grow_grafana_loki.LogQueryClient` on Loki (get it from
    :meth:`LokiClient.log_query`).

    ``search`` runs ``query_range`` (categorized labels), merge-sorts all streams and pages with
    an opaque cursor; a query that is not LogQL is treated as free text
    (``{service_name=~".+"} |= "<text>"``). ``tail`` uses the WebSocket tail (live only).
    """

    def __init__(self, client: LokiClient) -> None:
        self._client = client

    def search(
        self,
        query: str,
        *,
        start: _dt.datetime | None = None,
        end: _dt.datetime | None = None,
        limit: int = 100,
        order: SearchOrder = "desc",
        cursor: str | None = None,
    ) -> LogSearchResult:
        plan = _SearchPlan(query, start, end, limit, order, cursor)
        return plan.page(self._client.query_range(plan.logql, **plan.kwargs()))

    def tail(
        self,
        query: str,
        on_record: Callable[[LogRecord], None],
        *,
        interval_s: float = 1.0,
        start: _dt.datetime | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> TailSession:
        """Stream new records (WebSocket; ``interval_s`` is unused, Loki pushes frames)."""

        def on_frame(frame: TailFrame) -> None:
            for record in frame_records(frame):
                on_record(record)

        return self._client.tail(to_logql(query), on_frame, start=start, on_error=on_error)

    def list_streams(self) -> list[str]:
        """Label sets of the streams of the last hour, as ``{a="b", c="d"}`` strings."""
        return _series_streams(self._client.series([STREAM_SELECTOR_ALL]))

    def list_labels(self) -> list[str]:
        """Label names of the last hour."""
        return self._client.labels()


class AsyncLokiLogQueryClient(AsyncBaseLogQueryClient):
    """Async :class:`LokiLogQueryClient` (from :meth:`AsyncLokiClient.log_query`)."""

    def __init__(self, client: AsyncLokiClient) -> None:
        self._client = client

    async def search(
        self,
        query: str,
        *,
        start: _dt.datetime | None = None,
        end: _dt.datetime | None = None,
        limit: int = 100,
        order: SearchOrder = "desc",
        cursor: str | None = None,
    ) -> LogSearchResult:
        plan = _SearchPlan(query, start, end, limit, order, cursor)
        return plan.page(await self._client.query_range(plan.logql, **plan.kwargs()))

    async def tail(
        self,
        query: str,
        on_record: Callable[[LogRecord], None],
        *,
        interval_s: float = 1.0,
        start: _dt.datetime | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> AsyncTailSession:
        """Stream new records (WebSocket; ``interval_s`` is unused)."""

        def on_frame(frame: TailFrame) -> None:
            for record in frame_records(frame):
                on_record(record)

        return await self._client.tail(to_logql(query), on_frame, start=start, on_error=on_error)

    async def list_streams(self) -> list[str]:
        """Label sets of the streams of the last hour, as ``{a="b", c="d"}`` strings."""
        return _series_streams(await self._client.series([STREAM_SELECTOR_ALL]))

    async def list_labels(self) -> list[str]:
        """Label names of the last hour."""
        return await self._client.labels()


def _failure(records: Sequence[LogRecord], err: ServiceError) -> ExportResult:
    return ExportResult(accepted=0, rejected=len(records), error=err)


class LokiLogExporter:
    """:class:`~grow_grafana_loki.LogExporter` writing to ``POST /loki/api/v1/push``.

    Records are grouped into streams by the cardinality guard (:func:`records_to_streams`).
    Loki accepts or rejects a whole push, so a result is all accepted or all rejected.
    """

    def __init__(
        self,
        client: LokiClient,
        *,
        format: PushFormat = "json",  # noqa: A002 - option name shared with the other packages
        guard: CardinalityGuard | None = None,
    ) -> None:
        self._client = client
        self._format: PushFormat = format
        self._guard = guard or client.guard

    def export(self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float) -> ExportResult:
        if not records:
            return ExportResult(accepted=0)
        try:
            self._client.push(
                records_to_streams(records, self._guard),
                format=self._format,
                gzip=gzip,
                timeout_s=timeout_s,
            )
        except ServiceError as err:
            return _failure(records, err)
        return ExportResult(accepted=len(records))

    def shutdown(self) -> None:
        """No-op (the client owns the connections)."""


class AsyncLokiLogExporter:
    """Async :class:`LokiLogExporter`."""

    def __init__(
        self,
        client: AsyncLokiClient,
        *,
        format: PushFormat = "json",  # noqa: A002
        guard: CardinalityGuard | None = None,
    ) -> None:
        self._client = client
        self._format: PushFormat = format
        self._guard = guard or client.guard

    async def export(
        self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float
    ) -> ExportResult:
        if not records:
            return ExportResult(accepted=0)
        try:
            await self._client.push(
                records_to_streams(records, self._guard),
                format=self._format,
                gzip=gzip,
                timeout_s=timeout_s,
            )
        except ServiceError as err:
            return _failure(records, err)
        return ExportResult(accepted=len(records))

    async def shutdown(self) -> None:
        """No-op (the client owns the connections)."""


class LokiOtlpExporter:
    """:class:`~grow_grafana_loki.LogExporter` writing OTLP/JSON to ``POST /otlp/v1/logs``.

    Loki then decides which resource attributes become labels (its OTLP defaults) and stores
    the rest as structured metadata.
    """

    def __init__(self, client: LokiClient) -> None:
        self._client = client

    def export(self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float) -> ExportResult:
        if not records:
            return ExportResult(accepted=0)
        try:
            self._client.push_otlp(records, gzip=gzip, timeout_s=timeout_s)
        except ServiceError as err:
            return _failure(records, err)
        return ExportResult(accepted=len(records))

    def shutdown(self) -> None:
        """No-op (the client owns the connections)."""


class AsyncLokiOtlpExporter:
    """Async :class:`LokiOtlpExporter`."""

    def __init__(self, client: AsyncLokiClient) -> None:
        self._client = client

    async def export(
        self, records: Sequence[LogRecord], *, gzip: bool, timeout_s: float
    ) -> ExportResult:
        if not records:
            return ExportResult(accepted=0)
        try:
            await self._client.push_otlp(records, gzip=gzip, timeout_s=timeout_s)
        except ServiceError as err:
            return _failure(records, err)
        return ExportResult(accepted=len(records))

    async def shutdown(self) -> None:
        """No-op (the client owns the connections)."""
