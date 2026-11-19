"""Grafana Loki clients (sync and async), service-owned.

Service functions hang off the client directly: :meth:`~LokiClient.push`,
:meth:`~LokiClient.push_otlp`, :meth:`~LokiClient.query`, :meth:`~LokiClient.query_range`,
:meth:`~LokiClient.labels`, :meth:`~LokiClient.label_values`, :meth:`~LokiClient.series`,
:meth:`~LokiClient.tail`, :meth:`~LokiClient.index_stats`, :meth:`~LokiClient.volume`,
:meth:`~LokiClient.volume_range`, :meth:`~LokiClient.format_query`, :meth:`~LokiClient.ready`
and :meth:`~LokiClient.build_info`. The logging category is reached through
:meth:`~LokiClient.log_client` / :meth:`~LokiClient.otlp_log_client` (``LogClient``) and
:meth:`~LokiClient.log_query` (``LogQueryClient``). :meth:`~LokiClient.with_tenant` returns a
client for another tenant sharing the connection pool.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as _dt
import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import Any, Literal, TypeVar

import httpx
from websockets.asyncio.client import connect as _aconnect
from websockets.sync.client import connect as _connect

from ._calls import (
    HEADER_ENCODING_FLAGS,
    Call,
    PushFormat,
    build_info_call,
    format_query_call,
    health_call,
    index_stats_call,
    is_log_query,
    label_values_call,
    labels_call,
    ping_call,
    push_call,
    push_otlp_call,
    query_call,
    query_range_call,
    ready_call,
    series_call,
    volume_call,
)
from ._core import (
    AsyncHttpTransport,
    CancelledError,
    CancelToken,
    ConfigError,
    HealthStatus,
    HttpTransport,
    Request,
    ResourceRegistry,
    Response,
    ServiceError,
    TimeoutError,
    UnavailableError,
    ValidationError,
    user_agent,
)
from ._labels import CardinalityGuard, LokiStream
from ._logging import (
    AsyncBatchingLogClient,
    BatchingLogClient,
    BatchOptions,
    LogRecord,
    RedactionProcessor,
)
from ._logging._client import _DEFAULT_REDACTION
from ._logs import (
    AsyncLokiLogExporter,
    AsyncLokiLogQueryClient,
    AsyncLokiOtlpExporter,
    LokiLogExporter,
    LokiLogQueryClient,
    LokiOtlpExporter,
)
from ._models import BuildInfo, Direction, IndexStats, QueryResult, TailFrame
from ._tail import (
    MAX_TAIL_DELAY_S,
    AsyncTailSession,
    TailSession,
    connect_kwargs,
    handshake_error,
    ssl_context,
    tail_url,
)
from ._time import LokiTime, datetime_to_ns, to_ns
from .config import LokiConfig, tenant_problem

__all__ = ["AsyncLokiClient", "LokiClient"]

#: websockets >= 17 wants ``legacy=True`` to return a connection without a context manager.
_SYNC_CONNECT_EXTRA: dict[str, Any] = (
    {"legacy": True} if "legacy" in inspect.signature(_connect).parameters else {}
)
_sync_connect: Any = _connect

T = TypeVar("T")
Duration = float | _dt.timedelta | str
_C = TypeVar("_C", bound="_ClientBase")


def _health_from_error(err: ServiceError) -> HealthStatus:
    if isinstance(err, (CancelledError, ConfigError)):
        raise err
    word = "unreachable" if isinstance(err, (UnavailableError, TimeoutError)) else "unhealthy"
    return HealthStatus(healthy=False, status=word, details={"error": err.message})


def _tail_params(
    logql: str,
    start: LokiTime | None,
    limit: int | None,
    delay_for_s: float | _dt.timedelta | None,
) -> dict[str, str]:
    if not isinstance(logql, str) or not is_log_query(logql):
        raise ValidationError(
            "tail: only log queries (starting with a {...} selector) can be tailed"
        )
    if limit is not None and (isinstance(limit, bool) or limit < 0):
        raise ValidationError("tail: limit must not be negative")
    delay = 0.0
    if delay_for_s is not None:
        delay = (
            delay_for_s.total_seconds()
            if isinstance(delay_for_s, _dt.timedelta)
            else float(delay_for_s)
        )
        if not 0 <= delay <= MAX_TAIL_DELAY_S:
            raise ValidationError("tail: delay_for must be between 0 and 5 seconds")
    if start is None:
        start_ns = str(datetime_to_ns(_dt.datetime.now(_dt.timezone.utc)))
    else:
        start_ns = to_ns(start, "start")
    params = {"query": logql, "start": start_ns}
    if limit:
        params["limit"] = str(limit)
    if int(delay):
        params["delay_for"] = str(int(delay))
    return params


class _ClientBase:
    """State and call builders shared by :class:`LokiClient` and :class:`AsyncLokiClient`."""

    config: LokiConfig
    guard: CardinalityGuard
    _owner: bool

    def _init_common(self, config: LokiConfig | None) -> None:
        if config is None:
            config = LokiConfig.from_env()
        self.config = config.validate()
        self.guard = CardinalityGuard(self.config.effective_allowed_labels)
        self._resources = ResourceRegistry()
        self._owner = True

    @property
    def tenant_id(self) -> str | None:
        """The tenant id sent as ``X-Scope-OrgID`` (``None`` for none)."""
        return self.config.tenant_id

    @property
    def resources(self) -> ResourceRegistry:
        """Background resources (log clients, tails) stopped by ``close``."""
        return self._resources

    def _derive(self: _C, tenant_id: str) -> _C:
        reason = tenant_problem(tenant_id, self.config.multi_tenant_queries)
        if reason is not None:
            raise ValidationError(f"with_tenant: tenant id {reason}")
        clone = object.__new__(type(self))
        clone.__dict__.update(self.__dict__)
        clone.config = dataclasses.replace(self.config, tenant_id=tenant_id)
        clone._resources = ResourceRegistry()
        clone._owner = False
        return clone

    def _known_secrets(self) -> list[str]:
        return self.config.known_secrets()

    def _tail_headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": user_agent(self.config.user_agent_suffix),
            **dict(self.config.headers),
            **self.config.auth_headers(),
        }
        headers[HEADER_ENCODING_FLAGS] = "categorize-labels"
        return headers

    def _log_client_kwargs(
        self,
        options: BatchOptions | None,
        resource: Mapping[str, Any] | None,
        redaction: RedactionProcessor | None,
        correlate: bool,
    ) -> dict[str, Any]:
        return {
            "options": options,
            "resource": resource,
            "redaction": redaction,
            "correlate": correlate,
        }


class _Stopper:
    def __init__(self, fn: Callable[[], Any]) -> None:
        self.stop = fn


class LokiClient(_ClientBase):
    """Synchronous Grafana Loki client. Thread-safe; use as a context manager or call
    :meth:`close`.

    Every I/O method accepts ``cancel=`` (a :class:`CancelToken`) to abort from another thread.
    Time arguments accept an aware ``datetime``, ``int`` Unix nanoseconds or a string (RFC 3339
    or decimal nanoseconds); durations accept float seconds, ``timedelta`` or Loki text
    (``"5m"``).
    """

    def __init__(
        self,
        config: LokiConfig | None = None,
        *,
        http_transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._init_common(config)
        self._transport = HttpTransport(
            self.config, known_secrets=self._known_secrets, http_transport=http_transport
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **overrides: Any) -> LokiClient:
        """Build a client from the ``GROW_GRAFANA_LOKI_*`` and ``LOKI_*`` environment variables."""
        return cls(LokiConfig.from_env(env, **overrides))

    def with_tenant(self, tenant_id: str) -> LokiClient:
        """A client that sends ``X-Scope-OrgID: tenant_id`` on every request and on the tail
        handshake. It shares the connection pool and credentials; this client is unchanged.

        Raises :class:`ValidationError` for an invalid tenant id. Closing the derived client
        stops only its own log clients and tails; the connections close with this client.
        """
        return self._derive(tenant_id)

    # ------------------------------------------------------------------ plumbing

    def request(self, req: Request, *, cancel: CancelToken | None = None) -> Response:
        """Send a raw request through the client's transport (retries, hooks, redaction).

        Auth and tenant headers are not added automatically; use
        ``config.auth_headers()``.
        """
        return self._transport.request(req, cancel=cancel)

    def _run(self, call: Call[T], cancel: CancelToken | None) -> T:
        if self._resources.closed:
            raise CancelledError(f"{call.request.op}: client is closed")
        try:
            resp = self._transport.request(call.request, cancel=cancel)
        except ServiceError as err:
            raise call.fail(err) from None
        return call.decode(resp)

    # ------------------------------------------------------------------ core

    def health(self, *, cancel: CancelToken | None = None) -> HealthStatus:
        """Readiness (``GET /ready``); never raises for an unhealthy or unreachable service.

        ``healthy`` is true for HTTP 200; otherwise ``status`` is Loki's text (for example
        ``Ingester not ready: waiting for 15s after being ready``) or ``unreachable``.
        Configured credentials are sent because gateways may protect ``/ready``. Raises only
        :class:`CancelledError` and :class:`ConfigError`.
        """
        call = health_call(self.config, self._known_secrets())
        try:
            return self._run(call, cancel)
        except ServiceError as err:
            return _health_from_error(err)

    def ready(self, *, cancel: CancelToken | None = None) -> bool:
        """True when ``GET /ready`` answers 200, False for 503 (starting); errors otherwise."""
        return self._run(ready_call(self.config, self._known_secrets()), cancel)

    def ping(self, *, cancel: CancelToken | None = None) -> float:
        """Authenticated round trip (``GET /loki/api/v1/labels?since=5m``); latency in ms.

        :class:`AuthenticationError` on bad gateway credentials or a missing tenant on a
        multi-tenant Loki.
        """
        started = time.monotonic()
        self._run(ping_call(self.config), cancel)
        return (time.monotonic() - started) * 1000.0

    def build_info(self, *, cancel: CancelToken | None = None) -> BuildInfo:
        """Server build information; :class:`UnsupportedError` when the endpoint is missing."""
        return self._run(build_info_call(self.config), cancel)

    def server_version(self, *, cancel: CancelToken | None = None) -> str:
        """The Loki version (``build_info().version``)."""
        return self.build_info(cancel=cancel).version

    def close(self) -> None:
        """Flush and close the log clients created by this client, stop its tails and (for a
        client not made by :meth:`with_tenant`) close the connections. Idempotent."""
        self._resources.stop_all()
        if self._owner:
            self._transport.close()

    def __enter__(self) -> LokiClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"LokiClient(config={self.config!r})"

    # ------------------------------------------------------------------ push

    def push(
        self,
        streams: Sequence[LokiStream],
        *,
        format: PushFormat = "json",  # noqa: A002 - option name shared with the other packages
        gzip: bool = False,
        timeout_s: float | None = None,
        cancel: CancelToken | None = None,
    ) -> None:
        """Send streams to Loki (``POST /loki/api/v1/push``).

        Labels are validated client-side and the cardinality guard moves labels outside the
        allowlist to structured metadata (unless ``config.disable_label_guard``). ``format``
        is ``"json"`` (default; ``gzip=True`` compresses it) or ``"protobuf"`` (a Snappy
        compressed ``PushRequest``). The whole request succeeds or fails; Loki rejects entries
        older than its ``reject_old_samples_max_age`` or too far in the future with
        :class:`ValidationError`. Retrying is safe (Loki drops exact duplicates), so the
        transport replays the identical payload on 429/5xx. Empty ``streams`` sends nothing.
        """
        self._push(
            streams, self.guard, format=format, gzip=gzip, timeout_s=timeout_s, cancel=cancel
        )

    def _push(
        self,
        streams: Sequence[LokiStream],
        guard: CardinalityGuard,
        *,
        format: PushFormat = "json",  # noqa: A002
        gzip: bool = False,
        timeout_s: float | None = None,
        cancel: CancelToken | None = None,
    ) -> None:
        """``push`` with an explicit guard (the log exporter passes its own)."""
        call = push_call(self.config, guard, streams, format=format, gzip=gzip, timeout_s=timeout_s)
        if call is not None:
            self._run(call, cancel)

    def push_otlp(
        self,
        records: Sequence[LogRecord],
        *,
        gzip: bool = False,
        timeout_s: float | None = None,
        cancel: CancelToken | None = None,
    ) -> None:
        """Send log records as OTLP/JSON to Loki's native OTLP endpoint (``/otlp/v1/logs``).

        Loki promotes ``service.name``, ``deployment.environment`` and its other default
        resource attributes to labels and stores everything else as structured metadata.
        """
        call = push_otlp_call(
            self.config, records, self._known_secrets(), gzip=gzip, timeout_s=timeout_s
        )
        if call is not None:
            self._run(call, cancel)

    # ------------------------------------------------------------------ query

    def query(
        self,
        logql: str,
        *,
        time: LokiTime | None = None,
        limit: int | None = None,
        direction: Direction | None = None,
        cancel: CancelToken | None = None,
    ) -> QueryResult:
        """Instant metric query (``GET /loki/api/v1/query``).

        Loki answers log queries only as range queries, so a log selector (``{...}``) is
        rejected with :class:`ValidationError` before any request: use :meth:`query_range` or
        :meth:`log_query`.
        """
        return self._run(
            query_call(self.config, logql, time=time, limit=limit, direction=direction), cancel
        )

    def query_range(
        self,
        logql: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        step_s: Duration | None = None,
        interval_s: Duration | None = None,
        limit: int | None = None,
        direction: Direction | None = None,
        cancel: CancelToken | None = None,
    ) -> QueryResult:
        """Range query (``GET /loki/api/v1/query_range``).

        Log queries return ``streams`` (each stream ordered by ``direction``; streams are not
        ordered globally), metric queries return ``matrix``. ``start`` is inclusive, ``end``
        exclusive; without them Loki uses the last hour. Labels are categorized: stream labels
        are index labels, structured metadata and parsed labels come per entry.
        """
        call = query_range_call(
            self.config,
            logql,
            start=start,
            end=end,
            since_s=since_s,
            step_s=step_s,
            interval_s=interval_s,
            limit=limit,
            direction=direction,
        )
        return self._run(call, cancel)

    def labels(
        self,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        query: str | None = None,
        cancel: CancelToken | None = None,
    ) -> list[str]:
        """Label names (``GET /loki/api/v1/labels``); structured metadata keys are not labels."""
        call = labels_call(self.config, start=start, end=end, since_s=since_s, query=query)
        return self._run(call, cancel)

    def label_values(
        self,
        name: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        query: str | None = None,
        cancel: CancelToken | None = None,
    ) -> list[str]:
        """Values of label ``name`` (``GET /loki/api/v1/label/{name}/values``).

        Loki answers unknown and invalid names with an empty list, so the name is validated
        client-side (:class:`ValidationError`).
        """
        call = label_values_call(
            self.config, name, start=start, end=end, since_s=since_s, query=query
        )
        return self._run(call, cancel)

    def series(
        self,
        matchers: Sequence[str] | str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        cancel: CancelToken | None = None,
    ) -> list[dict[str, str]]:
        """Label sets of the streams matching any selector (``POST /loki/api/v1/series``).

        At least one matcher is required (Loki would otherwise return every series).
        """
        call = series_call(self.config, matchers, start=start, end=end, since_s=since_s)
        return self._run(call, cancel)

    def index_stats(
        self,
        query: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        cancel: CancelToken | None = None,
    ) -> IndexStats:
        """Index statistics for a selector (``GET /loki/api/v1/index/stats``).

        Values come from the TSDB index and stay zero until the ingester flushed the chunks.
        """
        return self._run(index_stats_call(self.config, query, start=start, end=end), cancel)

    def volume(
        self,
        query: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        limit: int | None = None,
        target_labels: Sequence[str] | None = None,
        aggregate_by: Literal["series", "labels"] | None = None,
        cancel: CancelToken | None = None,
    ) -> QueryResult:
        """Ingested bytes per series or label (``GET /loki/api/v1/index/volume``), a vector.

        Needs ``limits_config.volume_enabled`` on the server; otherwise Loki returns an empty
        vector ("no data", not an error).
        """
        call = volume_call(
            self.config,
            query,
            ranged=False,
            start=start,
            end=end,
            limit=limit,
            target_labels=target_labels,
            aggregate_by=aggregate_by,
        )
        return self._run(call, cancel)

    def volume_range(
        self,
        query: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        step_s: Duration | None = None,
        limit: int | None = None,
        target_labels: Sequence[str] | None = None,
        aggregate_by: Literal["series", "labels"] | None = None,
        cancel: CancelToken | None = None,
    ) -> QueryResult:
        """:meth:`volume` over time (``GET /loki/api/v1/index/volume_range``), a matrix."""
        call = volume_call(
            self.config,
            query,
            ranged=True,
            start=start,
            end=end,
            step_s=step_s,
            limit=limit,
            target_labels=target_labels,
            aggregate_by=aggregate_by,
        )
        return self._run(call, cancel)

    def format_query(self, logql: str, *, cancel: CancelToken | None = None) -> str:
        """Let Loki parse and pretty-print ``logql``; :class:`ValidationError` if it does not
        parse."""
        return self._run(format_query_call(self.config, logql), cancel)

    def tail(
        self,
        logql: str,
        on_entries: Callable[[TailFrame], None],
        *,
        start: LokiTime | None = None,
        limit: int | None = None,
        delay_for_s: float | _dt.timedelta | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> TailSession:
        """Stream new entries of a log query over a WebSocket (``GET /loki/api/v1/tail``).

        ``on_entries`` is called from one thread for every frame. ``start`` replays entries
        from that time first; without it the tail is live only. The handshake carries the
        auth, tenant and categorize-labels headers; handshake failures raise
        (401 → :class:`AuthenticationError`, 400 → :class:`ValidationError`). Metric queries
        and ``delay_for_s`` above 5 s are rejected client-side. ``on_error`` receives errors
        of the running tail (and exceptions raised by ``on_entries``). Stop it with
        :meth:`TailSession.stop`; :meth:`close` stops it too.
        """
        params = _tail_params(logql, start, limit, delay_for_s)
        if self._resources.closed:
            raise CancelledError("tail: client is closed")
        url = tail_url(self.config.url, params)
        kwargs = connect_kwargs(self.config, self._tail_headers())
        try:
            conn = _sync_connect(url, ssl=ssl_context(self.config), **kwargs, **_SYNC_CONNECT_EXTRA)
        except ServiceError:
            raise
        except Exception as exc:  # noqa: BLE001 - mapped to SDK errors
            raise handshake_error(exc, self._known_secrets()) from None
        session = TailSession(conn, on_entries, on_error)
        self._resources.register(session)
        return session

    # ------------------------------------------------------------------ logging category

    def log_client(
        self,
        *,
        options: BatchOptions | None = None,
        resource: Mapping[str, Any] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
        format: PushFormat = "json",  # noqa: A002
    ) -> BatchingLogClient:
        """A batching ``LogClient`` pushing to Loki through :class:`LokiLogExporter`.

        ``resource`` (``service.name``, ``deployment.environment`` …) is added to every record;
        :meth:`close` flushes and closes the log client.
        """
        exporter = LokiLogExporter(self, format=format)
        return self._track(
            BatchingLogClient(
                exporter, **self._log_client_kwargs(options, resource, redaction, correlate)
            )
        )

    def otlp_log_client(
        self,
        *,
        options: BatchOptions | None = None,
        resource: Mapping[str, Any] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
    ) -> BatchingLogClient:
        """A batching ``LogClient`` exporting through Loki's OTLP endpoint
        (:class:`LokiOtlpExporter`)."""
        exporter = LokiOtlpExporter(self)
        return self._track(
            BatchingLogClient(
                exporter, **self._log_client_kwargs(options, resource, redaction, correlate)
            )
        )

    def _track(self, log_client: BatchingLogClient) -> BatchingLogClient:
        self._resources.register(_Stopper(log_client.close))
        return log_client

    def log_query(self) -> LokiLogQueryClient:
        """The ``LogQueryClient`` of this client (search, tail, list_streams, list_labels,
        wait_for_log)."""
        return LokiLogQueryClient(self)


class AsyncLokiClient(_ClientBase):
    """Asynchronous Grafana Loki client; use ``async with`` or call :meth:`aclose`.

    Same methods as :class:`LokiClient`, awaited. Cancel a call by cancelling its task: the
    SDK raises :class:`CancelledError`, which is also an :class:`asyncio.CancelledError`.
    """

    def __init__(
        self,
        config: LokiConfig | None = None,
        *,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._init_common(config)
        self._log_clients: list[AsyncBatchingLogClient] = []
        self._tails: list[AsyncTailSession] = []
        self._transport = AsyncHttpTransport(
            self.config, known_secrets=self._known_secrets, http_transport=http_transport
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, **overrides: Any) -> AsyncLokiClient:
        """Build a client from the ``GROW_GRAFANA_LOKI_*`` and ``LOKI_*`` environment variables."""
        return cls(LokiConfig.from_env(env, **overrides))

    def with_tenant(self, tenant_id: str) -> AsyncLokiClient:
        """See :meth:`LokiClient.with_tenant`."""
        clone = self._derive(tenant_id)
        clone._log_clients = []
        clone._tails = []
        return clone

    async def request(self, req: Request) -> Response:
        """Send a raw request through the client's transport."""
        return await self._transport.request(req)

    async def _run(self, call: Call[T]) -> T:
        if self._resources.closed:
            raise CancelledError(f"{call.request.op}: client is closed")
        try:
            resp = await self._transport.request(call.request)
        except CancelledError:
            raise
        except ServiceError as err:
            raise call.fail(err) from None
        return call.decode(resp)

    async def health(self) -> HealthStatus:
        """See :meth:`LokiClient.health`."""
        call = health_call(self.config, self._known_secrets())
        try:
            return await self._run(call)
        except ServiceError as err:
            return _health_from_error(err)

    async def ready(self) -> bool:
        """See :meth:`LokiClient.ready`."""
        return await self._run(ready_call(self.config, self._known_secrets()))

    async def ping(self) -> float:
        """See :meth:`LokiClient.ping`."""
        started = time.monotonic()
        await self._run(ping_call(self.config))
        return (time.monotonic() - started) * 1000.0

    async def build_info(self) -> BuildInfo:
        """See :meth:`LokiClient.build_info`."""
        return await self._run(build_info_call(self.config))

    async def server_version(self) -> str:
        """See :meth:`LokiClient.server_version`."""
        return (await self.build_info()).version

    async def aclose(self) -> None:
        """Flush and close log clients, stop tails, close connections (idempotent)."""
        log_clients, self._log_clients = self._log_clients, []
        for log_client in log_clients:
            with contextlib.suppress(Exception):  # closing must not fail half way
                await log_client.close()
        self._resources.stop_all()
        sessions, self._tails = self._tails, []
        for session in sessions:
            await session.stop()
        if self._owner:
            await self._transport.aclose()

    close = aclose

    async def __aenter__(self) -> AsyncLokiClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"AsyncLokiClient(config={self.config!r})"

    async def push(
        self,
        streams: Sequence[LokiStream],
        *,
        format: PushFormat = "json",  # noqa: A002
        gzip: bool = False,
        timeout_s: float | None = None,
    ) -> None:
        """See :meth:`LokiClient.push`."""
        await self._push(streams, self.guard, format=format, gzip=gzip, timeout_s=timeout_s)

    async def _push(
        self,
        streams: Sequence[LokiStream],
        guard: CardinalityGuard,
        *,
        format: PushFormat = "json",  # noqa: A002
        gzip: bool = False,
        timeout_s: float | None = None,
    ) -> None:
        """``push`` with an explicit guard (the log exporter passes its own)."""
        call = push_call(self.config, guard, streams, format=format, gzip=gzip, timeout_s=timeout_s)
        if call is not None:
            await self._run(call)

    async def push_otlp(
        self,
        records: Sequence[LogRecord],
        *,
        gzip: bool = False,
        timeout_s: float | None = None,
    ) -> None:
        """See :meth:`LokiClient.push_otlp`."""
        call = push_otlp_call(
            self.config, records, self._known_secrets(), gzip=gzip, timeout_s=timeout_s
        )
        if call is not None:
            await self._run(call)

    async def query(
        self,
        logql: str,
        *,
        time: LokiTime | None = None,
        limit: int | None = None,
        direction: Direction | None = None,
    ) -> QueryResult:
        """See :meth:`LokiClient.query`."""
        return await self._run(
            query_call(self.config, logql, time=time, limit=limit, direction=direction)
        )

    async def query_range(
        self,
        logql: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        step_s: Duration | None = None,
        interval_s: Duration | None = None,
        limit: int | None = None,
        direction: Direction | None = None,
    ) -> QueryResult:
        """See :meth:`LokiClient.query_range`."""
        call = query_range_call(
            self.config,
            logql,
            start=start,
            end=end,
            since_s=since_s,
            step_s=step_s,
            interval_s=interval_s,
            limit=limit,
            direction=direction,
        )
        return await self._run(call)

    async def labels(
        self,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        query: str | None = None,
    ) -> list[str]:
        """See :meth:`LokiClient.labels`."""
        return await self._run(
            labels_call(self.config, start=start, end=end, since_s=since_s, query=query)
        )

    async def label_values(
        self,
        name: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
        query: str | None = None,
    ) -> list[str]:
        """See :meth:`LokiClient.label_values`."""
        return await self._run(
            label_values_call(self.config, name, start=start, end=end, since_s=since_s, query=query)
        )

    async def series(
        self,
        matchers: Sequence[str] | str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        since_s: Duration | None = None,
    ) -> list[dict[str, str]]:
        """See :meth:`LokiClient.series`."""
        return await self._run(
            series_call(self.config, matchers, start=start, end=end, since_s=since_s)
        )

    async def index_stats(
        self,
        query: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
    ) -> IndexStats:
        """See :meth:`LokiClient.index_stats`."""
        return await self._run(index_stats_call(self.config, query, start=start, end=end))

    async def volume(
        self,
        query: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        limit: int | None = None,
        target_labels: Sequence[str] | None = None,
        aggregate_by: Literal["series", "labels"] | None = None,
    ) -> QueryResult:
        """See :meth:`LokiClient.volume`."""
        return await self._run(
            volume_call(
                self.config,
                query,
                ranged=False,
                start=start,
                end=end,
                limit=limit,
                target_labels=target_labels,
                aggregate_by=aggregate_by,
            )
        )

    async def volume_range(
        self,
        query: str,
        *,
        start: LokiTime | None = None,
        end: LokiTime | None = None,
        step_s: Duration | None = None,
        limit: int | None = None,
        target_labels: Sequence[str] | None = None,
        aggregate_by: Literal["series", "labels"] | None = None,
    ) -> QueryResult:
        """See :meth:`LokiClient.volume_range`."""
        return await self._run(
            volume_call(
                self.config,
                query,
                ranged=True,
                start=start,
                end=end,
                step_s=step_s,
                limit=limit,
                target_labels=target_labels,
                aggregate_by=aggregate_by,
            )
        )

    async def format_query(self, logql: str) -> str:
        """See :meth:`LokiClient.format_query`."""
        return await self._run(format_query_call(self.config, logql))

    async def tail(
        self,
        logql: str,
        on_entries: Callable[[TailFrame], None],
        *,
        start: LokiTime | None = None,
        limit: int | None = None,
        delay_for_s: float | _dt.timedelta | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> AsyncTailSession:
        """See :meth:`LokiClient.tail`; frames are delivered from a task on the current loop."""
        params = _tail_params(logql, start, limit, delay_for_s)
        if self._resources.closed:
            raise CancelledError("tail: client is closed")
        url = tail_url(self.config.url, params)
        kwargs = connect_kwargs(self.config, self._tail_headers())
        try:
            conn = await _aconnect(url, ssl=ssl_context(self.config), **kwargs)
        except ServiceError:
            raise
        except Exception as exc:  # noqa: BLE001 - mapped to SDK errors
            raise handshake_error(exc, self._known_secrets()) from None
        session = AsyncTailSession(conn, on_entries, on_error)
        self._tails.append(session)
        return session

    def log_client(
        self,
        *,
        options: BatchOptions | None = None,
        resource: Mapping[str, Any] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
        format: PushFormat = "json",  # noqa: A002
    ) -> AsyncBatchingLogClient:
        """See :meth:`LokiClient.log_client` (async log client; ``flush``/``close`` awaited)."""
        exporter = AsyncLokiLogExporter(self, format=format)
        log_client = AsyncBatchingLogClient(
            exporter, **self._log_client_kwargs(options, resource, redaction, correlate)
        )
        self._log_clients.append(log_client)
        return log_client

    def otlp_log_client(
        self,
        *,
        options: BatchOptions | None = None,
        resource: Mapping[str, Any] | None = None,
        redaction: RedactionProcessor | None = _DEFAULT_REDACTION,
        correlate: bool = True,
    ) -> AsyncBatchingLogClient:
        """See :meth:`LokiClient.otlp_log_client`."""
        exporter = AsyncLokiOtlpExporter(self)
        log_client = AsyncBatchingLogClient(
            exporter, **self._log_client_kwargs(options, resource, redaction, correlate)
        )
        self._log_clients.append(log_client)
        return log_client

    def log_query(self) -> AsyncLokiLogQueryClient:
        """See :meth:`LokiClient.log_query`."""
        return AsyncLokiLogQueryClient(self)
