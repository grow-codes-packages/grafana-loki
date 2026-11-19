"""Live tail over a WebSocket (``GET /loki/api/v1/tail``), sync and async sessions."""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import json
import ssl
import threading
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

import httpx
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from ._core import (
    CancelledError,
    ConfigError,
    ServiceError,
    TimeoutError,
    UnavailableError,
    ValidationError,
    error_from_status,
    redact_text,
    truncate,
)
from ._models import TailFrame, decode_tail_frame
from .config import LokiConfig

__all__ = ["AsyncTailSession", "MAX_TAIL_DELAY_S", "TailSession"]

#: Largest ``delay_for`` Loki accepts, in seconds.
MAX_TAIL_DELAY_S = 5
#: Upper bound of one tail frame.
MAX_TAIL_FRAME = 64 << 20

_REJECTIONS = ("only log selector is supported", "parse error", "delay_for")

OnFrame = Callable[[TailFrame], None]
OnError = Callable[[Exception], None]


def tail_url(base_url: str, params: Mapping[str, str]) -> str:
    """WebSocket URL (``ws``/``wss``) of the tail endpoint with encoded query parameters."""
    base = base_url.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://") :]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://") :]
    query = "&".join(f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in params.items())
    return f"{base}/loki/api/v1/tail?{query}"


def ssl_context(config: LokiConfig) -> ssl.SSLContext | None:
    """TLS context for ``wss://`` URLs (``None`` for plain ``ws://``)."""
    if not config.url.startswith("https://"):
        return None
    tls = config.tls
    try:
        ctx = ssl.create_default_context(cafile=tls.ca_cert_path, cadata=tls.ca_cert_pem)
        if tls.client_cert_path and tls.client_key_path:
            ctx.load_cert_chain(tls.client_cert_path, tls.client_key_path)
    except (OSError, ssl.SSLError, ValueError):
        raise ConfigError("config: TLS certificate files could not be loaded") from None
    if tls.insecure_skip_verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def connect_kwargs(config: LokiConfig, headers: Mapping[str, str]) -> dict[str, Any]:
    """Keyword arguments shared by the sync and async ``websockets`` connect calls."""
    return {
        "additional_headers": dict(headers),
        "user_agent_header": None,
        "proxy": config.proxy,
        "open_timeout": config.connect_timeout + config.request_timeout,
        "max_size": MAX_TAIL_FRAME,
        "compression": None,
    }


def handshake_error(exc: BaseException, known_secrets: list[str], op: str = "tail") -> ServiceError:
    """Map a failed WebSocket handshake to the SPEC error (401 → AUTHENTICATION, …)."""
    if isinstance(exc, InvalidStatus):
        resp = exc.response
        headers = httpx.Headers([(k, v) for k, v in resp.headers.raw_items()])
        if "content-type" not in headers:
            headers["content-type"] = "text/plain"
        return error_from_status(
            resp.status_code,
            op,
            content=bytes(resp.body or b""),
            headers=headers,
            known_secrets=known_secrets,
        )
    if isinstance(exc, (builtins.TimeoutError, asyncio.TimeoutError)):
        return TimeoutError(f"{op}: handshake timed out")
    detail = truncate(redact_text(str(exc) or type(exc).__name__, known_secrets), 200)
    if isinstance(exc, InvalidHandshake):
        return UnavailableError(f"{op}: websocket handshake failed: {detail}")
    return UnavailableError(f"{op}: connection failed: {detail}")


def close_error(exc: ConnectionClosed, op: str = "tail") -> ServiceError | None:
    """Error for a server close: ``None`` for a normal close, VALIDATION for a rejected query."""
    frame = exc.rcvd
    if frame is None:
        return UnavailableError(f"{op}: connection lost")
    if frame.code in (1000, 1001):
        return None
    reason = frame.reason.strip()
    message = f"{op}: closed by server: {reason or 'code ' + str(frame.code)}"
    if any(r in reason for r in _REJECTIONS):
        return ValidationError(message)
    return UnavailableError(message)


def parse_frame(data: str | bytes) -> TailFrame:
    """Decode one tail text frame."""
    try:
        doc = json.loads(data)
    except ValueError:
        raise ValidationError("tail: invalid JSON frame") from None
    return decode_tail_frame(doc)


class _SessionState:
    def __init__(self, on_error: OnError | None) -> None:
        self.on_error = on_error
        self.error: ServiceError | None = None
        self.dropped = 0
        self.stopping = False

    def deliver(self, on_frame: OnFrame, data: str | bytes) -> None:
        try:
            frame = parse_frame(data)
        except ServiceError as err:
            self.report(err)
            return
        self.dropped += len(frame.dropped_entries)
        try:
            on_frame(frame)
        except Exception as exc:  # noqa: BLE001 - a callback must not end the tail
            if self.on_error is not None:
                with contextlib.suppress(Exception):
                    self.on_error(exc)

    def report(self, err: Exception) -> None:
        if self.on_error is not None:
            with contextlib.suppress(Exception):
                self.on_error(err)

    def finish(self, err: ServiceError | None) -> None:
        if err is not None and not self.stopping:
            self.error = err
            self.report(err)


class TailSession:
    """A running live tail (sync). Implements the logging ``TailHandle`` protocol.

    Frames are delivered from one daemon thread. :meth:`stop` is idempotent.
    """

    def __init__(self, conn: Any, on_frame: OnFrame, on_error: OnError | None) -> None:
        self._conn = conn
        self._state = _SessionState(on_error)
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run, args=(on_frame,), name="grow-grafana-loki-tail", daemon=True
        )
        self._thread.start()

    def _run(self, on_frame: OnFrame) -> None:
        err: ServiceError | None = None
        try:
            while True:
                data = self._conn.recv()
                self._state.deliver(on_frame, data)
        except ConnectionClosed as exc:
            err = close_error(exc)
        except Exception as exc:  # noqa: BLE001 - reported through error/on_error
            err = UnavailableError(f"tail: {type(exc).__name__}")
        finally:
            self._state.finish(err)
            self._done.set()

    @property
    def error(self) -> ServiceError | None:
        """Why the tail ended: ``None`` after :meth:`stop` or a normal close, VALIDATION when
        Loki closed it for a bad query, UNAVAILABLE on connection errors."""
        return self._state.error

    @property
    def dropped_count(self) -> int:
        """Number of entries Loki reported as dropped (slow tailer)."""
        return self._state.dropped

    @property
    def done(self) -> bool:
        """True once the tail has ended."""
        return self._done.is_set()

    def wait(self, timeout_s: float | None = None) -> bool:
        """Block until the tail ends (or ``timeout_s``); returns :attr:`done`."""
        return self._done.wait(timeout_s)

    def stop(self) -> None:
        """Close the WebSocket (code 1000) and wait for the reader thread (idempotent)."""
        with self._lock:
            self._state.stopping = True
            with contextlib.suppress(Exception):
                self._conn.close(1000)
        if self._thread is not threading.current_thread():
            self._thread.join()


class AsyncTailSession:
    """A running live tail (async). Implements the logging ``AsyncTailHandle`` protocol.

    Frames are delivered from a task on the event loop that opened the tail.
    """

    def __init__(self, conn: Any, on_frame: OnFrame, on_error: OnError | None) -> None:
        self._conn = conn
        self._state = _SessionState(on_error)
        self._task = asyncio.get_running_loop().create_task(self._run(on_frame))

    async def _run(self, on_frame: OnFrame) -> None:
        err: ServiceError | None = None
        try:
            while True:
                data = await self._conn.recv()
                self._state.deliver(on_frame, data)
        except ConnectionClosed as exc:
            err = close_error(exc)
        except asyncio.CancelledError:
            err = CancelledError("tail: cancelled") if not self._state.stopping else None
        except Exception as exc:  # noqa: BLE001 - reported through error/on_error
            err = UnavailableError(f"tail: {type(exc).__name__}")
        self._state.finish(err)

    @property
    def error(self) -> ServiceError | None:
        """Why the tail ended (see :attr:`TailSession.error`)."""
        return self._state.error

    @property
    def dropped_count(self) -> int:
        """Number of entries Loki reported as dropped (slow tailer)."""
        return self._state.dropped

    @property
    def done(self) -> bool:
        """True once the tail has ended."""
        return self._task.done()

    async def wait(self, timeout_s: float | None = None) -> bool:
        """Wait until the tail ends (or ``timeout_s``); returns :attr:`done`."""
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(self._task), timeout_s)
        return self.done

    async def stop(self) -> None:
        """Close the WebSocket (code 1000) and wait for the reader task (idempotent)."""
        self._state.stopping = True
        with contextlib.suppress(Exception):
            await self._conn.close(1000)
        if not self._task.done():
            self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
