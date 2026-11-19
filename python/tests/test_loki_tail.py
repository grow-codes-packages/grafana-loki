"""Live tail over WebSocket against a local websockets server."""

from __future__ import annotations

import asyncio
import json
import ssl
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.frames import Close
from websockets.sync.server import ServerConnection, serve

import grow_grafana_loki as sdk
from _loki import fx
from _support import closed_port_url, make_config
from grow_grafana_loki import _tail

TAIL_OK = fx("tail.ok")["response"]["frame"]
CATEGORIZED = fx("tail.categorized-labels")["response"]["frame"]


class WsServer:
    """Scripted tail server: ``script(conn)`` runs per connection; requests are recorded."""

    def __init__(self) -> None:
        self.script: Callable[[ServerConnection], None] = lambda conn: None
        self.reject: tuple[int, str] | None = None
        self.requests: list[tuple[str, dict[str, str]]] = []

        def process_request(conn: ServerConnection, request: Any) -> Any:
            self.requests.append((request.path, dict(request.headers.raw_items())))
            if self.reject is not None:
                status, body = self.reject
                return conn.respond(status, body)
            return None

        def handler(conn: ServerConnection) -> None:
            self.script(conn)
            try:
                conn.wait_closed() if hasattr(conn, "wait_closed") else None
                for _ in conn:
                    pass
            except ConnectionClosed:
                pass

        self.server = serve(handler, "127.0.0.1", 0, process_request=process_request)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.server.socket.getsockname()[:2]
        return f"http://{host}:{port}"

    def params(self, index: int = -1) -> dict[str, list[str]]:
        return parse_qs(urlsplit(self.requests[index][0]).query)

    def stop(self) -> None:
        self.server.shutdown()


@pytest.fixture
def ws() -> Iterator[WsServer]:
    srv = WsServer()
    yield srv
    srv.stop()


def until(fn: Callable[[], Any], timeout: float = 5.0) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("condition not met")


def test_tail_delivers_frames_and_stops(ws: WsServer) -> None:
    dropped = {"streams": [], "dropped_entries": [{"labels": {"a": "b"}, "timestamp": "1"}]}

    def script(conn: ServerConnection) -> None:
        conn.send(json.dumps(TAIL_OK))
        conn.send("not json")
        conn.send(json.dumps(dropped))
        conn.send(json.dumps(CATEGORIZED))

    ws.script = script
    frames: list[sdk.TailFrame] = []
    errors: list[Exception] = []
    c = sdk.LokiClient(
        make_config(ws.url, bearer_token="tok-1234", tenant_id="t1", headers={"X-Extra": "1"})
    )
    session = c.tail(
        '{run="x"}', frames.append, start=5, limit=10, delay_for_s=2, on_error=errors.append
    )
    until(lambda: len(frames) == 3)
    assert frames[0].streams[0].entries[0].line.startswith("tail marker")
    assert frames[2].streams[0].entries[0].structured_metadata["request_id"].startswith("tail-")
    assert session.dropped_count == 1
    assert isinstance(errors[0], sdk.ValidationError)
    path, headers = ws.requests[-1]
    assert urlsplit(path).path == "/loki/api/v1/tail"
    assert ws.params() == {
        "query": ['{run="x"}'],
        "start": ["5"],
        "limit": ["10"],
        "delay_for": ["2"],
    }
    lower = {k.lower(): v for k, v in headers.items()}
    assert lower["x-scope-orgid"] == "t1"
    assert lower["authorization"] == "Bearer tok-1234"
    assert lower["x-loki-response-encoding-flags"] == "categorize-labels"
    assert lower["user-agent"].startswith("grow-codes-grafana-loki/")
    assert lower["x-extra"] == "1"
    assert not session.done
    session.stop()
    session.stop()
    assert session.done
    assert session.wait(0.1)
    assert session.error is None
    c.close()


def test_tail_server_close_codes(ws: WsServer) -> None:
    reason = {"value": "only log selector is supported"}

    def script(conn: ServerConnection) -> None:
        conn.close(1011, reason["value"])

    ws.script = script
    c = sdk.LokiClient(make_config(ws.url))
    errors: list[Exception] = []
    session = c.tail('{a="b"}', lambda f: None, on_error=errors.append)
    assert session.wait(5)
    assert isinstance(session.error, sdk.ValidationError)
    assert errors == [session.error]
    reason["value"] = "boom"
    other = c.tail('{a="b"}', lambda f: None)
    assert other.wait(5)
    assert isinstance(other.error, sdk.UnavailableError)
    ws.script = lambda conn: conn.close(1000)
    normal = c.tail('{a="b"}', lambda f: None)
    assert normal.wait(5)
    assert normal.error is None
    # without start the tail is live only (start = now)
    assert int(ws.params()["start"][0]) > time.time_ns() - 60 * 10**9
    c.close()


def test_tail_callback_errors_do_not_stop(ws: WsServer) -> None:
    ws.script = lambda conn: [conn.send(json.dumps(TAIL_OK)) for _ in range(2)]
    calls: list[int] = []

    def on_entries(frame: sdk.TailFrame) -> None:
        calls.append(1)
        raise RuntimeError("callback failed")

    errors: list[Exception] = []
    c = sdk.LokiClient(make_config(ws.url))
    session = c.tail('{a="b"}', on_entries, on_error=errors.append)
    until(lambda: len(calls) == 2)
    assert all(isinstance(e, RuntimeError) for e in errors)
    c.close()  # close stops running tails
    assert session.done


def test_tail_handshake_errors(ws: WsServer) -> None:
    c = sdk.LokiClient(make_config(ws.url, bearer_token="secret-token-1"))
    ws.reject = (401, "no org id\n")
    with pytest.raises(sdk.AuthenticationError) as info:
        c.tail('{a="b"}', lambda f: None)
    assert "no org id" in str(info.value)
    ws.reject = (400, "delay_for can't be greater than 5\n")
    with pytest.raises(sdk.ValidationError):
        c.tail('{a="b"}', lambda f: None)
    with pytest.raises(sdk.UnavailableError):
        sdk.LokiClient(make_config(closed_port_url())).tail('{a="b"}', lambda f: None)


def test_tail_client_side_validation(ws: WsServer) -> None:
    c = sdk.LokiClient(make_config(ws.url))
    for call in (
        lambda: c.tail('count_over_time({a="b"}[1m])', lambda f: None),
        lambda: c.tail('{a="b"}', lambda f: None, delay_for_s=6),
        lambda: c.tail('{a="b"}', lambda f: None, limit=-1),
        lambda: c.tail('{a="b"}', lambda f: None, start="soon"),
    ):
        with pytest.raises(sdk.ValidationError):
            call()
    import datetime as dt

    ws.script = lambda conn: conn.close(1000)
    c.tail('{a="b"}', lambda f: None, delay_for_s=dt.timedelta(seconds=1)).wait(5)
    assert ws.params()["delay_for"] == ["1"]
    c.tail('{a="b"}', lambda f: None, delay_for_s=0.5).wait(5)
    assert "delay_for" not in ws.params()
    assert ws.requests
    c.close()
    with pytest.raises(sdk.CancelledError):
        c.tail('{a="b"}', lambda f: None)


def test_log_query_tail_records(ws: WsServer) -> None:
    ws.script = lambda conn: conn.send(json.dumps(CATEGORIZED))
    c = sdk.LokiClient(make_config(ws.url))
    records: list[sdk.LogRecord] = []
    handle = c.log_query().tail("tail marker", records.append)
    until(lambda: records)
    handle.stop()
    assert records[0].attributes["request_id"].startswith("tail-")
    assert records[0].resource["service.name"] == "sdk-verify"
    assert ws.params()["query"] == ['{service_name=~".+"} |= "tail marker"']
    c.close()


async def test_async_tail(ws: WsServer) -> None:
    ws.script = lambda conn: conn.send(json.dumps(TAIL_OK))
    c = sdk.AsyncLokiClient(make_config(ws.url, tenant_id="t"))
    frames: list[sdk.TailFrame] = []
    session = await c.tail('{a="b"}', frames.append, start=1)
    for _ in range(200):
        if frames:
            break
        await asyncio.sleep(0.01)
    assert frames
    assert session.dropped_count == 0
    assert not session.done
    assert not await session.wait(0.01)
    await session.stop()
    await session.stop()
    assert session.done
    assert session.error is None
    records: list[sdk.LogRecord] = []
    handle = await c.log_query().tail('{a="b"}', records.append)
    for _ in range(200):
        if records:
            break
        await asyncio.sleep(0.01)
    assert records
    ws.script = lambda conn: conn.close(1011, "only log selector is supported")
    closing = await c.tail('{a="b"}', lambda f: None)
    assert await closing.wait(5)
    assert isinstance(closing.error, sdk.ValidationError)
    await c.aclose()  # stops the remaining tail
    del handle
    with pytest.raises(sdk.CancelledError):
        await c.tail('{a="b"}', lambda f: None)


async def test_async_tail_errors(ws: WsServer) -> None:
    c = sdk.AsyncLokiClient(make_config(ws.url))
    ws.reject = (401, "no org id")
    with pytest.raises(sdk.AuthenticationError):
        await c.tail('{a="b"}', lambda f: None)
    with pytest.raises(sdk.ValidationError):
        await c.tail("vector(1)", lambda f: None)
    await c.aclose()


async def test_async_session_cancelled_task() -> None:
    class Conn:
        async def recv(self) -> str:
            await asyncio.sleep(10)
            return ""

        async def close(self, code: int) -> None:
            raise RuntimeError("already closed")

    session = _tail.AsyncTailSession(Conn(), lambda f: None, None)
    await asyncio.sleep(0)
    session._task.cancel()
    await asyncio.sleep(0.01)
    assert isinstance(session.error, sdk.CancelledError)
    await session.stop()

    class Broken:
        async def recv(self) -> str:
            raise OSError("socket gone")

    broken = _tail.AsyncTailSession(Broken(), lambda f: None, None)
    await asyncio.sleep(0.01)
    assert isinstance(broken.error, sdk.UnavailableError)


def test_sync_session_unexpected_error() -> None:
    class Broken:
        def recv(self) -> str:
            raise OSError("socket gone")

        def close(self, code: int) -> None:
            raise RuntimeError("cannot close")

    reported: list[Exception] = []

    def bad_handler(err: Exception) -> None:
        reported.append(err)
        raise RuntimeError("handler fails too")

    session = _tail.TailSession(Broken(), lambda f: None, bad_handler)
    assert session.wait(5)
    assert isinstance(session.error, sdk.UnavailableError)
    assert reported == [session.error]
    session.stop()


def test_helpers() -> None:
    assert _tail.tail_url("https://loki.example/base/", {"query": '{a="b"}'}).startswith(
        "wss://loki.example/base/loki/api/v1/tail?query=%7Ba%3D%22b%22%7D"
    )
    assert _tail.tail_url("http://h:1", {}).startswith("ws://h:1/")
    assert _tail.tail_url("ws://h:1", {}).startswith("ws://h:1/")
    assert _tail.ssl_context(sdk.LokiConfig(url="http://h")) is None
    ctx = _tail.ssl_context(sdk.LokiConfig(url="https://h"))
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    insecure = _tail.ssl_context(
        sdk.LokiConfig(url="https://h", tls=sdk.TlsConfig(insecure_skip_verify=True))
    )
    assert insecure is not None
    assert insecure.verify_mode == ssl.CERT_NONE
    with pytest.raises(sdk.ConfigError):
        _tail.ssl_context(
            sdk.LokiConfig(
                url="https://h",
                tls=sdk.TlsConfig(client_cert_path="/nope.pem", client_key_path="/nope.key"),
            )
        )
    assert isinstance(_tail.handshake_error(TimeoutError(), []), sdk.TimeoutError)
    assert isinstance(_tail.handshake_error(InvalidHandshake("x"), []), sdk.UnavailableError)
    closed = ConnectionClosed(None, None)
    assert isinstance(_tail.close_error(closed), sdk.UnavailableError)
    going = ConnectionClosed(Close(1001, "bye"), None)
    assert _tail.close_error(going) is None
    no_reason = _tail.close_error(ConnectionClosed(Close(1011, ""), None))
    assert no_reason is not None
    assert "code 1011" in no_reason.message
