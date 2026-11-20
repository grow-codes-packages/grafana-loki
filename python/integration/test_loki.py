"""Every Loki function against the real service, each with a round-trip assertion."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import TypeVar

import pytest
from factories import async_core_client, core_client, gateway_config, mt_client

import grow_grafana_loki as sdk

T = TypeVar("T")


def rid() -> str:
    return uuid.uuid4().hex[:10]


def now_ns() -> int:
    return time.time_ns()


def until(fn: Callable[[], T | None], timeout: float = 20.0, interval: float = 0.25) -> T:
    deadline = time.monotonic() + timeout
    while True:
        value = fn()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(interval)


def entries(result: sdk.QueryResult) -> list[tuple[dict[str, str], sdk.StreamEntry]]:
    return [(dict(s.labels), e) for s in result.streams for e in s.entries]


@pytest.fixture
def run() -> str:
    return rid()


@pytest.fixture
def pushed(client: sdk.LokiClient, run: str) -> Iterator[tuple[str, int]]:
    """Three entries in two streams of service ``sdk-py-<run>``."""
    base = now_ns()
    service = f"sdk-py-{run}"
    client.push(
        [
            sdk.LokiStream(
                {"service_name": service, "level": "info"},
                [
                    sdk.LokiEntry(f"hello {run} first", base, {"request_id": f"req-{run}"}),
                    sdk.LokiEntry(f'{{"msg": "json {run}", "status": 503}}', base + 1_000_000),
                ],
            ),
            sdk.LokiStream(
                {"service_name": service, "level": "error"},
                [sdk.LokiEntry(f"boom {run} err", base + 2_000_000)],
            ),
        ]
    )
    return service, base


# --------------------------------------------------------------------------- push + query_range


def test_push_json_and_query_range_round_trip(
    client: sdk.LokiClient, pushed: tuple[str, int], run: str
) -> None:
    service, base = pushed
    result = client.query_range(
        f'{{service_name="{service}"}}', start=base, end=base + 10_000_000, direction="forward"
    )
    assert result.result_type == "streams"
    assert "categorize-labels" in result.encoding_flags
    got = entries(result)
    assert len(got) == 3
    first = next(e for _, e in got if e.line == f"hello {run} first")
    assert first.timestamp_ns == base
    assert first.structured_metadata["request_id"] == f"req-{run}"
    labels = {frozenset(lbl.items()) for lbl, _ in got}
    assert frozenset({"service_name": service, "level": "error"}.items()) in labels
    # limit counts entries across streams; end is exclusive
    assert len(entries(client.query_range(f'{{service_name="{service}"}}', limit=2))) == 2
    only_first = client.query_range(f'{{service_name="{service}"}}', start=base, end=base + 1)
    assert [e.line for _, e in entries(only_first)] == [f"hello {run} first"]


def test_push_gzip_and_protobuf(client: sdk.LokiClient, run: str) -> None:
    service = f"sdk-py-enc-{run}"
    base = now_ns()
    client.push(
        [sdk.LokiStream({"service_name": service}, [sdk.LokiEntry(f"gzip {run}", base)])],
        gzip=True,
    )
    client.push(
        [
            sdk.LokiStream(
                {"service_name": service, "env": "proto"},
                [sdk.LokiEntry(f"proto {run}", base + 5, {"k": "v ü"})],
            )
        ],
        format="protobuf",
    )
    got = entries(client.query_range(f'{{service_name="{service}"}}', start=base))
    lines = {e.line: e for _, e in got}
    assert set(lines) == {f"gzip {run}", f"proto {run}"}
    proto = lines[f"proto {run}"]
    assert proto.timestamp_ns == base + 5
    # "env" is not in the allowlist: the guard moved it to structured metadata
    assert proto.structured_metadata["k"] == "v ü"
    assert proto.structured_metadata["env"] == "proto"


def test_cardinality_guard_and_label_validation(client: sdk.LokiClient, run: str) -> None:
    service = f"sdk-py-guard-{run}"
    base = now_ns()
    client.push([sdk.LokiStream({"service_name": service, "run": run}, [sdk.LokiEntry("g", base)])])
    ((labels, entry),) = entries(client.query_range(f'{{service_name="{service}"}}', start=base))
    assert "run" not in labels
    assert entry.structured_metadata["run"] == run
    with pytest.raises(sdk.ValidationError):
        client.push([sdk.LokiStream({"service_name": ""}, [sdk.LokiEntry("x")])])
    # server-side validation: too old entries are rejected with 400 → ValidationError
    old = now_ns() - 30 * 24 * 3600 * 1_000_000_000
    with pytest.raises(sdk.ValidationError) as info:
        client.push([sdk.LokiStream({"service_name": service}, [sdk.LokiEntry("old", old)])])
    assert info.value.http_status == 400


def test_push_otlp_round_trip(client: sdk.LokiClient, run: str) -> None:
    service = f"sdk-py-otlp-{run}"
    trace_id = uuid.uuid4().hex
    client.push_otlp(
        [
            sdk.LogRecord(
                body=f"otlp {run}",
                severity=sdk.Severity.ERROR,
                attributes={"request.id": f"o-{run}", "http.status_code": 503},
                resource={"service.name": service, "deployment.environment": "test"},
                trace_id=trace_id,
                span_id="00f067aa0ba902b7",
                trace_flags=1,
            )
        ],
        gzip=True,
    )
    result = client.query_range(f'{{service_name="{service}"}} | trace_id="{trace_id}"')
    ((labels, entry),) = entries(result)
    assert labels["deployment_environment"] == "test"
    assert entry.line == f"otlp {run}"
    assert entry.structured_metadata["request_id"] == f"o-{run}"
    assert entry.structured_metadata["http_status_code"] == "503"
    with pytest.raises(sdk.ValidationError) as info:  # an export without any stream → 422
        client.request(
            sdk.Request(
                "POST",
                "/otlp/v1/logs",
                content=b'{"resourceLogs":[]}',
                headers={"Content-Type": "application/json"},
            )
        )
    assert info.value.http_status == 422


def test_query_metric_and_log_selector(client: sdk.LokiClient, pushed: tuple[str, int]) -> None:
    service, _ = pushed
    # metric queries are eventually consistent: poll until all 3 pushed lines count
    q = f'sum(count_over_time({{service_name="{service}"}}[5m]))'
    result = until(lambda: (r := client.query(q)).vector and r.vector[0].value.value == 3.0 and r)
    assert result.result_type == "vector"
    with pytest.raises(sdk.ValidationError):
        client.query(f'{{service_name="{service}"}}')
    with pytest.raises(sdk.ValidationError) as info:
        client.query("sum(")
    assert info.value.http_status == 400
    matrix = client.query_range(
        f'sum by (level) (count_over_time({{service_name="{service}"}}[1m]))', step_s=60
    )
    assert matrix.result_type == "matrix"
    assert {s.metric["level"] for s in matrix.matrix} == {"info", "error"}


def test_logql_builder_and_format_query(
    client: sdk.LokiClient, pushed: tuple[str, int], run: str
) -> None:
    service, base = pushed
    q = sdk.LogQL.selector({"service_name": service}).line_contains("json").json()
    q = q.where("status", ">=", 500)
    assert client.format_query(str(q)) == (
        f'{{service_name="{service}"}} |= "json" | json | status>=500'
    )
    ((_, entry),) = entries(client.query_range(str(q), start=base))
    assert entry.parsed["status"] == "503"
    nasty = 'quote " back \\ tab \t ü 🚀'
    client.push(
        [sdk.LokiStream({"service_name": service}, [sdk.LokiEntry(f"{run} {nasty}", base + 9)])]
    )
    found = client.query_range(
        str(sdk.LogQL.selector({"service_name": service}).line_contains(nasty)), start=base
    )
    assert [e.line for _, e in entries(found)] == [f"{run} {nasty}"]
    with pytest.raises(sdk.ValidationError):
        client.format_query("{app=")


def test_labels_label_values_series(client: sdk.LokiClient, pushed: tuple[str, int]) -> None:
    service, base = pushed
    assert {"service_name", "level"} <= set(client.labels())
    assert "request_id" not in client.labels(start=base)
    assert service in client.label_values("service_name")
    scoped = client.label_values("level", query=f'{{service_name="{service}"}}', since_s=600)
    assert sorted(scoped) == ["error", "info"]
    assert client.label_values("does_not_exist") == []
    with pytest.raises(sdk.ValidationError):
        client.label_values("bad-name")
    series = client.series([f'{{service_name="{service}"}}'], start=base)
    assert sorted(s["level"] for s in series) == ["error", "info"]


def test_index_stats_volume_volume_range(client: sdk.LokiClient, pushed: tuple[str, int]) -> None:
    service, base = pushed
    selector = f'{{service_name="{service}"}}'
    start = base - 600_000_000_000
    # volume is eventually consistent (about a second after the push)
    volume = until(lambda: (v := client.volume(selector, start=start)).vector and v)
    assert volume.result_type == "vector"
    assert volume.vector[0].value.value > 0
    by_level = until(
        lambda: (
            (
                b := client.volume(
                    selector, start=start, target_labels=["level"], aggregate_by="labels", limit=10
                )
            ).vector
            and {s.metric.get("level") for s in b.vector} == {""}
            and b
        )
    )
    assert {s.metric.get("level") for s in by_level.vector} == {""}
    # like the Rust/Java suites: an end in the future and a longer budget (volume_range lags volume)
    ranged = until(
        lambda: (
            (
                r := client.volume_range(
                    selector, start=start, end=time.time_ns() + 60_000_000_000, step_s=10
                )
            ).matrix
            and r
        ),
        timeout=60,
    )
    assert ranged.result_type == "matrix"
    assert any(p.value > 0 for series in ranged.matrix for p in series.values)
    # index/stats reads the TSDB index only: flush the ingester, then the streams appear
    client.request(sdk.Request("POST", "/flush", operation="flush"))
    stats = until(
        lambda: (s := client.index_stats(selector, start=start)).streams >= 2 and s, timeout=30
    )
    assert stats.entries >= 3
    assert stats.chunks >= 1


def test_tail_round_trip(client: sdk.LokiClient, run: str) -> None:
    service = f"sdk-py-tail-{run}"
    frames: list[sdk.TailFrame] = []
    got = threading.Event()

    def on_entries(frame: sdk.TailFrame) -> None:
        frames.append(frame)
        got.set()

    session = client.tail(f'{{service_name="{service}"}}', on_entries)
    try:
        time.sleep(0.5)
        client.push(
            [
                sdk.LokiStream(
                    {"service_name": service}, [sdk.LokiEntry(f"tail {run}", None, {"m": "1"})]
                )
            ]
        )
        assert got.wait(15)
        entry = frames[0].streams[0].entries[0]
        assert entry.line == f"tail {run}"
        assert entry.structured_metadata["m"] == "1"
        assert frames[0].streams[0].labels["service_name"] == service
    finally:
        session.stop()
        session.stop()
    assert session.done
    assert session.error is None
    with pytest.raises(sdk.ValidationError):
        client.tail(f'count_over_time({{service_name="{service}"}}[1m])', on_entries)
    with pytest.raises(sdk.ValidationError):
        client.tail("{app=", on_entries)  # server answers the handshake with 400


# --------------------------------------------------------------------------- tenants + gateway


def test_multi_tenancy_with_tenant(env: dict[str, str], run: str) -> None:
    tenant_a = env["GROW_GRAFANA_LOKI_TENANT_A"]
    tenant_b = env["GROW_GRAFANA_LOKI_TENANT_B"]
    service = f"sdk-py-mt-{run}"
    with mt_client(env) as root:
        a, b = root.with_tenant(tenant_a), root.with_tenant(tenant_b)
        assert a.tenant_id == tenant_a
        assert root.tenant_id is None
        a.push([sdk.LokiStream({"service_name": service}, [sdk.LokiEntry(f"mt {run}")])])
        sel = f'{{service_name="{service}"}}'
        assert [e.line for _, e in entries(a.query_range(sel))] == [f"mt {run}"]
        assert entries(b.query_range(sel)) == []
        assert service in a.label_values("service_name")
        with pytest.raises(sdk.AuthenticationError):
            root.ping()
        with pytest.raises(sdk.AuthenticationError):
            root.tail(sel, lambda f: None)
        with pytest.raises(sdk.ValidationError):
            root.with_tenant("a|b")
        frames: list[sdk.TailFrame] = []
        session = a.tail(sel, frames.append, start=now_ns() - 60_000_000_000)
        try:
            until(lambda: frames, timeout=15)
        finally:
            session.stop()
        assert frames[0].streams[0].entries[0].line == f"mt {run}"
        assert a.health().healthy
        assert root.ready()
    # tenant from the environment
    with core_client(
        {
            "GROW_GRAFANA_LOKI_URL": env["GROW_GRAFANA_LOKI_MULTI_TENANT_URL"],
            "LOKI_TENANT_ID": tenant_a,
        }
    ) as from_env:
        assert entries(from_env.query_range(f'{{service_name="{service}"}}'))


def test_gateway_basic_bearer_and_faults(env: dict[str, str], run: str) -> None:
    user = env["GROW_GRAFANA_LOKI_AUTH_PROXY_USERNAME"]
    password = env["GROW_GRAFANA_LOKI_AUTH_PROXY_PASSWORD"]
    token = env["GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN"]
    service = f"sdk-py-gw-{run}"
    with sdk.LokiClient(gateway_config(env, username=user, password=password)) as basic:
        assert basic.ping() >= 0
        assert basic.health().healthy
    with sdk.LokiClient(gateway_config(env, bearer_token=token)) as bearer:
        bearer.push([sdk.LokiStream({"service_name": service}, [sdk.LokiEntry(f"gw {run}")])])
        sel = f'{{service_name="{service}"}}'
        assert [e.line for _, e in entries(bearer.query_range(sel))] == [f"gw {run}"]
        frames: list[sdk.TailFrame] = []
        session = bearer.tail(sel, frames.append, start=now_ns() - 60_000_000_000)
        try:
            until(lambda: frames, timeout=15)
        finally:
            session.stop()
    with sdk.LokiClient(gateway_config(env)) as anonymous:
        with pytest.raises(sdk.AuthenticationError):
            anonymous.tail(f'{{service_name="{service}"}}', lambda f: None)
    retry = sdk.RetryPolicy(max_retries=1, initial_backoff=0.01)
    for fault, cls in (("429", sdk.RateLimitError), ("503", sdk.UnavailableError)):
        cfg = gateway_config(env, bearer_token=token, headers={"X-Grow-Fault": fault}, retry=retry)
        with sdk.LokiClient(cfg) as faulty:
            started = time.monotonic()
            with pytest.raises(cls) as info:
                faulty.ping()
            assert info.value.retry_after == 1.0
            assert time.monotonic() - started >= 0.9  # honoured Retry-After once
            assert token not in str(info.value)


# --------------------------------------------------------------------------- logging category


def test_log_client_logger_search_and_cursor(client: sdk.LokiClient, run: str) -> None:
    service = f"sdk-py-log-{run}"
    log_client = client.log_client(
        resource={"service.name": service, "service.version": "1.2.3"},
        options=sdk.BatchOptions(flush_interval_s=0.2),
    )
    logger = log_client.logger("integration").child({"run": run})
    for i in range(5):
        logger.warn(f"cursor {run} #{i}", {"i": i, "password": "hunter2"})
    log_client.send_batch([sdk.LogInput(f"batch {run} {i}") for i in range(2)])
    log_client.flush(10)
    assert log_client.stats().sent == 7
    assert log_client.stats().failed == 0
    q = client.log_query()
    record = q.wait_for_log(f"cursor {run} #4", timeout_s=20, interval_s=0.25)
    assert record.severity == sdk.Severity.WARN
    assert record.resource["service.name"] == service
    assert record.attributes["run"] == run
    assert record.attributes["i"] == "4"
    assert record.attributes["password"] == "[REDACTED]"
    assert record.attributes["service_version"] == "1.2.3"
    # page through the 5 records, 2 per page, newest first
    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = q.search(f'{{service_name="{service}"}} |= "cursor"', limit=2, cursor=cursor)
        seen += [r.body for r in page.records]
        cursor = page.next_cursor
        if not cursor:
            break
    assert seen == [f"cursor {run} #{i}" for i in range(4, -1, -1)]
    asc = q.search(f"cursor {run}", order="asc", limit=10)
    assert [r.body for r in asc.records] == [f"cursor {run} #{i}" for i in range(5)]
    assert f'service_name="{service}"' in " ".join(q.list_streams())
    assert "service_name" in q.list_labels()
    received: list[sdk.LogRecord] = []
    handle = q.tail(f'{{service_name="{service}"}}', received.append)
    try:
        time.sleep(0.5)
        logger.error(f"tailed {run}")
        log_client.flush(10)
        until(lambda: received, timeout=15)
    finally:
        handle.stop()
    assert received[0].body == f"tailed {run}"
    assert received[0].severity == sdk.Severity.ERROR
    client.close()  # closes the log client too
    assert log_client.stats().queued == 0


def test_otlp_log_client(client: sdk.LokiClient, run: str) -> None:
    service = f"sdk-py-otlplog-{run}"
    log_client = client.otlp_log_client(resource={"service.name": service})
    log_client.send(
        sdk.LogInput(
            f"otlp client {run}",
            severity=sdk.Severity.ERROR,
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            span_id="00f067aa0ba902b7",
        )
    )
    log_client.close(10)
    record = client.log_query().wait_for_log(
        f'{{service_name="{service}"}} | trace_id="4bf92f3577b34da6a3ce929d0e0e4736"',
        timeout_s=20,
        interval_s=0.25,
    )
    assert record.body == f"otlp client {run}"
    assert record.severity == sdk.Severity.ERROR
    assert record.span_id == "00f067aa0ba902b7"


# --------------------------------------------------------------------------- async client


def test_async_round_trips(env: dict[str, str], run: str) -> None:
    service = f"sdk-py-async-{run}"

    async def main() -> None:
        async with async_core_client(env) as c:
            base = now_ns()
            await c.push(
                [sdk.LokiStream({"service_name": service}, [sdk.LokiEntry(f"a {run}", base)])],
                format="protobuf",
            )
            await c.push_otlp([sdk.LogRecord(body=f"o {run}", resource={"service.name": service})])
            sel = f'{{service_name="{service}"}}'
            # queries are eventually consistent: poll like the sync suite does
            got: list[tuple[dict[str, str], sdk.StreamEntry]] = []
            metric = None
            for _ in range(80):
                got = entries(await c.query_range(sel, start=base, direction="forward"))
                metric = await c.query(f"sum(count_over_time({sel}[5m]))")
                if len(got) == 2 and metric.vector and metric.vector[0].value.value == 2.0:
                    break
                await asyncio.sleep(0.25)
            assert sorted(e.line for _, e in got) == [f"a {run}", f"o {run}"]
            assert metric is not None and metric.vector[0].value.value == 2.0
            assert "service_name" in await c.labels()
            assert service in await c.label_values("service_name")
            assert (await c.series(sel))[0]["service_name"] == service
            # volume_range lags volume: same 60 s budget and future end as the sync suite
            for _ in range(240):
                vol = await c.volume(sel, start=base - 600_000_000_000)
                vrange = await c.volume_range(
                    sel,
                    start=base - 600_000_000_000,
                    end=time.time_ns() + 60_000_000_000,
                    step_s=10,
                )
                if vol.vector and vrange.matrix:
                    break
                await asyncio.sleep(0.25)
            assert vol.vector
            assert vrange.matrix
            assert isinstance((await c.index_stats(sel)).streams, int)
            assert await c.format_query(sel) == sel
            frames: list[sdk.TailFrame] = []
            session = await c.tail(sel, frames.append, start=base - 1)
            for _ in range(60):
                if frames:
                    break
                await asyncio.sleep(0.25)
            await session.stop()
            assert frames
            assert session.done
            log_client = c.log_client(resource={"service.name": service})
            log_client.send(sdk.LogInput(f"async log {run}", attributes={"k": "v"}))
            await log_client.flush(10)
            q = c.log_query()
            record = await q.wait_for_log(f"async log {run}", timeout_s=20, interval_s=0.25)
            assert record.attributes["k"] == "v"
            page = await q.search(sel, limit=1)
            assert len(page.records) == 1
            assert page.next_cursor
            assert any(service in s for s in await q.list_streams())
            assert "service_name" in await q.list_labels()
            received: list[sdk.LogRecord] = []
            handle = await q.tail(sel, received.append)
            await asyncio.sleep(0.5)
            await c.push([sdk.LokiStream({"service_name": service}, [sdk.LokiEntry("live")])])
            for _ in range(60):
                if received:
                    break
                await asyncio.sleep(0.25)
            await handle.stop()
            assert received
            assert received[0].body == "live"
            otlp_client = c.otlp_log_client(resource={"service.name": service})
            otlp_client.send(sdk.LogInput(f"async otlp {run}"))
            await otlp_client.flush(10)
            await q.wait_for_log(f"async otlp {run}", timeout_s=20, interval_s=0.25)
        mt = sdk.AsyncLokiClient(sdk.LokiConfig(url=env["GROW_GRAFANA_LOKI_MULTI_TENANT_URL"]))
        async with mt:
            tenant = mt.with_tenant(env["GROW_GRAFANA_LOKI_TENANT_B"])
            await tenant.push([sdk.LokiStream({"service_name": service}, [sdk.LokiEntry("t")])])
            assert entries(await tenant.query_range(f'{{service_name="{service}"}}'))
            with pytest.raises(sdk.AuthenticationError):
                await mt.ping()

    asyncio.run(main())
