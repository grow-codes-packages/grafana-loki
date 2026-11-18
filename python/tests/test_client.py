"""LokiClient / AsyncLokiClient against a mock Loki built from the recorded fixtures."""

from __future__ import annotations

import base64
import datetime as dt
import gzip
import json
from typing import Any
from urllib.parse import parse_qs

import cramjam
import pytest

import grow_grafana_loki as sdk
from _loki import aclient, client, fx, fx_reply, json_reply, last, query_of, text_reply
from _support import MockServer, Reply, closed_port_url

PUSH = "/loki/api/v1/push"
RANGE = "/loki/api/v1/query_range"
PASSWORD = "gateway-password-123"


def stream(**labels: str) -> sdk.LokiStream:
    return sdk.LokiStream(labels or {"service_name": "svc"}, [sdk.LokiEntry("hello", 1_000)])


# --------------------------------------------------------------------------- core


def test_health_ready_ping_build_info(server: MockServer) -> None:
    server.defaults["/ready"] = fx_reply("ready.ok")
    server.defaults["/loki/api/v1/labels"] = fx_reply("ping.ok-tenant")
    server.defaults["/loki/api/v1/status/buildinfo"] = fx_reply("buildInfo.ok")
    with client(server, username="u", password=PASSWORD, tenant_id="team-a") as c:
        health = c.health()
        assert health == sdk.HealthStatus(True, "ok", {"httpStatus": "200", "message": "ready"})
        assert c.ready() is True
        assert c.ping() >= 0
        info = c.build_info()
        assert info == sdk.BuildInfo(
            "3.7.10", "c792f585", "release-3.7.x", "root@buildkitsandbox", "2026-11-13T00:20:40Z"
        )
        assert c.server_version() == "3.7.10"
        assert c.tenant_id == "team-a"
    ping = last(server, "/loki/api/v1/labels")
    assert query_of(ping) == {"since": ["5m"]}
    expected = base64.b64encode(f"u:{PASSWORD}".encode()).decode()
    assert ping.headers["Authorization"] == f"Basic {expected}"
    assert ping.headers["X-Scope-OrgID"] == "team-a"
    assert last(server, "/ready").headers["Authorization"].startswith("Basic ")


def test_health_not_ready_and_unreachable(server: MockServer) -> None:
    starting = "Ingester not ready: waiting for 15s after being ready"
    server.enqueue("/ready", text_reply(starting + "\n", 503), text_reply(starting, 503))
    c = client(server)
    status = c.health()
    assert status.healthy is False
    assert status.status == starting
    assert status.details == {"httpStatus": "503", "message": starting}
    assert c.ready() is False
    server.enqueue("/ready", Reply(500, b""))
    assert c.health().status == "unhealthy"
    assert client_unreachable().health().status == "unreachable"


def client_unreachable() -> sdk.LokiClient:
    return sdk.LokiClient(sdk.LokiConfig(url=closed_port_url()))


def test_ready_raises_for_other_statuses(server: MockServer) -> None:
    server.enqueue("/ready", fx_reply("ping.gateway-bad-credentials"))
    with pytest.raises(sdk.AuthenticationError):
        client(server).ready()
    server.enqueue("/ready", text_reply("rpc error: Ingester is shutting down", 500))
    with pytest.raises(sdk.UnavailableError) as info:
        client(server).ready()
    assert info.value.retryable


def test_health_raises_cancelled_when_closed(server: MockServer) -> None:
    c = client(server)
    c.close()
    c.close()
    with pytest.raises(sdk.CancelledError):
        c.health()
    with pytest.raises(sdk.CancelledError):
        c.ping()


def test_ping_bad_credentials_redacted(server: MockServer) -> None:
    server.enqueue("/loki/api/v1/labels", fx_reply("ping.gateway-bad-credentials"))
    c = client(server, username="u", password=PASSWORD)
    with pytest.raises(sdk.AuthenticationError) as info:
        c.ping()
    assert info.value.http_status == 401
    assert PASSWORD not in str(info.value)
    assert PASSWORD not in repr(c)


def test_build_info_unsupported_and_missing_tenant(server: MockServer) -> None:
    server.enqueue("/loki/api/v1/status/buildinfo", fx_reply("http.not-found"))
    with pytest.raises(sdk.UnsupportedError):
        client(server).server_version()
    server.enqueue("/loki/api/v1/status/buildinfo", fx_reply("ping.no-tenant"))
    with pytest.raises(sdk.AuthenticationError) as info:
        client(server).build_info()
    assert "no org id" in str(info.value)


def test_with_tenant(server: MockServer) -> None:
    server.defaults["/loki/api/v1/labels"] = fx_reply("labels.ok")
    root = client(server, bearer_token="tok-12345")
    tenant = root.with_tenant("tenant-b")
    assert "service_name" in tenant.labels()
    seen = last(server, "/loki/api/v1/labels")
    assert seen.headers["X-Scope-OrgID"] == "tenant-b"
    assert seen.headers["Authorization"] == "Bearer tok-12345"
    root.labels()
    assert "X-Scope-OrgID" not in last(server, "/loki/api/v1/labels").headers
    for bad in ("a|b", "", "..", "x" * 151, "bad/tenant"):
        with pytest.raises(sdk.ValidationError):
            root.with_tenant(bad)
    multi = client(server, multi_tenant_queries=True).with_tenant("a|b")
    assert multi.tenant_id == "a|b"
    tenant.close()  # derived clients do not close the shared pool
    assert root.labels()
    root.close()


# --------------------------------------------------------------------------- push


def test_push_json(server: MockServer) -> None:
    server.defaults[PUSH] = fx_reply("push.json")
    c = client(server, tenant_id="t1")
    c.push(
        [
            sdk.LokiStream(
                {"service_name": "svc", "level": "info", "run": "r1"},
                [
                    sdk.LokiEntry("first", 1795034767064713072, {"request_id": "req-1"}),
                    sdk.LokiEntry("second", dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)),
                ],
            )
        ]
    )
    seen = last(server, PUSH)
    assert seen.headers["Content-Type"] == "application/json"
    assert seen.headers["X-Scope-OrgID"] == "t1"
    body = json.loads(seen.body)
    (s,) = body["streams"]
    assert s["stream"] == {"service_name": "svc", "level": "info"}
    assert s["values"][0] == ["1795034767064713072", "first", {"run": "r1", "request_id": "req-1"}]
    assert s["values"][1] == ["1767225600000000000", "second", {"run": "r1"}]
    c.push([])  # nothing to send
    assert len([x for x in server.seen if x.path == PUSH]) == 1


def test_push_default_timestamp_gzip_and_guard_off(server: MockServer) -> None:
    server.defaults[PUSH] = fx_reply("push.json-gzip")
    c = client(server, disable_label_guard=True)
    c.push([sdk.LokiStream({"env": "test"}, [sdk.LokiEntry("now")])], gzip=True)
    seen = last(server, PUSH)
    assert seen.headers["Content-Encoding"] == "gzip"
    body = json.loads(gzip.decompress(seen.body))
    assert body["streams"][0]["stream"] == {"env": "test"}
    ts = int(body["streams"][0]["values"][0][0])
    assert abs(ts - dt.datetime.now(dt.timezone.utc).timestamp() * 1e9) < 60e9
    assert len(body["streams"][0]["values"][0]) == 2  # no metadata element


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    value = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7


def parse_pb(data: bytes) -> list[tuple[int, Any]]:
    """Minimal protobuf reader: [(field, int | bytes)]."""
    out: list[tuple[int, Any]] = []
    pos = 0
    while pos < len(data):
        key, pos = _read_varint(data, pos)
        if key & 7 == 0:
            value, pos = _read_varint(data, pos)
            out.append((key >> 3, value))
        else:
            length, pos = _read_varint(data, pos)
            out.append((key >> 3, data[pos : pos + length]))
            pos += length
    return out


def test_push_protobuf(server: MockServer) -> None:
    server.defaults[PUSH] = fx_reply("push.protobuf-snappy")
    c = client(server)
    c.push(
        [
            sdk.LokiStream(
                {"service_name": 'sdk "verify"'},
                [sdk.LokiEntry("pb line", 1795034767064713072, {"b": "2", "a": "1"})],
            )
        ],
        format="protobuf",
    )
    seen = last(server, PUSH)
    assert seen.headers["Content-Type"] == "application/x-protobuf"
    raw = bytes(cramjam.snappy.decompress_raw(seen.body))
    ((field, stream_bytes),) = parse_pb(raw)
    assert field == 1
    fields = parse_pb(stream_bytes)
    assert fields[0] == (1, b'{service_name="sdk \\"verify\\""}')
    entry = parse_pb(fields[1][1])
    stamp = dict(parse_pb(entry[0][1]))
    assert stamp == {1: 1795034767, 2: 64713072}
    assert entry[1] == (2, b"pb line")
    pairs = [dict(parse_pb(v)) for f, v in entry[2:]]
    assert pairs == [{1: b"a", 2: b"1"}, {1: b"b", 2: b"2"}]


def test_push_validation(server: MockServer) -> None:
    c = client(server)
    with pytest.raises(sdk.ValidationError, match="unknown push format"):
        c.push([stream()], format="xml")  # type: ignore[arg-type]
    with pytest.raises(sdk.ValidationError, match="stream 0"):
        c.push([sdk.LokiStream({"service_name": ""}, [sdk.LokiEntry("x")])])
    guard_off = client(server, disable_label_guard=True)
    for labels in ({"bad-label": "x"}, {"__name__": "x"}):
        with pytest.raises(sdk.ValidationError):
            guard_off.push([sdk.LokiStream(labels, [sdk.LokiEntry("x")])])
    bad_entries: list[sdk.LokiEntry] = [
        sdk.LokiEntry(1),  # type: ignore[arg-type]
        sdk.LokiEntry("x", None, {"": "v"}),
        sdk.LokiEntry("x", None, {"k": 1}),  # type: ignore[dict-item]
    ]
    for entry in bad_entries:
        with pytest.raises(sdk.ValidationError):
            c.push([sdk.LokiStream({"service_name": "s"}, [entry])])
    with pytest.raises(sdk.ValidationError):
        c.push([sdk.LokiStream({"service_name": "s"}, [sdk.LokiEntry("x", -5)])])
    assert server.seen == []


@pytest.mark.parametrize(
    "name",
    [
        "push.invalid-label-name",
        "push.too-old",
        "push.too-far-future",
        "push.metadata-non-string",
        "push.numeric-timestamp",
        "push.mt-invalid-tenant-slash",
        "push.protobuf-not-snappy",
    ],
)
def test_push_server_errors_are_validation(server: MockServer, name: str) -> None:
    server.enqueue(PUSH, fx_reply(name))
    with pytest.raises(sdk.ValidationError) as info:
        client(server).push([stream()])
    assert info.value.http_status == 400
    text = fx(name)["response"]["body"].strip()
    assert text[:40] in str(info.value)


def test_push_statuses(server: MockServer) -> None:
    c = client(server)
    server.enqueue(PUSH, fx_reply("push.mt-no-tenant"))
    with pytest.raises(sdk.AuthenticationError):
        c.push([stream()])
    server.enqueue(PUSH, Reply(260, b""))
    with pytest.raises(sdk.LokiError) as blocked:
        c.push([stream()])
    assert blocked.value.http_status == 260
    assert blocked.value.retryable is False
    shutting = "rpc error: code = Unknown desc = Ingester is shutting down"
    server.defaults[PUSH] = text_reply(shutting, 500)
    with pytest.raises(sdk.UnavailableError) as info:
        c.push([stream()])
    assert info.value.retryable is True
    assert "WAL disk" in str(info.value)
    assert len([s for s in server.seen if s.path == PUSH]) == 2 + 4  # push is replayed
    rate = (
        "Ingestion rate limit exceeded for user fake (limit: 4194304 bytes/sec) while attempting "
        "to ingest '1' lines totaling '1' bytes, reduce log volume or contact your Loki "
        "administrator to see if the limit can be increased"
    )
    server.defaults[PUSH] = text_reply(rate, 429)
    with pytest.raises(sdk.RateLimitError) as limited:
        client(server, retry=sdk.RetryPolicy(max_retries=0)).push([stream()])
    assert "Ingestion rate limit exceeded" in str(limited.value)


def test_push_otlp(server: MockServer) -> None:
    server.defaults["/otlp/v1/logs"] = fx_reply("otlpLogs.json")
    record = sdk.LogRecord(
        body="otlp body",
        severity=sdk.Severity.ERROR,
        timestamp=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        attributes={"request.id": "r-1", "code": 503, "ratio": 0.5, "ok": True, "tags": ["a"]},
        resource={"service.name": "svc", "nested": {"x": 1}},
        trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        span_id="00f067aa0ba902b7",
        trace_flags=1,
    )
    c = client(server, tenant_id="t")
    c.push_otlp([record, sdk.LogRecord(body="other", resource={"service.name": "x"})], gzip=True)
    seen = last(server, "/otlp/v1/logs")
    assert seen.headers["X-Scope-OrgID"] == "t"
    body = json.loads(gzip.decompress(seen.body))
    assert len(body["resourceLogs"]) == 2
    first = body["resourceLogs"][0]
    assert first["resource"]["attributes"][0]["key"] == "nested"
    assert first["scopeLogs"][0]["scope"] == {"name": sdk.OTLP_SCOPE_NAME}
    item = first["scopeLogs"][0]["logRecords"][0]
    assert item["timeUnixNano"] == "1767225600000000000"
    assert item["severityNumber"] == 17
    assert item["traceId"] == record.trace_id
    assert item["flags"] == 1
    attrs = {a["key"]: a["value"] for a in item["attributes"]}
    assert attrs["code"] == {"intValue": "503"}
    assert attrs["ratio"] == {"doubleValue": 0.5}
    assert attrs["ok"] == {"boolValue": True}
    assert attrs["tags"] == {"arrayValue": {"values": [{"stringValue": "a"}]}}
    c.push_otlp([])


@pytest.mark.parametrize(
    ("name", "cls", "text"),
    [
        ("otlpLogs.empty-json", sdk.ValidationError, "at least one valid stream"),
        ("otlpLogs.invalid-body", sdk.ValidationError, "illegal wireType 6"),
        ("otlpLogs.mt-no-tenant", sdk.AuthenticationError, "no org id"),
    ],
)
def test_push_otlp_errors(server: MockServer, name: str, cls: type, text: str) -> None:
    server.enqueue("/otlp/v1/logs", fx_reply(name))
    with pytest.raises(cls) as info:
        client(server).push_otlp([sdk.LogRecord(body="x")])
    assert text in str(info.value)


def test_push_otlp_shutting_down(server: MockServer) -> None:
    status = b"\x12\x19Ingester is shutting down"
    server.defaults["/otlp/v1/logs"] = Reply(
        503, status, {"Content-Type": "application/x-protobuf"}
    )
    c = client(server, retry=sdk.RetryPolicy(max_retries=0))
    with pytest.raises(sdk.UnavailableError) as info:
        c.push_otlp([sdk.LogRecord(body="x")])
    assert info.value.retryable


# --------------------------------------------------------------------------- query


def test_query_metric_vector_and_scalar(server: MockServer) -> None:
    server.enqueue(
        "/loki/api/v1/query", fx_reply("query.metric-vector"), fx_reply("query.scalar-expr")
    )
    c = client(server)
    result = c.query(
        'sum by (level) (count_over_time({run="x"}[5m]))', time=5, limit=10, direction="forward"
    )
    assert result.result_type == "vector"
    assert result.vector[1].metric == {"level": "error"}
    assert result.vector[2].value.value == 8.0
    assert isinstance(result.vector[0].value.value, float)
    assert result.stats
    seen = last(server, "/loki/api/v1/query")
    assert query_of(seen)["time"] == ["5"]
    assert query_of(seen)["direction"] == ["forward"]
    assert seen.headers["X-Loki-Response-Encoding-Flags"] == "categorize-labels"
    scalar = c.query("vector(1)+1")
    assert scalar.vector[0].value.value == 2.0


def test_query_client_side_validation(server: MockServer) -> None:
    c = client(server)
    with pytest.raises(sdk.ValidationError, match="query_range"):
        c.query('{app="x"}')
    for kwargs in ({"limit": 0}, {"direction": "up"}, {"limit": True}):
        with pytest.raises(sdk.ValidationError):
            c.query("vector(1)", **kwargs)  # type: ignore[arg-type]
    with pytest.raises(sdk.ValidationError):
        c.query("  ")
    with pytest.raises(sdk.ValidationError):
        c.query_range('{a="b"}', start=10, end=10)
    with pytest.raises(sdk.ValidationError):
        c.query_range('{a="b"}', start="yesterday")
    with pytest.raises(sdk.ValidationError):
        c.query_range('{a="b"}', step_s=0)
    assert server.seen == []


def test_query_server_errors(server: MockServer) -> None:
    server.enqueue("/loki/api/v1/query", fx_reply("query.parse-error"))
    with pytest.raises(sdk.ValidationError, match="parse error"):
        client(server).query("sum(")
    server.enqueue(RANGE, fx_reply("queryRange.limit-too-high"))
    with pytest.raises(sdk.ValidationError, match="max entries limit"):
        client(server).query_range('{a="b"}', limit=100000)
    server.enqueue(RANGE, fx_reply("queryRange.mt-no-tenant"))
    with pytest.raises(sdk.AuthenticationError):
        client(server).query_range('{a="b"}')


def test_query_range_streams(server: MockServer) -> None:
    server.enqueue(RANGE, fx_reply("queryRange.streams-forward"))
    start = dt.datetime(2026, 11, 18, tzinfo=dt.timezone.utc)
    result = client(server).query_range(
        '{run="x"}',
        start=start,
        end="2026-11-18T01:00:00Z",
        step_s=dt.timedelta(seconds=30),
        interval_s="1m",
        limit=100,
        direction="forward",
    )
    assert result.result_type == "streams"
    assert sum(len(s.entries) for s in result.streams) == 3
    entry = result.streams[1].entries[0]
    assert entry.timestamp_ns == 1795034767064713072
    assert entry.timestamp.tzinfo is not None
    assert entry.line == "hello c023892b83 first"
    q = query_of(last(server, RANGE))
    assert q["start"] == [str(int(start.timestamp()) * 10**9)]
    assert q["end"] == [str((int(start.timestamp()) + 3600) * 10**9)]
    assert q["step"] == ["30000ms"]
    assert q["interval"] == ["1m"]
    assert q["limit"] == ["100"]


def test_query_range_since_categorized_and_matrix(server: MockServer) -> None:
    server.enqueue(
        RANGE,
        fx_reply("queryRange.categorized-labels"),
        fx_reply("queryRange.metric-matrix"),
        fx_reply("queryRange.empty"),
    )
    c = client(server)
    result = c.query_range('{run="x"} | json', since_s=90)
    assert query_of(last(server, RANGE))["since"] == ["90000ms"]
    assert result.encoding_flags == ["categorize-labels"]
    entry = result.streams[0].entries[0]
    assert entry.structured_metadata["request_id"].startswith("req2-")
    assert entry.parsed["status"] == "200"
    matrix = c.query_range('sum(count_over_time({a="b"}[1m]))', step_s=60)
    assert matrix.result_type == "matrix"
    assert matrix.matrix[0].values
    empty = c.query_range('{a="b"}')
    assert empty.streams == []


def test_labels_label_values_series(server: MockServer) -> None:
    server.enqueue("/loki/api/v1/labels", fx_reply("labels.ok"), json_reply({"status": "success"}))
    c = client(server)
    assert c.labels(start=1, end=2, query='{a="b"}') == [
        "dedup",
        "deployment_environment",
        "enc",
        "env",
        "hdr",
        "level",
        "run",
        "service_name",
    ]
    assert query_of(last(server, "/loki/api/v1/labels")) == {
        "start": ["1"],
        "end": ["2"],
        "query": ['{a="b"}'],
    }
    assert c.labels(since_s="10m") == []
    path = "/loki/api/v1/label/level/values"
    server.enqueue(path, fx_reply("labelValues.with-query"), fx_reply("labelValues.unknown-label"))
    assert c.label_values("level", query='{a="b"}') == ["error", "info"]
    assert c.label_values("level") == []
    with pytest.raises(sdk.ValidationError):
        c.label_values("bad-name")
    server.enqueue("/loki/api/v1/series", fx_reply("series.ok"), json_reply({"status": "success"}))
    series = c.series(['{run="x"}', '{env="test"}'], start=1, since_s=5)
    assert series[0]["service_name"] == "sdk-verify"
    seen = last(server, "/loki/api/v1/series")
    assert seen.method == "POST"
    assert seen.headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert parse_qs(seen.body.decode()) == {
        "match[]": ['{run="x"}', '{env="test"}'],
        "start": ["1"],
    }
    assert c.series('{a="b"}') == []
    for bad in ([], [""]):
        with pytest.raises(sdk.ValidationError):
            c.series(bad)
    server.enqueue("/loki/api/v1/series", fx_reply("series.invalid-matcher"))
    with pytest.raises(sdk.ValidationError):
        c.series("{")


def test_index_stats_volume_format(server: MockServer) -> None:
    server.enqueue("/loki/api/v1/index/stats", fx_reply("indexStats.after-flush"))
    server.enqueue("/loki/api/v1/index/volume", fx_reply("volume.target-labels"))
    server.enqueue("/loki/api/v1/index/volume_range", fx_reply("volumeRange.before-flush"))
    server.enqueue(
        "/loki/api/v1/format_query", fx_reply("formatQuery.ok"), fx_reply("formatQuery.parse-error")
    )
    c = client(server)
    assert c.index_stats('{a="b"}', start=1, end=2) == sdk.IndexStats(8, 8, 0, 13)
    volume = c.volume('{a="b"}', start=1, limit=10, target_labels=["level"], aggregate_by="labels")
    assert volume.vector[0].metric == {"level": ""}
    q = query_of(last(server, "/loki/api/v1/index/volume"))
    assert q["targetLabels"] == ["level"]
    assert q["aggregateBy"] == ["labels"]
    ranged = c.volume_range('{a="b"}', step_s=60, aggregate_by="series")
    assert ranged.result_type == "matrix"
    assert query_of(last(server, "/loki/api/v1/index/volume_range"))["step"] == ["60000ms"]
    assert c.format_query('{app="x"}|="err"') == '{app="x"} |= "err" | json | status>=500'
    with pytest.raises(sdk.ValidationError, match="parse error"):
        c.format_query("{app=")
    for call in (
        lambda: c.index_stats(""),
        lambda: c.volume("", start=1),
        lambda: c.volume('{a="b"}', aggregate_by="x"),  # type: ignore[arg-type]
        lambda: c.format_query(""),
    ):
        with pytest.raises(sdk.ValidationError):
            call()


def test_invalid_responses(server: MockServer) -> None:
    c = client(server)
    server.enqueue(
        RANGE, json_reply([1]), json_reply({"data": {"resultType": "streams", "result": 5}})
    )
    with pytest.raises(sdk.ValidationError):
        c.query_range('{a="b"}')
    with pytest.raises(sdk.ValidationError):
        c.query_range('{a="b"}')
    server.enqueue("/loki/api/v1/format_query", json_reply({"data": 1}))
    with pytest.raises(sdk.ValidationError):
        c.format_query("x")
    server.enqueue("/loki/api/v1/labels", Reply(200, b"not json", {"Content-Type": "text/plain"}))
    with pytest.raises(sdk.ValidationError):
        c.labels()


def test_request_passthrough(server: MockServer) -> None:
    server.enqueue("/flush", Reply(204, b""))
    resp = client(server).request(sdk.Request("POST", "/flush"))
    assert resp.status == 204


def test_cancel_token(server: MockServer) -> None:
    server.enqueue(
        "/loki/api/v1/labels", Reply(200, b"{}", {"Content-Type": "application/json"}, delay=2)
    )
    token = sdk.CancelToken()
    token.cancel()
    with pytest.raises(sdk.CancelledError):
        client(server).labels(cancel=token)


# --------------------------------------------------------------------------- async


async def test_async_client_round_trip(server: MockServer) -> None:
    server.defaults["/ready"] = fx_reply("ready.ok")
    server.defaults["/loki/api/v1/labels"] = fx_reply("labels.ok")
    server.defaults["/loki/api/v1/status/buildinfo"] = fx_reply("buildInfo.ok")
    server.defaults[PUSH] = Reply(204, b"")
    server.defaults["/otlp/v1/logs"] = Reply(204, b"")
    server.defaults["/loki/api/v1/query"] = fx_reply("query.metric-vector")
    server.defaults[RANGE] = fx_reply("queryRange.streams-forward")
    server.defaults["/loki/api/v1/label/level/values"] = fx_reply("labelValues.with-query")
    server.defaults["/loki/api/v1/series"] = fx_reply("series.ok")
    server.defaults["/loki/api/v1/index/stats"] = fx_reply("indexStats.after-flush")
    server.defaults["/loki/api/v1/index/volume"] = fx_reply("volume.ok")
    server.defaults["/loki/api/v1/index/volume_range"] = fx_reply("volumeRange.after-flush")
    server.defaults["/loki/api/v1/format_query"] = fx_reply("formatQuery.ok")
    server.defaults["/flush"] = Reply(204, b"")
    async with aclient(server, tenant_id="a") as c:
        assert (await c.health()).healthy
        assert await c.ready()
        assert await c.ping() >= 0
        assert await c.server_version() == "3.7.10"
        await c.push([stream()], format="protobuf")
        await c.push([])
        await c.push_otlp([sdk.LogRecord(body="x")])
        await c.push_otlp([])
        assert (await c.query("vector(1)")).vector
        assert (await c.query_range('{a="b"}', since_s=60)).streams
        assert await c.labels()
        assert await c.label_values("level") == ["error", "info"]
        assert await c.series('{a="b"}')
        assert (await c.index_stats('{a="b"}')).entries == 13
        assert (await c.volume('{a="b"}')).vector
        assert (await c.volume_range('{a="b"}', step_s=60)).matrix
        assert await c.format_query('{a="b"}')
        assert (await c.request(sdk.Request("POST", "/flush"))).status == 204
        other = c.with_tenant("b")
        await other.labels()
        assert last(server, "/loki/api/v1/labels").headers["X-Scope-OrgID"] == "b"
        await other.aclose()
    with pytest.raises(sdk.CancelledError):
        await c.ping()
    await c.close()


async def test_async_health_variants(server: MockServer) -> None:
    server.enqueue("/loki/api/v1/status/buildinfo", fx_reply("http.not-found"))
    server.enqueue("/ready", text_reply("Ingester not ready", 503))
    c = aclient(server)
    with pytest.raises(sdk.UnsupportedError):
        await c.build_info()
    assert (await c.health()).status == "Ingester not ready"
    unreachable = sdk.AsyncLokiClient(sdk.LokiConfig(url=closed_port_url()))
    assert (await unreachable.health()).status == "unreachable"
    await c.aclose()
    with pytest.raises(sdk.CancelledError):
        await c.health()


def test_from_env_and_default_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOKI_URL", "http://loki.example:3100")
    monkeypatch.setenv("LOKI_TENANT_ID", "env-tenant")
    c = sdk.LokiClient()
    assert c.config.url == "http://loki.example:3100"
    assert c.tenant_id == "env-tenant"
    a = sdk.AsyncLokiClient.from_env({"GROW_GRAFANA_LOKI_URL": "http://x:1"})
    assert a.config.url == "http://x:1"
    assert sdk.LokiClient.from_env({}, tenant_id="t").tenant_id == "t"
    assert sdk.AsyncLokiClient().config.tenant_id == "env-tenant"
    assert "LokiClient(" in repr(c)
    assert "AsyncLokiClient(" in repr(a)
    assert c.resources is not None
