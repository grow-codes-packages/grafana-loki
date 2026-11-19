"""Logging category on Loki: exporters, log clients, search with cursor, record mapping."""

from __future__ import annotations

import datetime as dt
import gzip
import json
from typing import Any

import pytest

import grow_grafana_loki as sdk
from _loki import aclient, client, fx_reply, json_reply, last, query_of, text_reply
from _support import MockServer, Reply

PUSH = "/loki/api/v1/push"
RANGE = "/loki/api/v1/query_range"
UTC = dt.timezone.utc


def streams_body(*streams: tuple[dict[str, str], list[list[Any]]]) -> dict[str, Any]:
    return {
        "status": "success",
        "data": {
            "resultType": "streams",
            "encodingFlags": ["categorize-labels"],
            "result": [{"stream": s, "values": v} for s, v in streams],
        },
    }


def test_to_logql_and_cursor() -> None:
    assert sdk.to_logql(' {a="b"} |= "x" ') == '{a="b"} |= "x"'
    assert sdk.to_logql('free "text"') == '{service_name=~".+"} |= "free \\"text\\""'
    assert sdk.to_logql("  ") == sdk.STREAM_SELECTOR_ALL
    from grow_grafana_loki._logs import decode_cursor, encode_cursor

    assert decode_cursor(encode_cursor(1795034767064713072, 3)) == (1795034767064713072, 3)
    for bad in ("!!!", encode_cursor(1, 2)[:-1] + "@", "eA", "LTE6LTE"):
        with pytest.raises(sdk.ValidationError):
            decode_cursor(bad)


def test_entry_to_record_mapping() -> None:
    entry = sdk.StreamEntry(
        1767225600000000000,
        "line",
        {
            "severity_number": "13",
            "severity_text": "WARNING",
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "span_id": "00f067aa0ba902b7",
            "trace_flags": "1",
            "request_id": "r",
            "detected_level": "warn",
        },
        {"status": "503"},
    )
    labels = {"service_name": "svc", "deployment_environment": "prod", "level": "warn", "x": "y"}
    record = sdk.entry_to_record(labels, entry)
    assert record.body == "line"
    assert record.timestamp == dt.datetime(2026, 1, 1, tzinfo=UTC)
    assert record.severity == sdk.Severity.WARN
    assert record.severity_text == "WARNING"
    assert record.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert record.span_id == "00f067aa0ba902b7"
    assert record.trace_flags == 1
    assert record.attributes == {"status": "503", "request_id": "r", "detected_level": "warn"}
    assert record.resource == {
        "service.name": "svc",
        "deployment.environment": "prod",
        "level": "warn",
        "x": "y",
    }
    detected = sdk.entry_to_record({}, sdk.StreamEntry(1, "l", {"detected_level": "error"}))
    assert (detected.severity, detected.severity_text) == (sdk.Severity.ERROR, "ERROR")
    from_label = sdk.entry_to_record({"level": "debug"}, sdk.StreamEntry(1, "l"))
    assert from_label.severity == sdk.Severity.DEBUG
    bogus = sdk.entry_to_record(
        {"level": "loud"},
        sdk.StreamEntry(1, "l", {"severity_number": "99", "detected_level": "unknown"}),
    )
    assert (bogus.severity, bogus.severity_text) == (sdk.Severity.INFO, "INFO")
    text_only = sdk.entry_to_record({}, sdk.StreamEntry(1, "l", {"severity_text": "FATAL"}))
    assert text_only.severity == sdk.Severity.FATAL


def test_search_desc_paging_with_cursor(server: MockServer) -> None:
    t = 1795034767064713072
    # page 1: 3 entries newest first, the last two share a timestamp across streams
    server.enqueue(
        RANGE,
        json_reply(
            streams_body(
                ({"service_name": "a"}, [[str(t + 2), "n2"], [str(t + 1), "a1"]]),
                ({"service_name": "b"}, [[str(t + 1), "b1"]]),
            )
        ),
        # the page was truncated: the boundary nanosecond t+1 is fetched completely
        json_reply(
            streams_body(
                ({"service_name": "a"}, [[str(t + 1), "a1"]]),
                ({"service_name": "b"}, [[str(t + 1), "b1"]]),
            )
        ),
        # page 2 re-queries end = t+2 (exclusive) and skips the two entries at t+1 already seen
        json_reply(
            streams_body(
                ({"service_name": "a"}, [[str(t + 1), "a1"], [str(t), "a0"]]),
                ({"service_name": "b"}, [[str(t + 1), "b1"]]),
            )
        ),
    )
    q = client(server).log_query()
    page1 = q.search('{service_name=~"a|b"}', limit=3)
    assert [r.body for r in page1.records] == ["n2", "a1", "b1"]
    assert page1.next_cursor
    boundary_params = query_of(last(server, RANGE))
    assert boundary_params["start"] == [str(t + 1)]
    assert boundary_params["end"] == [str(t + 2)]
    assert boundary_params["direction"] == ["backward"]
    assert boundary_params["limit"] == ["5000"]
    assert last(server, RANGE).headers["X-Loki-Response-Encoding-Flags"] == "categorize-labels"
    page2 = q.search('{service_name=~"a|b"}', limit=3, cursor=page1.next_cursor)
    assert [r.body for r in page2.records] == ["a0"]
    assert page2.next_cursor is None
    second = query_of(last(server, RANGE))
    assert second["end"] == [str(t + 2)]
    assert second["limit"] == ["5"]


def test_search_asc_cursor_accumulates_skip(server: MockServer) -> None:
    t = 100
    server.enqueue(
        RANGE,
        json_reply(streams_body(({"s": "a"}, [[str(t), "x1"], [str(t), "x2"]]))),
        # truncated inside t: the boundary nanosecond is fetched completely
        json_reply(streams_body(({"s": "a"}, [[str(t), "x1"], [str(t), "x2"], [str(t), "x3"]]))),
        json_reply(streams_body(({"s": "a"}, [[str(t), "x1"], [str(t), "x2"], [str(t), "x3"]]))),
        json_reply(streams_body(({"s": "a"}, [[str(t), "x1"], [str(t), "x2"], [str(t), "x3"]]))),
        json_reply(streams_body()),
    )
    q = client(server).log_query()
    page1 = q.search('{s="a"}', limit=2, order="asc", start=dt.datetime(2026, 1, 1, tzinfo=UTC))
    assert [r.body for r in page1.records] == ["x1", "x2"]
    page2 = q.search('{s="a"}', limit=1, order="asc", cursor=page1.next_cursor)
    assert [r.body for r in page2.records] == ["x3"]
    params = query_of(last(server, RANGE))
    assert params["start"] == [str(t)]
    assert params["end"] == [str(t + 1)]
    assert params["direction"] == ["forward"]
    from grow_grafana_loki._logs import decode_cursor

    assert page2.next_cursor is not None
    assert decode_cursor(page2.next_cursor) == (t, 3)
    empty = q.search('{s="a"}', cursor=page2.next_cursor, order="asc")
    assert empty.records == []
    for kwargs in ({"limit": 0}, {"limit": 5001}, {"order": "up"}):
        with pytest.raises(sdk.ValidationError):
            q.search("x", **kwargs)  # type: ignore[arg-type]


def test_query_client_streams_labels_wait(server: MockServer) -> None:
    server.defaults["/loki/api/v1/series"] = fx_reply("series.ok")
    server.defaults["/loki/api/v1/labels"] = fx_reply("labels.ok")
    server.enqueue(RANGE, fx_reply("queryRange.empty"), fx_reply("queryRange.categorized-labels"))
    q = client(server).log_query()
    streams = q.list_streams()
    assert streams == sorted(streams)
    assert streams[0].startswith("{")
    assert 'service_name="sdk-verify"' in streams[0]
    body = last(server, "/loki/api/v1/series").body.decode()
    assert "match%5B%5D=%7Bservice_name%3D~%22.%2B%22%7D" in body
    assert "service_name" in q.list_labels()
    record = q.wait_for_log("json line", timeout_s=5, interval_s=0.01)
    assert record.attributes["status"] in ("200", "503")


def test_log_client_exports_through_push(server: MockServer) -> None:
    server.defaults[PUSH] = Reply(204, b"")
    c = client(server)
    log_client = c.log_client(
        resource={"service.name": "svc"},
        options=sdk.BatchOptions(flush_interval_s=10, gzip=False),
    )
    log_client.logger("app").info("hello", {"password": "hunter2", "n": 1})
    log_client.send(sdk.LogInput("second", severity=sdk.Severity.ERROR))
    log_client.flush(5)
    assert log_client.stats().sent == 2
    body = json.loads(last(server, PUSH).body)
    labels = sorted(s["stream"]["level"] for s in body["streams"])
    assert labels == ["error", "info"]
    info = next(s for s in body["streams"] if s["stream"]["level"] == "info")
    meta = info["values"][0][2]
    assert meta["password"] == "[REDACTED]"
    assert meta["logger_name"] == "app"
    c.close()  # flushes and closes the log client
    assert log_client.stats().queued == 0


def test_log_client_failures_and_protobuf(server: MockServer) -> None:
    server.defaults[PUSH] = fx_reply("push.too-old")
    errors: list[Exception] = []
    c = client(server)
    log_client = c.log_client(
        options=sdk.BatchOptions(on_error=lambda e, r: errors.append(e)), format="protobuf"
    )
    log_client.send(sdk.LogInput("x"))
    log_client.flush(5)
    assert log_client.stats().failed == 1
    assert isinstance(errors[0], sdk.ValidationError)
    assert last(server, PUSH).headers["Content-Type"] == "application/x-protobuf"
    exporter = sdk.LokiLogExporter(c)
    assert exporter.export([], gzip=False, timeout_s=1) == sdk.ExportResult(accepted=0)
    exporter.shutdown()
    c.close()


def test_otlp_log_client(server: MockServer) -> None:
    server.defaults["/otlp/v1/logs"] = Reply(204, b"")
    c = client(server)
    log_client = c.otlp_log_client(resource={"service.name": "svc"})
    log_client.send(sdk.LogInput("otlp"))
    log_client.close(5)
    seen = last(server, "/otlp/v1/logs")
    assert seen.headers["Content-Encoding"] == "gzip"
    body = json.loads(gzip.decompress(seen.body))
    assert body["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["body"] == {
        "stringValue": "otlp"
    }
    exporter = sdk.LokiOtlpExporter(c)
    assert exporter.export([], gzip=True, timeout_s=1).accepted == 0
    server.defaults["/otlp/v1/logs"] = fx_reply("otlpLogs.mt-no-tenant")
    result = exporter.export([sdk.LogRecord(body="x")], gzip=True, timeout_s=1)
    assert result.rejected == 1
    assert isinstance(result.error, sdk.AuthenticationError)
    exporter.shutdown()


async def test_async_logging(server: MockServer) -> None:
    t = 1795034767064713072
    server.defaults[PUSH] = Reply(204, b"")
    server.defaults["/otlp/v1/logs"] = Reply(204, b"")
    server.defaults["/loki/api/v1/series"] = fx_reply("series.ok")
    server.defaults["/loki/api/v1/labels"] = fx_reply("labels.ok")
    server.defaults[RANGE] = json_reply(
        streams_body(
            ({"service_name": "svc"}, [[str(t), "async line", {"structuredMetadata": {"k": "v"}}]])
        )
    )
    c = aclient(server)
    log_client = c.log_client(resource={"service.name": "svc"})
    log_client.send(sdk.LogInput("one"))
    await log_client.flush(5)
    assert log_client.stats().sent == 1
    otlp = c.otlp_log_client()
    otlp.send(sdk.LogInput("two"))
    await otlp.flush(5)
    assert otlp.stats().sent == 1
    q = c.log_query()
    page = await q.search("async", limit=1)
    assert page.records[0].attributes == {"k": "v"}
    assert page.next_cursor
    assert await q.list_streams()
    assert "service_name" in await q.list_labels()
    assert (await q.wait_for_log("async", timeout_s=2, interval_s=0.01)).body == "async line"
    server.defaults[PUSH] = text_reply("Ingester is shutting down", 500)
    exporter = sdk.AsyncLokiLogExporter(c)
    result = await exporter.export([sdk.LogRecord(body="x")], gzip=True, timeout_s=1)
    assert result.rejected == 1
    assert isinstance(result.error, sdk.UnavailableError)
    assert (await exporter.export([], gzip=True, timeout_s=1)).accepted == 0
    await exporter.shutdown()
    server.defaults["/otlp/v1/logs"] = fx_reply("otlpLogs.invalid-body")
    otlp_exporter = sdk.AsyncLokiOtlpExporter(c)
    assert (
        await otlp_exporter.export([sdk.LogRecord(body="x")], gzip=False, timeout_s=1)
    ).rejected == 1
    assert (await otlp_exporter.export([], gzip=False, timeout_s=1)).accepted == 0
    await otlp_exporter.shutdown()
    await c.aclose()
    await c.aclose()
