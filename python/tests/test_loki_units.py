"""Config, labels, time helpers, wire encoders, decoders and the LogQL builder."""

from __future__ import annotations

import base64
import datetime as dt
import gzip
import json
import sys
from typing import Any

import cramjam
import pytest

import grow_grafana_loki as sdk
from _loki import fx
from grow_grafana_loki import _models, _time, _wire

P = sdk.ENV_PREFIX
UTC = dt.timezone.utc

# --------------------------------------------------------------------------- config


def test_config_env_precedence_and_aliases() -> None:
    assert P == "GROW_GRAFANA_LOKI_"
    cfg = sdk.LokiConfig.from_env(
        {
            "LOKI_URL": "http://vendor:3100",
            "LOKI_TENANT_ID": "vendor-tenant",
            "LOKI_BASIC_AUTH_USER": "platform-user",
            "LOKI_BASIC_AUTH_PASSWORD": "platform-pass",
        }
    )
    assert cfg.url == "http://vendor:3100"
    assert cfg.tenant_id == "vendor-tenant"
    assert cfg.username == "platform-user"
    assert isinstance(cfg.password, sdk.Secret)
    assert cfg.password.reveal() == "platform-pass"
    env = {
        P + "URL": "http://grow:1",
        P + "TENANT_ID": "grow-tenant",
        P + "USERNAME": "grow-user",
        P + "PASSWORD": "grow-pass",
        "LOKI_USERNAME": "loki-user",
        "LOKI_PASSWORD": "loki-pass",
    }
    cfg = sdk.LokiConfig.from_env(env)
    assert (cfg.url, cfg.tenant_id, cfg.username) == ("http://grow:1", "grow-tenant", "grow-user")
    assert cfg.from_env(env, tenant_id="explicit").tenant_id == "explicit"
    bearer = sdk.LokiConfig.from_env({"LOKI_BEARER_TOKEN": "bt-12345"})
    assert bearer.bearer_token is not None
    assert bearer.auth_headers() == {"Authorization": "Bearer bt-12345"}
    assert sdk.LokiConfig.from_env({}).url == sdk.DEFAULT_URL


def test_config_validate_rules() -> None:
    bad: list[dict[str, Any]] = [
        {"username": "u"},
        {"password": "p"},
        {"username": "u", "password": "p", "bearer_token": "t"},
        {"bearer_token": "  "},
        {"tenant_id": ""},
        {"tenant_id": "a|b"},
        {"tenant_id": "a|.."},
        {"tenant_id": "x" * 151},
        {"tenant_id": "."},
        {"tenant_id": "bad/tenant"},
        {"allowed_labels": ["bad-name"]},
        {"allowed_labels": ["__reserved"]},
    ]
    for kwargs in bad:
        with pytest.raises(sdk.ConfigError):
            sdk.LokiConfig(**kwargs).validate()
    ok = sdk.LokiConfig(tenant_id="a|b", multi_tenant_queries=True).validate()
    assert ok.tenant_id == "a|b"
    assert sdk.tenant_problem("ok-Tenant_1!*'().") is None
    assert sdk.tenant_problem("é") is not None
    assert sdk.LokiConfig(allowed_labels=["app"]).effective_allowed_labels == ("app",)
    assert sdk.LokiConfig().effective_allowed_labels == sdk.DEFAULT_ALLOWED_LABELS


def test_config_secrets_redacted() -> None:
    cfg = sdk.LokiConfig(username="user", password="pass-12345", tenant_id="t")
    token = base64.b64encode(b"user:pass-12345").decode()
    assert cfg.auth_headers() == {"Authorization": f"Basic {token}", "X-Scope-OrgID": "t"}
    assert "pass-12345" not in repr(cfg)
    assert set(cfg.known_secrets()) == {"pass-12345", token}
    assert sdk.LokiConfig().auth_headers() == {}


# --------------------------------------------------------------------------- labels


def test_label_helpers() -> None:
    sdk.validate_label("service_name", "x")
    for name, value in (("bad-name", "x"), ("1x", "x"), ("__name__", "x"), ("ok", "")):
        with pytest.raises(sdk.ValidationError):
            sdk.validate_label(name, value)
    with pytest.raises(sdk.ValidationError):
        sdk.validate_label("ok", 5)  # type: ignore[arg-type]
    assert sdk.is_valid_label_name("a_1")
    assert not sdk.is_valid_label_name("__a")
    assert sdk.normalize_label_name("service.name") == "service_name"
    assert sdk.normalize_label_name("1st-key") == "_1st_key"
    assert sdk.normalize_label_name("") == ""
    assert sdk.label_set_string({"b": 'say "hi"', "a": "ü"}) == '{a="ü", b="say \\"hi\\""}'
    assert sdk.attr_to_string("s") == "s"
    assert sdk.attr_to_string(True) == "true"
    assert sdk.attr_to_string(False) == "false"
    assert sdk.attr_to_string(3) == "3"
    assert sdk.attr_to_string(0.5) == "0.5"
    assert sdk.attr_to_string(None) == ""  # type: ignore[arg-type]
    assert sdk.attr_to_string({"a": [1, (2, 3)]}) == '{"a":[1,[2,3]]}'


def test_cardinality_guard() -> None:
    guard = sdk.CardinalityGuard(["service_name", "level"])
    assert guard.allowed == ["level", "service_name"]
    assert guard.is_allowed("level")
    stream = sdk.LokiStream(
        {"service_name": "s", "run": "r", "env": "e"},
        [sdk.LokiEntry("x", 1, {"run": "own"}), sdk.LokiEntry("y", 2)],
    )
    applied = guard.apply(stream)
    assert applied.labels == {"service_name": "s"}
    assert applied.entries[0].structured_metadata == {"run": "own", "env": "e"}
    assert applied.entries[1].structured_metadata == {"run": "r", "env": "e"}
    same = guard.apply(sdk.LokiStream({"level": "info"}, []))
    assert same.labels == {"level": "info"}
    labels, meta = guard.split(
        {"service.name": "svc", "level": "", "n": 1, "": "dropped", "list": [1], "level2": "x"}
    )
    assert labels == {"service_name": "svc"}
    assert meta == {"level": "", "n": "1", "list": "[1]", "level2": "x"}
    assert sdk.CardinalityGuard().allowed == sorted(sdk.DEFAULT_ALLOWED_LABELS)


def test_records_to_streams() -> None:
    ts = dt.datetime(2026, 1, 1, tzinfo=UTC)
    records = [
        sdk.LogRecord(
            body="a",
            severity=sdk.Severity.WARN,
            timestamp=ts,
            attributes={"run": "r1", "trace_id": "label-not-allowed"},
            resource={"service.name": "svc", "service.version": "1.0", "host.name": "h"},
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
            span_id="00f067aa0ba902b7",
            trace_flags=1,
        ),
        sdk.LogRecord(
            body="b",
            severity=sdk.Severity.WARN,
            timestamp=ts,
            resource={"service.name": "svc", "host.name": "h"},
        ),
        sdk.LogRecord(body="c", resource={"service.name": "svc", "level": "custom"}),
    ]
    streams = sdk.records_to_streams(records)
    assert len(streams) == 2
    first, second = streams
    assert first.labels == {"service_name": "svc", "host_name": "h", "level": "warn"}
    assert len(first.entries) == 2
    meta = first.entries[0].structured_metadata
    assert meta["service_version"] == "1.0"
    assert meta["run"] == "r1"
    assert meta["severity_text"] == "WARN"
    assert meta["severity_number"] == "13"
    assert meta["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert meta["span_id"] == "00f067aa0ba902b7"
    assert meta["trace_flags"] == "1"
    assert first.entries[0].timestamp == 1767225600000000000
    assert "trace_id" not in first.entries[1].structured_metadata
    assert second.labels["level"] == "custom"
    no_level = sdk.records_to_streams(records[:1], sdk.CardinalityGuard(["service_name"]))
    assert no_level[0].labels == {"service_name": "svc"}


def test_loki_entry_timestamp_ns() -> None:
    assert sdk.LokiEntry("x", 5).timestamp_ns() == 5
    assert sdk.LokiEntry("x").timestamp_ns(7) == 7
    assert sdk.LokiEntry("x").timestamp_ns() > 1_700_000_000 * 10**9
    assert sdk.LokiEntry("x", "2026-01-01T00:00:00Z").timestamp_ns() == 1767225600000000000


# --------------------------------------------------------------------------- time


def test_time_helpers() -> None:
    naive = dt.datetime(2026, 1, 1)
    assert sdk.datetime_to_ns(naive) == 1767225600000000000
    assert sdk.to_ns(dt.datetime(2026, 1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1)))) == (
        "1767225600000000000"
    )
    assert sdk.to_ns(12) == "12"
    assert sdk.to_ns(" 1795034767064713072 ") == "1795034767064713072"
    assert sdk.to_ns("2026-01-01T00:00:00.5Z") == "1767225600500000000"
    for bad in (-1, True, "yesterday", 1.5):
        with pytest.raises(sdk.ValidationError):
            sdk.to_ns(bad)  # type: ignore[arg-type]
    assert sdk.ns_to_datetime("1767225600000001999") == dt.datetime(2026, 1, 1, 0, 0, 0, 1, UTC)
    assert _time.optional_ns(None, "x") is None
    assert _time.duration_param(1.5, "d") == "1500ms"
    assert _time.duration_param(dt.timedelta(minutes=2), "d") == "120000ms"
    assert _time.duration_param(" 5m ", "d") == "5m"
    for bad_d in ("", 0, -1, True, float("inf")):
        with pytest.raises(sdk.ValidationError):
            _time.duration_param(bad_d, "d")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- wire


def test_push_json_encoding() -> None:
    body = json.loads(_wire.encode_push_json([({"a": "b"}, [(1, "x", {}), (2, "y", {"k": "v"})])]))
    assert body == {
        "streams": [{"stream": {"a": "b"}, "values": [["1", "x"], ["2", "y", {"k": "v"}]]}]
    }
    assert gzip.decompress(_wire.gzip_bytes(b"abc")) == b"abc"


def test_snappy_literal_and_cramjam(monkeypatch: pytest.MonkeyPatch) -> None:
    for size in (0, 10, 59, 60, 61, 255, 256, 300, 70000):
        data = bytes(i % 251 for i in range(size))
        block = _wire._snappy_literal_block(data)
        assert bytes(cramjam.snappy.decompress_raw(block)) == data
    data = b"abc" * 1000
    assert bytes(cramjam.snappy.decompress_raw(_wire.snappy_compress(data))) == data
    assert len(_wire.snappy_compress(data)) < len(data)
    monkeypatch.setitem(sys.modules, "cramjam", None)  # simulate the extra not being installed
    fallback = _wire.snappy_compress(data)
    assert len(fallback) > len(data)
    monkeypatch.undo()
    assert bytes(cramjam.snappy.decompress_raw(fallback)) == data


def test_protobuf_timestamp_edges() -> None:
    raw = _wire.encode_push_protobuf([({}, [(0, "", {}), (3 * 10**9, "x", {})])])
    assert raw.startswith(b"\x0a")
    assert _wire._varint(300) == b"\xac\x02"


def test_rpc_status_decoding() -> None:
    body = base64.b64decode(fx("otlpLogs.empty-json")["response"]["body"]["base64"])
    assert (
        _wire.decode_rpc_status(body) == "error at least one valid stream is required for ingestion"
    )
    assert _wire.decode_rpc_status(b"\x08\x03\x12\x02hi") == "hi"
    assert _wire.decode_rpc_status(b"") is None
    assert _wire.decode_rpc_status(b"\x12\x05ab") is None  # truncated
    assert _wire.decode_rpc_status(b"\x0d\x00\x00") is None  # fixed32: unsupported wire type
    assert _wire.decode_rpc_status(b"\x08") is None  # truncated varint
    assert _wire.decode_rpc_status(b"\x12\x01 ") is None  # blank message


def test_otlp_any_values() -> None:
    record = sdk.LogRecord(
        body="b",
        attributes={"inf": float("inf"), "obj": object(), "kv": {"a": 1}},
        observed_timestamp=dt.datetime(2026, 1, 1, tzinfo=UTC),
    )
    doc = json.loads(_wire.encode_otlp_logs_json([record], "scope"))
    item = doc["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    attrs = {a["key"]: a["value"] for a in item["attributes"]}
    assert attrs["inf"] == {"stringValue": "inf"}
    assert "stringValue" in attrs["obj"]
    assert attrs["kv"] == {"kvlistValue": {"values": [{"key": "a", "value": {"intValue": "1"}}]}}
    assert item["observedTimeUnixNano"] == "1767225600000000000"
    assert "traceId" not in item


# --------------------------------------------------------------------------- decoders


def test_decode_query_data_variants() -> None:
    assert _models.decode_query_data(None) == sdk.QueryResult()
    scalar = _models.decode_query_data({"resultType": "scalar", "result": [1.5, "2"]})
    assert scalar.scalar is not None
    assert scalar.scalar.value == 2.0
    assert scalar.scalar.timestamp == dt.datetime.fromtimestamp(1.5, UTC)
    unknown = _models.decode_query_data({"resultType": "string", "result": [1, "x"], "stats": 5})
    assert unknown.result_type == "string"
    assert unknown.stats == {}
    no_result = _models.decode_query_data({"resultType": "streams"})
    assert no_result.streams == []
    two = _models.decode_entry(["1", 5])
    assert two.line == "5"
    assert two.structured_metadata == {}
    bad_data: list[Any] = [
        5,
        {"resultType": "streams", "result": 5},
        {"resultType": "streams", "result": [5]},
        {"resultType": "streams", "result": [{"values": 5}]},
        {"resultType": "streams", "result": [{"values": [["x", "line"]]}]},
        {"resultType": "streams", "result": [{"values": [["1"]]}]},
        {"resultType": "streams", "result": [{"stream": 5, "values": []}]},
        {"resultType": "vector", "result": 5},
        {"resultType": "vector", "result": [5]},
        {"resultType": "vector", "result": [{"value": [1]}]},
        {"resultType": "vector", "result": [{"value": ["x", "1"]}]},
        {"resultType": "vector", "result": [{"value": [float("nan"), "1"]}]},
        {"resultType": "matrix", "result": 5},
    ]
    for data in bad_data:
        with pytest.raises(sdk.ValidationError):
            _models.decode_query_data(data)


def test_decode_misc_bodies() -> None:
    with pytest.raises(sdk.ValidationError):
        _models.decode_string_list({"data": 5})
    with pytest.raises(sdk.ValidationError):
        _models.decode_string_list([1])
    assert _models.decode_series({}) == []
    with pytest.raises(sdk.ValidationError):
        _models.decode_series({"data": {}})
    assert _models.decode_series({"data": [{"a": 1}]}) == [{"a": "1"}]
    with pytest.raises(sdk.ValidationError):
        _models.decode_index_stats({"streams": "x"})
    assert _models.decode_build_info({"version": 3}).version == ""
    frame = _models.decode_tail_frame(
        {
            "streams": fx("tail.categorized-labels")["response"]["frame"]["streams"],
            "dropped_entries": [{"labels": {"a": "b"}, "timestamp": "1767225600000000000"}],
        }
    )
    assert frame.streams[0].entries[0].structured_metadata["request_id"].startswith("tail-")
    assert frame.dropped_entries == [
        sdk.DroppedEntry({"a": "b"}, dt.datetime(2026, 1, 1, tzinfo=UTC))
    ]
    with pytest.raises(sdk.ValidationError):
        _models.decode_tail_frame({"dropped_entries": [{"timestamp": "x"}]})
    entry = sdk.StreamEntry(1767225600000000000, "l")
    assert entry.timestamp == dt.datetime(2026, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------- LogQL


def test_logql_builder() -> None:
    q = sdk.LogQL.selector({"app": "x"}).line_contains("err").json().where("status", ">=", 500)
    assert str(q) == '{app="x"} |= "err" | json | status >= 500'
    full = (
        sdk.LogQL.selector({"app": "x"})
        .label("env", "!=", "dev")
        .label("pod", "=~", "web-.*")
        .line_not_contains("debug")
        .line_matches("(?i)timeout")
        .line_not_matches("health")
        .logfmt()
        .unpack()
        .regexp("(?P<ip>\\S+)")
        .pattern("<ip> <_>")
        .line_format("{{.msg}}")
        .where("level", "=", "error")
        .where("latency", "<", 0.25)
        .where("n", "==", 1e20)
        .pipe(" drop pod ")
    )
    assert full.build() == (
        '{app="x", env!="dev", pod=~"web-.*"} != "debug" |~ "(?i)timeout" !~ "health" | logfmt '
        '| unpack | regexp "(?P<ip>\\\\S+)" | pattern "<ip> <_>" | line_format "{{.msg}}" '
        '| level = "error" | latency < 0.25 | n == 100000000000000000000 | drop pod'
    )
    nasty = 'q " b \\ n \n t \t ü 🚀'
    assert sdk.LogQL.quote(nasty) == sdk.quote_logql(nasty) == json.dumps(nasty, ensure_ascii=False)
    assert repr(q).startswith("LogQL(")
    assert repr(sdk.LogQL()) == "LogQL(<incomplete>)"


def test_logql_validation() -> None:
    for build in (
        lambda: sdk.LogQL().build(),
        lambda: sdk.LogQL.selector({"app": ""}).build(),
        lambda: sdk.LogQL().label("app", "=~", ".*").build(),
        lambda: sdk.LogQL().label("app", "!~", "x").build(),
        lambda: sdk.LogQL().label("app", "!=", "x").build(),
        lambda: sdk.LogQL.selector({"bad-name": "x"}),
        lambda: sdk.LogQL().label("a", "==", "x"),  # type: ignore[arg-type]
        lambda: sdk.LogQL.selector({"a": "b"}).where("n", "=~", 5),
        lambda: sdk.LogQL.selector({"a": "b"}).where("n", ">", "x"),
        lambda: sdk.LogQL.selector({"a": "b"}).where("n", ">", float("nan")),
        lambda: sdk.LogQL.selector({"a": "b"}).where("n", "==", True),  # type: ignore[arg-type]
    ):
        with pytest.raises(sdk.ValidationError):
            build()
    # an invalid regex is left for the server; a non-empty-matching one is accepted
    assert sdk.LogQL().label("app", "=~", "(").build() == '{app=~"("}'
    assert sdk.LogQL().label("app", "=~", ".+").build() == '{app=~".+"}'
    assert sdk.LogQL().label("app", "!~", ".*").label("b", "!=", "").build()
