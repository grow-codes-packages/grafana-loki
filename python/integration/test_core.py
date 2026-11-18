"""Core round trips against the real service plus the core conformance scenarios."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from factories import CORE_CAPABILITIES, async_core_client, core_client, gateway_config

import grow_grafana_loki as sdk
from grow_grafana_loki.testing import ConformanceRunner, assert_scenario, pytest_params


def test_health(client: sdk.LokiClient) -> None:
    status = client.health()
    assert status.healthy, status
    assert status.status == "ok"
    assert status.details["httpStatus"] == "200"


def test_ready(client: sdk.LokiClient) -> None:
    assert client.ready() is True


def test_ping(client: sdk.LokiClient) -> None:
    assert client.ping() >= 0


def test_server_version_and_build_info(client: sdk.LokiClient, env: dict[str, str]) -> None:
    expected = env.get("GROW_GRAFANA_LOKI_EXPECTED_VERSION", "3.7.10")
    assert client.server_version() == expected
    info = client.build_info()
    assert info.version == expected
    assert info.revision
    assert info.branch


def test_close_is_idempotent(env: dict[str, str]) -> None:
    c = core_client(env)
    c.close()
    c.close()
    with pytest.raises(sdk.CancelledError):
        c.ping()


def test_async_client(env: dict[str, str]) -> None:
    async def run() -> None:
        async with async_core_client(env) as c:
            assert (await c.health()).healthy
            assert await c.ready()
            assert await c.ping() >= 0
            assert (await c.build_info()).version == await c.server_version()

    asyncio.run(run())


def test_bad_credentials_are_rejected(env: dict[str, str]) -> None:
    secret = "definitely-wrong-password"
    cfg = gateway_config(env, username="nobody", password=secret)
    with sdk.LokiClient(cfg) as c:
        with pytest.raises(sdk.AuthenticationError) as info:
            c.ping()
        assert secret not in str(info.value)
        assert secret not in repr(c)


@pytest.mark.parametrize("scenario", pytest_params(categories=["core"]))
def test_core_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    runner = ConformanceRunner(
        {"core": lambda ctx: core_client(env)}, capabilities=CORE_CAPABILITIES
    )
    assert_scenario(runner, scenario)
