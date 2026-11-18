"""Factories the integration suite and the conformance runner use (owned by this repository)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import grow_grafana_loki as sdk
from grow_grafana_loki.testing import LoggingTarget
from grow_grafana_loki.testing.logging_conformance import batch_options_for

URL_VAR = "GROW_GRAFANA_LOKI_URL"

#: Capabilities of the deployment under test for the core scenarios ("version" = serverVersion).
CORE_CAPABILITIES: set[str] = {"version"}

#: Capabilities for the logging scenarios: Loki has query, tail, streams and labels.
LOGGING_CAPABILITIES: set[str] = {"query", "tail", "streams", "labels"}


def core_client(env: Mapping[str, str], **overrides: Any) -> sdk.LokiClient:
    """A client for the plain single-tenant Loki (``GROW_GRAFANA_LOKI_URL``)."""
    return sdk.LokiClient.from_env(env, **overrides)


def async_core_client(env: Mapping[str, str], **overrides: Any) -> sdk.AsyncLokiClient:
    return sdk.AsyncLokiClient.from_env(env, **overrides)


def mt_client(env: Mapping[str, str], **overrides: Any) -> sdk.LokiClient:
    """A client for the multi-tenant Loki (``auth_enabled: true``), without a tenant."""
    return sdk.LokiClient(
        sdk.LokiConfig(url=env["GROW_GRAFANA_LOKI_MULTI_TENANT_URL"], **overrides)
    )


def gateway_config(env: Mapping[str, str], **overrides: Any) -> sdk.LokiConfig:
    """Config for the test auth gateway in front of the plain Loki (no credentials set)."""
    return sdk.LokiConfig(url=env["GROW_GRAFANA_LOKI_AUTH_PROXY_URL"], **overrides)


def logging_target(env: Mapping[str, str], scenario: Mapping[str, Any]) -> LoggingTarget:
    """A ``LoggingTarget(client, query)`` for one scenario (applies ``batchOptions``)."""
    client = core_client(env)
    log_client = client.log_client(
        options=batch_options_for(scenario),
        resource={"service.name": "grow-sdk-conformance", "deployment.environment": "test"},
    )
    return LoggingTarget(client=log_client, query=client.log_query())
