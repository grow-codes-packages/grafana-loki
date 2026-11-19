"""Logging conformance scenarios (send → flush → search) against the real Loki."""

from __future__ import annotations

from typing import Any

import pytest
from factories import LOGGING_CAPABILITIES, logging_target

from grow_grafana_loki.testing import ConformanceRunner, assert_scenario, pytest_params


@pytest.mark.parametrize("scenario", pytest_params(categories=["logging"]))
def test_logging_conformance(env: dict[str, str], scenario: dict[str, Any]) -> None:
    runner = ConformanceRunner(
        {"logging": lambda ctx: logging_target(env, ctx.scenario)},
        capabilities=LOGGING_CAPABILITIES,
    )
    assert_scenario(runner, scenario)
