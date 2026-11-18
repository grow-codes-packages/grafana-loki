"""A small, immutable LogQL query builder with safe escaping.

>>> str(LogQL.selector({"app": "x"}).line_contains("err").json().where("status", ">=", 500))
'{app="x"} |= "err" | json | status >= 500'
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Literal

from ._core import ValidationError
from ._labels import LABEL_NAME_PATTERN

__all__ = ["LogQL", "MatchOperator", "NumericOperator", "quote_logql"]

MatchOperator = Literal["=", "!=", "=~", "!~"]
"""Selector / label-filter operators for string values."""

NumericOperator = Literal["==", "!=", ">", ">=", "<", "<="]
"""Numeric label-filter operators."""

_MATCH_OPS = frozenset({"=", "!=", "=~", "!~"})
_NUMERIC_OPS = frozenset({"==", "!=", ">", ">=", "<", "<="})


def quote_logql(value: str) -> str:
    """Quote a LogQL string literal with Go/JSON escaping (``"``, ``\\``, newlines, tabs;
    Unicode stays as is). Verified against Loki 3.7 (``format_query`` and round trips)."""
    return json.dumps(value, ensure_ascii=False)


def _check_name(name: str, what: str) -> None:
    if not isinstance(name, str) or not LABEL_NAME_PATTERN.match(name):
        raise ValidationError(f"LogQL: invalid {what} {name!r}: must match [a-zA-Z_][a-zA-Z0-9_]*")


def _format_number(value: float) -> str:
    if not math.isfinite(value):
        raise ValidationError("LogQL: numbers must be finite")
    if isinstance(value, int):
        return str(value)
    text = format(Decimal(repr(value)), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _matches_empty(op: str, value: str) -> bool:
    if op == "=":
        return value == ""
    if op == "!=":
        return value != ""
    try:
        pattern = re.compile(f"^(?:{value})$")
    except re.error:
        return False  # let the server report RE2 syntax problems
    hit = pattern.match("") is not None
    return hit if op == "=~" else not hit


class LogQL:
    """Immutable LogQL builder; every method returns a new builder.

    >>> LogQL.selector({"app": "x", "env": "prod"}).line_matches("(?i)timeout").build()
    '{app="x", env="prod"} |~ "(?i)timeout"'
    """

    __slots__ = ("_matchers", "_stages")

    def __init__(
        self,
        matchers: tuple[tuple[str, str, str], ...] = (),
        stages: tuple[str, ...] = (),
    ) -> None:
        self._matchers = matchers
        self._stages = stages

    @classmethod
    def selector(cls, labels: Mapping[str, str] | None = None) -> LogQL:
        """Start a query from equality matchers (``{app="x", env="prod"}``)."""
        query = cls()
        for name, value in (labels or {}).items():
            query = query.label(name, "=", value)
        return query

    @staticmethod
    def quote(value: str) -> str:
        """Quote a string literal (see :func:`quote_logql`)."""
        return quote_logql(value)

    def label(self, name: str, op: MatchOperator, value: str) -> LogQL:
        """Add a stream-selector matcher (``=``, ``!=``, ``=~``, ``!~``)."""
        _check_name(name, "label name")
        if op not in _MATCH_OPS:
            raise ValidationError(f"LogQL: invalid matcher operator {op}")
        return LogQL((*self._matchers, (name, op, str(value))), self._stages)

    def _stage(self, text: str) -> LogQL:
        return LogQL(self._matchers, (*self._stages, text))

    def line_contains(self, text: str) -> LogQL:
        """Line filter ``|= "text"`` (case-sensitive substring)."""
        return self._stage(f"|= {quote_logql(text)}")

    def line_not_contains(self, text: str) -> LogQL:
        """Line filter ``!= "text"``."""
        return self._stage(f"!= {quote_logql(text)}")

    def line_matches(self, regex: str) -> LogQL:
        """Line filter ``|~ "regex"`` (RE2; prefix ``(?i)`` for case-insensitive)."""
        return self._stage(f"|~ {quote_logql(regex)}")

    def line_not_matches(self, regex: str) -> LogQL:
        """Line filter ``!~ "regex"``."""
        return self._stage(f"!~ {quote_logql(regex)}")

    def json(self) -> LogQL:
        """Parser stage ``| json``."""
        return self._stage("| json")

    def logfmt(self) -> LogQL:
        """Parser stage ``| logfmt``."""
        return self._stage("| logfmt")

    def unpack(self) -> LogQL:
        """Parser stage ``| unpack`` (lines packed by Promtail's ``pack`` stage)."""
        return self._stage("| unpack")

    def regexp(self, pattern: str) -> LogQL:
        """Parser stage ``| regexp "<re>"`` with named capture groups."""
        return self._stage(f"| regexp {quote_logql(pattern)}")

    def pattern(self, pattern: str) -> LogQL:
        """Parser stage ``| pattern "<pattern>"``."""
        return self._stage(f"| pattern {quote_logql(pattern)}")

    def line_format(self, template: str) -> LogQL:
        """Formatting stage ``| line_format "<template>"``."""
        return self._stage(f"| line_format {quote_logql(template)}")

    def where(self, field: str, op: MatchOperator | NumericOperator, value: str | float) -> LogQL:
        """Label filter on an extracted label or structured-metadata key.

        Numbers use the numeric operators (``==``, ``!=``, ``>``, ``>=``, ``<``, ``<=``);
        strings use ``=``, ``!=``, ``=~``, ``!~``.
        """
        _check_name(field, "field name")
        if isinstance(value, bool):
            raise ValidationError("LogQL: where() compares strings or numbers")
        if isinstance(value, (int, float)):
            if op not in _NUMERIC_OPS:
                raise ValidationError(f"LogQL: operator {op} cannot compare numbers")
            return self._stage(f"| {field} {op} {_format_number(value)}")
        if op not in _MATCH_OPS:
            raise ValidationError(f"LogQL: operator {op} cannot compare strings")
        return self._stage(f"| {field} {op} {quote_logql(value)}")

    def build(self) -> str:
        """Render the query.

        Raises :class:`ValidationError` when the selector is empty or every matcher also
        matches the empty string (Loki rejects such selectors).
        """
        if not self._matchers:
            raise ValidationError("LogQL: a stream selector needs at least one matcher")
        if all(_matches_empty(op, value) for _, op, value in self._matchers):
            raise ValidationError(
                "LogQL: the selector needs at least one matcher that does not match the empty value"
            )
        selector = ", ".join(f"{n}{op}{quote_logql(v)}" for n, op, v in self._matchers)
        return " ".join(("{" + selector + "}", *self._stages))

    def __str__(self) -> str:
        return self.build()

    def __repr__(self) -> str:
        try:
            return f"LogQL({self.build()!r})"
        except ValidationError:
            return "LogQL(<incomplete>)"
