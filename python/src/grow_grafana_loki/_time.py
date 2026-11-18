"""Time helpers for the Loki HTTP API (Unix nanosecond timestamps)."""

from __future__ import annotations

import datetime as _dt
import math
import re
from typing import TypeAlias

from ._core import ValidationError, to_seconds
from ._logging import parse_timestamp

__all__ = [
    "LokiTime",
    "datetime_to_ns",
    "ns_to_datetime",
    "to_ns",
    "duration_param",
]

LokiTime: TypeAlias = "_dt.datetime | int | str"
"""A point in time accepted by the Loki API: an aware ``datetime`` (naive = UTC), an ``int`` of
Unix nanoseconds (lossless) or a string (decimal Unix nanoseconds or RFC 3339)."""

_DIGITS = re.compile(r"^\d+$")
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def datetime_to_ns(value: _dt.datetime) -> int:
    """Unix nanoseconds of ``value`` (naive datetimes are taken as UTC)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def ns_to_datetime(ns: int | str) -> _dt.datetime:
    """Aware UTC ``datetime`` of Unix nanoseconds (microsecond precision)."""
    value = int(ns)
    return _EPOCH + _dt.timedelta(microseconds=value // 1_000)


def to_ns(value: LokiTime, field: str = "time") -> str:
    """Render a :data:`LokiTime` as the decimal nanosecond string Loki expects.

    Raises :class:`ValidationError` for negative numbers or unparsable text.
    """
    if isinstance(value, _dt.datetime):
        return str(datetime_to_ns(value))
    if isinstance(value, bool):
        raise ValidationError(f"{field}: expected a datetime, int nanoseconds or RFC 3339 text")
    if isinstance(value, int):
        if value < 0:
            raise ValidationError(f"{field}: must not be negative")
        return str(value)
    if isinstance(value, str):
        text = value.strip()
        if _DIGITS.match(text):
            return text
        try:
            return str(datetime_to_ns(parse_timestamp(text)))
        except ValidationError:
            pass
    raise ValidationError(f"{field}: expected a datetime, int nanoseconds or RFC 3339 text")


def optional_ns(value: LokiTime | None, field: str) -> str | None:
    """:func:`to_ns` for optional values."""
    return None if value is None else to_ns(value, field)


def duration_param(value: float | _dt.timedelta | str, field: str) -> str:
    """Render a duration (float seconds, ``timedelta`` or Loki text such as ``"5m"``).

    Numbers become whole milliseconds (``1.5`` → ``"1500ms"``), the form all five packages send.
    """
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValidationError(f"{field}: must not be empty")
        return text
    if isinstance(value, bool):
        raise ValidationError(f"{field}: must be a positive duration")
    seconds = to_seconds(value)
    if not (seconds > 0 and math.isfinite(seconds)):
        raise ValidationError(f"{field}: must be a positive duration")
    return f"{round(seconds * 1000)}ms"
