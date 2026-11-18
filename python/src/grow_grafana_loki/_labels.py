"""Label validation, the cardinality guard and ``LogRecord`` → Loki stream mapping."""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ._core import ValidationError
from ._logging import AttrValue, LogRecord, Severity
from ._time import datetime_to_ns, to_ns
from .config import DEFAULT_ALLOWED_LABELS

__all__ = [
    "LABEL_NAME_PATTERN",
    "RESERVED_METADATA_KEYS",
    "CardinalityGuard",
    "LokiEntry",
    "LokiStream",
    "attr_to_string",
    "is_valid_label_name",
    "label_set_string",
    "normalize_label_name",
    "records_to_streams",
    "validate_label",
]

#: Label names allowed by Loki (Prometheus rules).
LABEL_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

#: Structured metadata keys the SDK writes itself and reads back into ``LogRecord`` fields.
RESERVED_METADATA_KEYS: tuple[str, ...] = (
    "severity_text",
    "severity_number",
    "trace_id",
    "span_id",
    "trace_flags",
)

_NOT_NAME_CHAR = re.compile(r"[^a-zA-Z0-9_]")


def is_valid_label_name(name: str) -> bool:
    """True when ``name`` is a valid, non-reserved Loki label name."""
    return bool(LABEL_NAME_PATTERN.match(name)) and not name.startswith("__")


def validate_label(name: str, value: str) -> None:
    """Check one index label; raise :class:`ValidationError`.

    The name must match ``[a-zA-Z_][a-zA-Z0-9_]*`` and must not start with ``__`` (reserved);
    the value must be a non-empty string.
    """
    if not isinstance(name, str) or not LABEL_NAME_PATTERN.match(name):
        raise ValidationError(
            f"label: invalid label name {name!r} (must match [a-zA-Z_][a-zA-Z0-9_]*)"
        )
    if name.startswith("__"):
        raise ValidationError(f"label: label name {name!r} is reserved (starts with __)")
    if not isinstance(value, str) or value == "":
        raise ValidationError(f"label: label {name!r} must have a non-empty string value")


def normalize_label_name(key: str) -> str:
    """Turn an attribute key into a label / metadata name the way Loki does for OTLP.

    Every character outside ``[a-zA-Z0-9_]`` becomes ``_`` (``service.name`` →
    ``service_name``) and a leading digit gets a ``_`` prefix. An empty key stays empty.
    """
    if not key:
        return ""
    out = _NOT_NAME_CHAR.sub("_", key)
    return "_" + out if out[0].isdigit() else out


def label_set_string(labels: Mapping[str, str]) -> str:
    """Prometheus label-set syntax with sorted names and quoted values: ``{a="b", c="d"}``."""
    parts = [f"{k}={json.dumps(labels[k], ensure_ascii=False)}" for k in sorted(labels)]
    return "{" + ", ".join(parts) + "}"


def attr_to_string(value: Any) -> str:
    """Render an attribute value as a metadata string (objects and lists as JSON)."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value) if isinstance(value, float) else str(value)
    if value is None:
        return ""
    return json.dumps(_plain(value), separators=(",", ":"), ensure_ascii=False, default=str)


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


@dataclasses.dataclass(frozen=True)
class LokiEntry:
    """One log line pushed to Loki.

    Attributes:
        line: the log line.
        timestamp: ``datetime`` or ``int`` Unix nanoseconds (also a nanosecond / RFC 3339
            string); ``None`` means "now" at push time.
        structured_metadata: per-entry metadata (string → string), stored next to the line but
            not indexed.
    """

    line: str
    timestamp: _dt.datetime | int | str | None = None
    structured_metadata: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def timestamp_ns(self, default: int | None = None) -> int:
        """The timestamp in Unix nanoseconds (``default`` or now when unset)."""
        if self.timestamp is None:
            if default is not None:
                return default
            return datetime_to_ns(_dt.datetime.now(_dt.timezone.utc))
        return int(to_ns(self.timestamp, "timestamp"))


@dataclasses.dataclass(frozen=True)
class LokiStream:
    """A stream: one label set and its entries (``push`` input).

    Keep labels few and low-cardinality; the cardinality guard moves labels outside the
    allowlist to structured metadata.
    """

    labels: Mapping[str, str]
    entries: Sequence[LokiEntry]


class CardinalityGuard:
    """Keeps Loki's index small: only allowlisted keys become labels, the rest is metadata."""

    def __init__(self, allowed: Iterable[str] | None = None) -> None:
        names = DEFAULT_ALLOWED_LABELS if allowed is None else tuple(allowed)
        self._allowed = frozenset(names)

    @property
    def allowed(self) -> list[str]:
        """The allowlist, sorted."""
        return sorted(self._allowed)

    def is_allowed(self, name: str) -> bool:
        """Whether ``name`` may be an index label."""
        return name in self._allowed

    def apply(self, stream: LokiStream) -> LokiStream:
        """Copy of ``stream`` whose labels outside the allowlist are moved to every entry's
        structured metadata (an entry's own metadata wins)."""
        labels = {k: v for k, v in stream.labels.items() if k in self._allowed}
        moved = {k: v for k, v in stream.labels.items() if k not in self._allowed}
        if not moved:
            return LokiStream(labels, stream.entries)
        entries = [
            dataclasses.replace(e, structured_metadata={**moved, **e.structured_metadata})
            for e in stream.entries
        ]
        return LokiStream(labels, entries)

    def split(self, attributes: Mapping[str, AttrValue]) -> tuple[dict[str, str], dict[str, str]]:
        """Divide attributes into index labels (allowlisted, scalar, non-empty) and metadata.

        Keys are normalized with :func:`normalize_label_name`; non-string values are rendered
        with :func:`attr_to_string`.
        """
        labels: dict[str, str] = {}
        metadata: dict[str, str] = {}
        for key in sorted(attributes):
            name = normalize_label_name(key)
            if not name:
                continue
            value = attributes[key]
            text = attr_to_string(value)
            scalar = isinstance(value, (str, int, float, bool))
            if name in self._allowed and scalar and text and is_valid_label_name(name):
                labels[name] = text
            else:
                metadata[name] = text
        return labels, metadata


def records_to_streams(
    records: Iterable[LogRecord], guard: CardinalityGuard | None = None
) -> list[LokiStream]:
    """Map log records to Loki streams with the cardinality guard.

    Resource and attribute keys are normalized (``service.name`` → ``service_name``); keys in
    the allowlist with scalar, non-empty values become index labels, everything else becomes
    structured metadata. ``level`` (the lower-case severity text) is added as a label when
    allowed. Severity, trace id, span id and trace flags travel as metadata (``severity_text``,
    ``severity_number``, ``trace_id``, ``span_id``, ``trace_flags``) and are never labels.
    Records with the same final label set share a stream.
    """
    guard = guard or CardinalityGuard()
    streams: dict[str, LokiStream] = {}
    for record in records:
        labels, metadata = guard.split(record.resource)
        attr_labels, attr_meta = guard.split(record.attributes)
        labels.update(attr_labels)
        metadata.update(attr_meta)
        for reserved in RESERVED_METADATA_KEYS:
            labels.pop(reserved, None)
        text = record.severity_text or Severity(record.severity).name
        if guard.is_allowed("level") and "level" not in labels:
            labels["level"] = text.lower()
        metadata["severity_text"] = text
        metadata["severity_number"] = str(int(record.severity))
        if record.trace_id:
            metadata["trace_id"] = record.trace_id
            if record.span_id:
                metadata["span_id"] = record.span_id
            if record.trace_flags is not None:
                metadata["trace_flags"] = str(record.trace_flags)
        key = label_set_string(labels)
        entry = LokiEntry(
            line=record.body,
            timestamp=datetime_to_ns(record.timestamp),
            structured_metadata=metadata,
        )
        stream = streams.get(key)
        if stream is None:
            streams[key] = LokiStream(labels, [entry])
        else:
            stream.entries.append(entry)  # type: ignore[attr-defined]
    return list(streams.values())
