"""Grow Codes SDK for Grafana Loki.

>>> from grow_grafana_loki import LokiClient, LokiConfig
>>> client = LokiClient.from_env()  # doctest: +SKIP
"""

from . import _core
from ._core import *  # noqa: F403
from ._version import __version__
from .client import AsyncLokiClient, LokiClient
from .config import (
    DEFAULT_ALLOWED_LABELS,
    DEFAULT_URL,
    MAX_TENANT_ID_LENGTH,
    LokiConfig,
    tenant_problem,
)

__all__ = [
    "__version__",
    "LokiClient",
    "AsyncLokiClient",
    "LokiConfig",
    "DEFAULT_URL",
    "DEFAULT_ALLOWED_LABELS",
    "MAX_TENANT_ID_LENGTH",
    "tenant_problem",
]
__all__ += _core.__all__

from . import _logging  # noqa: E402
from ._logging import *  # noqa: E402, F403

__all__ += _logging.__all__

from ._calls import (  # noqa: E402
    HEADER_ENCODING_FLAGS,
    HEADER_TENANT,
    INGESTER_SHUTTING_DOWN,
    OTLP_SCOPE_NAME,
    STATUS_INGESTION_BLOCKED,
    PushFormat,
)
from ._labels import (  # noqa: E402
    LABEL_NAME_PATTERN,
    RESERVED_METADATA_KEYS,
    CardinalityGuard,
    LokiEntry,
    LokiStream,
    attr_to_string,
    is_valid_label_name,
    label_set_string,
    normalize_label_name,
    records_to_streams,
    validate_label,
)
from ._logs import (  # noqa: E402
    STREAM_SELECTOR_ALL,
    AsyncLokiLogExporter,
    AsyncLokiLogQueryClient,
    AsyncLokiOtlpExporter,
    LokiLogExporter,
    LokiLogQueryClient,
    LokiOtlpExporter,
    entry_to_record,
    to_logql,
)
from ._models import (  # noqa: E402
    BuildInfo,
    Direction,
    DroppedEntry,
    IndexStats,
    Point,
    QueryResult,
    Sample,
    SampleSeries,
    StreamEntry,
    StreamResult,
    TailFrame,
)
from ._tail import MAX_TAIL_DELAY_S, AsyncTailSession, TailSession  # noqa: E402
from ._time import LokiTime, datetime_to_ns, ns_to_datetime, to_ns  # noqa: E402
from .logql import LogQL, MatchOperator, NumericOperator, quote_logql  # noqa: E402

__all__ += [
    # push + labels
    "PushFormat",
    "LokiEntry",
    "LokiStream",
    "CardinalityGuard",
    "LABEL_NAME_PATTERN",
    "RESERVED_METADATA_KEYS",
    "attr_to_string",
    "is_valid_label_name",
    "label_set_string",
    "normalize_label_name",
    "records_to_streams",
    "validate_label",
    # results
    "BuildInfo",
    "Direction",
    "DroppedEntry",
    "IndexStats",
    "Point",
    "QueryResult",
    "Sample",
    "SampleSeries",
    "StreamEntry",
    "StreamResult",
    "TailFrame",
    # tail
    "MAX_TAIL_DELAY_S",
    "TailSession",
    "AsyncTailSession",
    # logging category
    "LokiLogExporter",
    "AsyncLokiLogExporter",
    "LokiOtlpExporter",
    "AsyncLokiOtlpExporter",
    "LokiLogQueryClient",
    "AsyncLokiLogQueryClient",
    "STREAM_SELECTOR_ALL",
    "entry_to_record",
    "to_logql",
    # LogQL
    "LogQL",
    "MatchOperator",
    "NumericOperator",
    "quote_logql",
    # time + constants
    "LokiTime",
    "datetime_to_ns",
    "ns_to_datetime",
    "to_ns",
    "HEADER_TENANT",
    "HEADER_ENCODING_FLAGS",
    "INGESTER_SHUTTING_DOWN",
    "OTLP_SCOPE_NAME",
    "STATUS_INGESTION_BLOCKED",
]
