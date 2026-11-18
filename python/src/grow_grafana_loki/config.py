"""Configuration for the Grafana Loki client (service-owned)."""

from __future__ import annotations

import base64
import dataclasses
import re
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, TypeVar

from ._core import BaseConfig, ConfigError, EnvReader, Secret

__all__ = [
    "DEFAULT_ALLOWED_LABELS",
    "DEFAULT_URL",
    "MAX_TENANT_ID_LENGTH",
    "LokiConfig",
    "tenant_problem",
]

#: Default endpoint when neither the config nor the environment sets one (HTTP API, OTLP and
#: WebSocket tail share one port).
DEFAULT_URL = "http://127.0.0.1:3100"

#: Longest tenant id (``X-Scope-OrgID``) Loki accepts, in bytes.
MAX_TENANT_ID_LENGTH = 150

#: The cardinality guard's default label allowlist: only these keys become Loki index labels,
#: every other attribute or label is sent as structured metadata.
DEFAULT_ALLOWED_LABELS: tuple[str, ...] = (
    "service_name",
    "service_namespace",
    "deployment_environment",
    "host_name",
    "level",
    "job",
    "instance",
    "namespace",
    "pod",
    "container",
)

_TENANT_CHARS = re.compile(r"^[A-Za-z0-9!\-_.*'()]+$")
_LABEL_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_C = TypeVar("_C", bound="LokiConfig")


def tenant_problem(tenant_id: str, multi_tenant_queries: bool = False) -> str | None:
    """Why ``tenant_id`` is not a valid Loki tenant id, or ``None`` when it is valid.

    Rules (verified against Loki 3.7): not empty, at most 150 bytes, not ``.`` or ``..``,
    characters ``[A-Za-z0-9!-_.*'()]`` only. ``|`` joins tenants for a multi-tenant query and is
    accepted only when ``multi_tenant_queries`` is set (each part must then be valid).
    """
    if "|" in tenant_id:
        if not multi_tenant_queries:
            return "must not contain '|' (multi-tenant queries are not enabled)"
        for part in tenant_id.split("|"):
            reason = tenant_problem(part, False)
            if reason is not None:
                return reason
        return None
    if not tenant_id:
        return "must not be empty"
    if len(tenant_id.encode("utf-8")) > MAX_TENANT_ID_LENGTH:
        return f"is too long (max {MAX_TENANT_ID_LENGTH} bytes)"
    if tenant_id in (".", ".."):
        return "must not be '.' or '..'"
    if not _TENANT_CHARS.match(tenant_id):
        return "contains unsupported characters (allowed: A-Z a-z 0-9 ! - _ . * ' ( ))"
    return None


@dataclasses.dataclass(frozen=True, repr=False)
class LokiConfig(BaseConfig):
    """Immutable settings for :class:`~grow_grafana_loki.LokiClient`.

    Common fields (``url``, timeouts, ``retry``, ``tls``, ``proxy``, ``headers``,
    ``user_agent_suffix``, ``hooks``, ``logger``) come from :class:`BaseConfig`. Durations are
    float seconds; ``datetime.timedelta`` is accepted.

    Loki itself has no authentication: ``tenant_id`` is sent as ``X-Scope-OrgID`` (needed when
    Loki runs with ``auth_enabled: true``), and Basic (``username``/``password``) or Bearer
    (``bearer_token``) credentials are for gateways and hosted Loki (Grafana Cloud: Basic user =
    instance id, password = access token). The client never interprets them. ``password`` and
    ``bearer_token`` accept ``str`` and are stored as :class:`Secret`; ``repr()`` never shows
    them.

    Environment (``from_env``; explicit value > ``GROW_GRAFANA_LOKI_*`` > vendor > default):

    ===============  ========================================================================
    ``url``          ``GROW_GRAFANA_LOKI_URL``, ``LOKI_URL``
    ``tenant_id``    ``GROW_GRAFANA_LOKI_TENANT_ID``, ``LOKI_TENANT_ID``
    ``username``     ``GROW_GRAFANA_LOKI_USERNAME``, ``LOKI_USERNAME``, ``LOKI_BASIC_AUTH_USER``
    ``password``     ``GROW_GRAFANA_LOKI_PASSWORD``, ``LOKI_PASSWORD``,
                     ``LOKI_BASIC_AUTH_PASSWORD``
    ``bearer_token`` ``GROW_GRAFANA_LOKI_BEARER_TOKEN``, ``LOKI_BEARER_TOKEN``
    ===============  ========================================================================

    plus the common ``CONNECT_TIMEOUT``, ``REQUEST_TIMEOUT``, ``TOTAL_TIMEOUT``,
    ``MAX_RETRIES``, ``CA_CERT``, ``CLIENT_CERT``, ``CLIENT_KEY``, ``INSECURE_SKIP_VERIFY`` and
    ``PROXY`` (all with the ``GROW_GRAFANA_LOKI_`` prefix).

    Attributes:
        tenant_id: sent as ``X-Scope-OrgID`` on every request and on the tail handshake
            (``None`` sends no header). See :func:`tenant_problem` for the rules.
        username: HTTP Basic user (together with ``password``).
        password: HTTP Basic password.
        bearer_token: sends ``Authorization: Bearer <token>`` (not together with Basic).
        multi_tenant_queries: allow tenant ids joined with ``|`` (the server must enable
            ``multi_tenant_queries_enabled``).
        allowed_labels: cardinality guard allowlist used by ``push`` and the log exporter;
            ``None`` means :data:`DEFAULT_ALLOWED_LABELS`.
        disable_label_guard: send every stream label of ``push`` as an index label (the
            ``LogRecord`` exporter always applies the guard).
    """

    ENV_VENDOR: ClassVar[Mapping[str, tuple[str, ...]]] = {
        "URL": ("LOKI_URL",),
        "TENANT_ID": ("LOKI_TENANT_ID",),
        "USERNAME": ("LOKI_USERNAME", "LOKI_BASIC_AUTH_USER"),
        "PASSWORD": ("LOKI_PASSWORD", "LOKI_BASIC_AUTH_PASSWORD"),
        "BEARER_TOKEN": ("LOKI_BEARER_TOKEN",),
    }

    url: str = DEFAULT_URL
    tenant_id: str | None = None
    username: str | None = None
    password: Secret | None = None
    bearer_token: Secret | None = None
    multi_tenant_queries: bool = False
    allowed_labels: Sequence[str] | None = None
    disable_label_guard: bool = False

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.allowed_labels is not None:
            object.__setattr__(self, "allowed_labels", tuple(self.allowed_labels))

    @classmethod
    def _env_values(cls, reader: EnvReader) -> dict[str, Any]:
        values = super()._env_values(reader)
        vendor = cls.ENV_VENDOR
        for field, suffix in (("tenant_id", "TENANT_ID"), ("username", "USERNAME")):
            value = reader.string(suffix, vendor.get(suffix, ()))
            if value is not None:
                values[field] = value
        for field, suffix in (("password", "PASSWORD"), ("bearer_token", "BEARER_TOKEN")):
            value = reader.string(suffix, vendor.get(suffix, ()))
            if value is not None:
                values[field] = Secret(value)
        return values

    @property
    def effective_allowed_labels(self) -> tuple[str, ...]:
        """The allowlist the cardinality guard uses (``allowed_labels`` or the default)."""
        if self.allowed_labels is None:
            return DEFAULT_ALLOWED_LABELS
        return tuple(self.allowed_labels)

    def auth_headers(self) -> dict[str, str]:
        """``Authorization`` and ``X-Scope-OrgID`` headers for every request (may be empty)."""
        headers: dict[str, str] = {}
        if self.username:
            password = self.password.reveal() if self.password is not None else ""
            headers["Authorization"] = "Basic " + _basic_token(self.username, password)
        elif self.bearer_token is not None:
            headers["Authorization"] = "Bearer " + self.bearer_token.reveal()
        if self.tenant_id:
            headers["X-Scope-OrgID"] = self.tenant_id
        return headers

    def known_secrets(self) -> list[str]:
        """Credential values to redact from error text (password, token, Basic token)."""
        out = super().known_secrets()
        if self.username and self.password is not None:
            out.append(_basic_token(self.username, self.password.reveal()))
        return out

    def validate(self: _C) -> _C:
        """Common checks plus the Loki rules; raises :class:`ConfigError`.

        Username and password go together; Basic and Bearer are mutually exclusive; the tenant
        id follows :func:`tenant_problem`; allowlisted label names must be valid label names.
        """
        super().validate()
        has_password = self.password is not None and bool(self.password.reveal())
        if bool(self.username) != has_password:
            raise ConfigError("config: username and password must be given together")
        if self.username and self.bearer_token is not None:
            raise ConfigError("config: basic credentials and bearer token are mutually exclusive")
        if self.bearer_token is not None and not self.bearer_token.reveal().strip():
            raise ConfigError("config: bearer_token must not be blank")
        if self.tenant_id is not None:
            reason = tenant_problem(self.tenant_id, self.multi_tenant_queries)
            if reason is not None:
                raise ConfigError(f"config: tenant_id {reason}")
        for name in self.allowed_labels or ():
            if not _LABEL_NAME.match(name) or name.startswith("__"):
                raise ConfigError(f"config: allowed_labels contains an invalid label name {name!r}")
        return self


def _basic_token(username: str, password: str) -> str:
    return base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
