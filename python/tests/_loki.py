"""Helpers for Loki unit tests: recorded fixtures as mock replies and request inspection."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import grow_grafana_loki as sdk
from _support import MockServer, Reply, Seen, make_config

FIXTURES = Path(__file__).parent / "fixtures"


def fx(name: str) -> dict[str, Any]:
    """A recorded fixture ``{request, response}`` (``tests/fixtures/<name>.json``)."""
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def fx_reply(name: str, **headers: str) -> Reply:
    """The recorded response of a fixture as a mock reply (status, content type, body)."""
    resp = fx(name)["response"]
    body = resp.get("body")
    if isinstance(body, dict) and "base64" in body:
        payload: Any = base64.b64decode(body["base64"])
    elif isinstance(body, (dict, list)):
        payload = json.dumps(body).encode()
    else:
        payload = (body or "").encode()
    ctype = resp.get("contentType") or "text/plain"
    return Reply(resp["status"], payload, {"Content-Type": ctype, **headers})


def json_reply(body: Any, status: int = 200) -> Reply:
    return Reply(status, json.dumps(body).encode(), {"Content-Type": "application/json"})


def text_reply(text: str, status: int) -> Reply:
    return Reply(status, text.encode(), {"Content-Type": "text/plain; charset=utf-8"})


def query_of(seen: Seen) -> dict[str, list[str]]:
    return parse_qs(urlsplit(seen.path).query, keep_blank_values=True)


def last(server: MockServer, path: str) -> Seen:
    return [s for s in server.seen if s.path.split("?", 1)[0] == path][-1]


def client(server: MockServer, **kw: Any) -> sdk.LokiClient:
    return sdk.LokiClient(make_config(server.url, **kw))


def aclient(server: MockServer, **kw: Any) -> sdk.AsyncLokiClient:
    return sdk.AsyncLokiClient(make_config(server.url, **kw))
