# grow-grafana-loki (Python)

Python SDK for [Grafana Loki](https://grow-codes-packages.github.io/grafana-loki/) by Grow Codes.
Unofficial and community maintained; "Grafana Loki" is a trademark of Grafana Labs.
Tested against `grafana/loki:3.7.10`.

```bash
pip install grow-grafana-loki            # JSON push, queries, tail, logging
pip install "grow-grafana-loki[snappy]"  # + real Snappy compression for protobuf push
```

```python
import grow_grafana_loki as sdk

# Reads GROW_GRAFANA_LOKI_URL / LOKI_URL, LOKI_TENANT_ID, LOKI_USERNAME + LOKI_PASSWORD
# or LOKI_BEARER_TOKEN from the environment.
with sdk.LokiClient.from_env() as client:
    client.push([sdk.LokiStream({"service_name": "api"}, [sdk.LokiEntry("hello", None, {"user": "42"})])])
    query = sdk.LogQL.selector({"service_name": "api"}).line_contains("hello")
    for stream in client.query_range(str(query)).streams:
        for entry in stream.entries:
            print(entry.timestamp, entry.line, entry.structured_metadata)

    logs = client.log_client(resource={"service.name": "api"})  # batching LogClient
    logs.logger("http").info("request done", {"status": 200})
```

The async client has the same methods:

```python
import asyncio
import grow_grafana_loki as sdk

async def main() -> None:
    async with sdk.AsyncLokiClient.from_env() as client:
        print(await client.labels())

asyncio.run(main())
```

| Area | Functions |
| - | - |
| Core | `health()`, `ready()`, `ping()`, `server_version()`, `build_info()`, `with_tenant(id)`, `close()` |
| Push | `push(streams, format="json"/"protobuf", gzip=...)`, `push_otlp(records)`, `LokiStream`, `LokiEntry`, `CardinalityGuard` |
| Query | `query()`, `query_range()`, `labels()`, `label_values()`, `series()`, `tail()`, `index_stats()`, `volume()`, `volume_range()`, `format_query()` |
| LogQL | `LogQL.selector({...}).line_contains(...).json().where("status", ">=", 500)` |
| Logging | `log_client()`, `otlp_log_client()`, `log_query()` (search, tail, list_streams, list_labels, wait_for_log), `LogHandler`, `structlog_processor` |

- Python 3.10 or newer, typed (`py.typed`), `httpx` + `websockets` based, environment proxies
  ignored unless configured.
- Retries with full-jitter backoff and `Retry-After`, connect/request/total timeouts,
  cancellation, hooks and a silent-by-default `logging` logger named `grow_grafana_loki`.
- Credentials are wrapped in `Secret` and never appear in `repr()`, logs or error messages.
- Protobuf push is hand-encoded from Loki's Apache-2.0 `pkg/push/push.proto` field numbers; no
  AGPL Loki code is used.
- `grow_grafana_loki.testing` ships in-memory fakes and the conformance runner.

Documentation: https://grow-codes-packages.github.io/grafana-loki/ ·
Source: https://github.com/grow-codes-packages/grafana-loki · Licence: Apache-2.0
