# Python examples

Install the package, point it at a running Grafana Loki and run an example:

```bash
pip install grow-grafana-loki
export GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100
export LOKI_TENANT_ID=...            # only for multi-tenant Loki (X-Scope-OrgID)
export LOKI_USERNAME=... LOKI_PASSWORD=...   # or LOKI_BEARER_TOKEN, for gateways / Grafana Cloud
python examples/python/basic.py
```

| File | Shows |
| - | - |
| `basic.py` | sync client: health, ping, server version, error handling |
| `async_basic.py` | async client, cancellation by task |
| `hooks_and_logging.py` | request hooks, the `grow_grafana_loki` logger, retries and timeouts |
| `push_and_query.py` | push (JSON + protobuf), LogQL builder, query, query_range, labels, series, volume |
| `logging_client.py` | batching log client, `Logger`, `logging.Handler`, search and `wait_for_log` |
| `tail_and_tenants.py` | WebSocket live tail and `with_tenant()` |
