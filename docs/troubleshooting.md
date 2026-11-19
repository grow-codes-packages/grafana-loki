# Troubleshooting

## The client cannot connect

- `health()` returns `healthy: false, status: "unreachable"`: the URL is wrong or the service is
  down. Check `GROW_GRAFANA_LOKI_URL` (default `http://127.0.0.1:3100`) and that the port is
  reachable from where your code runs (inside Docker, `127.0.0.1` is the container itself).
- Calls fail with `UNAVAILABLE` after several seconds: the client retried with backoff. Lower
  `maxRetries` or `totalTimeout` to fail faster.
- Behind a corporate proxy: `HTTP_PROXY`/`HTTPS_PROXY` are **ignored** on purpose. Set `proxy`
  (or `GROW_GRAFANA_LOKI_PROXY`).

## `CONFIG` errors

The message names the setting or environment variable, never its value. Typical causes: a URL
without `http://` or `https://`, a duration such as `10 sec` (use `10s` or `10000`), a client
certificate without its key, `MAX_RETRIES` below 0.

## `AUTHENTICATION` or `PERMISSION_DENIED`

- `ping()` is the quickest credential check: it makes one authenticated call.
- `401 no org id`: the Loki server runs with `auth_enabled: true` and needs a tenant. Set
  `tenantId` (`GROW_GRAFANA_LOKI_TENANT_ID` or `LOKI_TENANT_ID`) or use `client.withTenant(id)`.
- Behind a gateway or on Grafana Cloud, check the credentials the SDK reads:
  `GROW_GRAFANA_LOKI_USERNAME` / `GROW_GRAFANA_LOKI_PASSWORD` (or `LOKI_USERNAME` /
  `LOKI_PASSWORD`, `LOKI_BASIC_AUTH_USER` / `LOKI_BASIC_AUTH_PASSWORD`) or
  `GROW_GRAFANA_LOKI_BEARER_TOKEN` (`LOKI_BEARER_TOKEN`); explicit config values win over the
  environment ([Configuration](configuration.md#grafana-loki-settings)).
- The SDK never prints credentials, so `toString()` of the config shows `[REDACTED]` even when a
  password or token is set.

## TLS errors

- Self-signed or private CA: set `tls.caCertPath` (`GROW_GRAFANA_LOKI_CA_CERT`).
- Mutual TLS: set both `tls.clientCertPath` and `tls.clientKeyPath`.
- `tls.insecureSkipVerify` only for local tests.

## `RATE_LIMITED`

The service asked the client to slow down. The client waits for `Retry-After` up to
`maxRetryAfter` (60 s); longer waits fail at once with `retryAfter` set on the error so your code
can schedule a later attempt.

## `TIMEOUT`

`requestTimeout` (30 s) bounds one attempt and `totalTimeout` (120 s) the whole call including
retries. Raise them for slow operations, or pass a per-call deadline (signal, ctx, cancel token,
future timeout).

## See what the client does

Turn on the [logger](hooks.md#logger) or add [hooks](hooks.md#hooks): both see every attempt with
redacted headers and URLs.

## Docker integration tests fail

- Run one language: `make test-docker LANGS=go`.
- In CI, download the `compose-logs-<lang>` artifact of the failed job.
- Locally, the stack is removed after each run; rerun with Docker logs open
  (`docker compose -f docker/compose.yml logs -f`) in a second terminal.

## Still stuck?

Open an issue with the language, package version, Grafana Loki version and a minimal reproduction
(with secrets removed): <https://github.com/grow-codes-packages/grafana-loki/issues>.

## Loki-specific problems

- **Every push fails with `UNAVAILABLE` "Ingester is shutting down"** although reads work: the
  filesystem that holds Loki's WAL is at least 90 % full (`-ingester.wal-disk-full-threshold`,
  default 0.9) and Loki throttles all writes. The SDK retries it as a transient error; free disk
  space on the Loki volume.
- **`push` succeeds but a selector finds nothing**: the cardinality guard moved the label to
  structured metadata. Filter it with `{service_name="x"} | run="r1"` or add the name to
  `allowedLabels`.
- **`400 … timestamp too old` / `too new`**: Loki rejects entries older than
  `reject_old_samples_max_age` (7 days in the Grow Codes config) or more than about 10 minutes in
  the future. The whole push is rejected.
- **`query()` rejects a selector with `VALIDATION`**: log selectors are not instant queries in
  Loki; use `queryRange` or `search`.
- **`volume` / `volumeRange` return no data**: the server needs `limits_config.volume_enabled:
  true` (the image default config has it off). `indexStats` and `volumeRange` read the TSDB index,
  which sees data only after the ingester flushed its chunks.
- **A tail sees old lines**: pass no `start` (the default is "live only") or a recent one; with an
  explicit `start` Loki replays from there (up to `limit`).
