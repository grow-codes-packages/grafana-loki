# Configuration

`LokiConfig` holds every setting of a client. It is **immutable** after construction,
validated when it is built (invalid values raise a `CONFIG` error) and its string/debug form
**redacts every credential**. The Grafana Loki fields (tenant, Basic/Bearer credentials and the
cardinality guard) are listed in [Grafana Loki settings](#grafana-loki-settings) below.

## Common settings

| Setting | Default | Environment variable |
| - | - | - |
| `url` — endpoint base URL | `http://127.0.0.1:3100` | `GROW_GRAFANA_LOKI_URL` |
| `connectTimeout` — connection setup | 10 s | `GROW_GRAFANA_LOKI_CONNECT_TIMEOUT` |
| `requestTimeout` — per attempt (headers + body) | 30 s | `GROW_GRAFANA_LOKI_REQUEST_TIMEOUT` |
| `totalTimeout` — whole call incl. retries and backoff | 120 s | `GROW_GRAFANA_LOKI_TOTAL_TIMEOUT` |
| `retry` — retry policy (below) | 3 retries | `GROW_GRAFANA_LOKI_MAX_RETRIES` |
| `tls.caCertPath` / `tls.caCertPem` — extra CA | none | `GROW_GRAFANA_LOKI_CA_CERT` (path) |
| `tls.clientCertPath` + `tls.clientKeyPath` — mutual TLS | none | `GROW_GRAFANA_LOKI_CLIENT_CERT`, `GROW_GRAFANA_LOKI_CLIENT_KEY` |
| `tls.insecureSkipVerify` — tests only, logs a warning once | `false` | `GROW_GRAFANA_LOKI_INSECURE_SKIP_VERIFY` |
| `proxy` — proxy URL | none (environment proxies are **ignored**) | `GROW_GRAFANA_LOKI_PROXY` |
| `headers` — extra request headers | `{}` | — |
| `userAgentSuffix` — appended to the User-Agent | none | — |
| `hooks` — request/response callbacks ([Hooks](hooks.md)) | none | — |
| `logger` — diagnostic logger ([Hooks](hooks.md)) | silent | — |

Every request sends `User-Agent: grow-codes-grafana-loki/<version> (<language>)`.

## Grafana Loki settings

| Setting | Default | Environment variables (precedence order) |
| - | - | - |
| `url` | `http://127.0.0.1:3100` | `GROW_GRAFANA_LOKI_URL`, `LOKI_URL` |
| `tenantId` — sent as `X-Scope-OrgID` on every request and on the tail handshake | none (header not sent) | `GROW_GRAFANA_LOKI_TENANT_ID`, `LOKI_TENANT_ID` |
| `username` — HTTP Basic user (gateways, Grafana Cloud instance id) | none | `GROW_GRAFANA_LOKI_USERNAME`, `LOKI_USERNAME`, `LOKI_BASIC_AUTH_USER` |
| `password` — HTTP Basic password / access token (secret) | none | `GROW_GRAFANA_LOKI_PASSWORD`, `LOKI_PASSWORD`, `LOKI_BASIC_AUTH_PASSWORD` |
| `bearerToken` — `Authorization: Bearer …` (secret) | none | `GROW_GRAFANA_LOKI_BEARER_TOKEN`, `LOKI_BEARER_TOKEN` |
| `multiTenantQueries` — allow tenant ids joined with `\|` | `false` | — |
| `allowedLabels` — cardinality guard allowlist | the default list below | — |
| `disableLabelGuard` — `push` sends every stream label as an index label | `false` | — |

Field names follow each language: `tenant_id`, `bearer_token`, `allowed_labels`, … in Python and
Rust, `TenantID`, `BearerToken`, `AllowedLabels`, … in Go.

Validation (`CONFIG` error, the message never contains a value):

- `username` and `password` are given together; Basic and Bearer are mutually exclusive; a blank
  bearer token is rejected.
- Tenant id: not empty, at most 150 bytes, not `.` or `..`, only `A-Z a-z 0-9 ! - _ . * ' ( )`.
  `a|b` (a multi-tenant query) is accepted only with `multiTenantQueries` (the server must enable
  `multi_tenant_queries_enabled`); every part must then be valid. `withTenant(id)` applies the same
  rules (`VALIDATION` error).
- Every `allowedLabels` entry is a valid label name (`[a-zA-Z_][a-zA-Z0-9_]*`, not starting with
  `__`).

Loki itself has **no authentication**: `auth_enabled: true` only makes the tenant header
mandatory. Basic and Bearer credentials are for gateways in front of Loki and for hosted Loki
(Grafana Cloud: user = numeric instance id, password = access token). The aliases
`LOKI_BASIC_AUTH_USER` / `LOKI_BASIC_AUTH_PASSWORD` are the names the Grow Codes platform already
uses.

### Cardinality guard

Every label value creates a new Loki stream, so high-cardinality labels (request ids, user ids,
trace ids) make Loki slow and expensive. `push` and the log exporter therefore keep only
allowlisted names as **index labels** and move every other label or attribute to the entry's
**structured metadata** (an entry's own metadata wins). Structured metadata is stored next to the
line and can still be filtered: `{service_name="api"} | request_id="r-1"`.

Default allowlist: `service_name`, `service_namespace`, `deployment_environment`, `host_name`,
`level`, `job`, `instance`, `namespace`, `pod`, `container`. Set `allowedLabels` to change it, or
`disableLabelGuard` to send `push` streams unchanged (the `LogRecord` exporter always applies the
guard). Attribute keys are normalized the way Loki normalizes OTLP attributes (`service.name` →
`service_name`).

## From the environment

`fromEnv` resolves each setting with this precedence:

1. the value passed in code (overrides),
2. `GROW_GRAFANA_LOKI_<NAME>`,
3. the vendor variables of Grafana Loki (see [Grafana Loki settings](#grafana-loki-settings)), in
   their documented order,
4. the default.

Parsing rules:

- **Durations** accept integer milliseconds (`1500`) or Go-style units: `500ms`, `10s`, `2m`, `1h`,
  combinations such as `1m30s`.
- **Booleans** accept `true/false`, `1/0`, `yes/no`, `on/off` (any case).
- An **empty** variable counts as unset.
- A present but **invalid** value raises a `CONFIG` error that names the variable, never its value.

=== "TypeScript"

    ```ts
    import { LokiClient, LokiConfig } from "@grow-codes/grafana-loki";

    // Environment only
    const client = LokiClient.fromEnv();

    // Environment with explicit overrides (explicit values win)
    const config = LokiConfig.fromEnv({ requestTimeoutMs: 5_000 });

    // Everything in code
    const explicit = new LokiConfig({
      url: "https://grafana-loki.example.com",
      tenantId: "team-a",
      bearerToken: process.env.MY_LOKI_TOKEN,
      connectTimeoutMs: 5_000,
      requestTimeoutMs: 15_000,
      totalTimeoutMs: 60_000,
      retry: { maxRetries: 5, initialBackoffMs: 200 },
      tls: { caCertPath: "/etc/ssl/internal-ca.pem" },
      proxy: "http://proxy.internal:3128",
      headers: { "x-team": "payments" },
      userAgentSuffix: "my-app/1.4",
    });
    const custom = new LokiClient(explicit);
    console.log(String(explicit)); // bearerToken shown as [REDACTED]
    ```

=== "Rust"

    ```rust
    use std::time::Duration;
    use grow_grafana_loki::{LokiClient, LokiConfig, RetryPolicy};

    // Environment only
    let client = LokiClient::from_env()?;

    // Environment with explicit overrides (explicit values win)
    let config = LokiConfig::builder()
        .request_timeout(Duration::from_secs(5))
        .build_from_env()?;

    // Everything in code
    let explicit = LokiConfig::builder()
        .url("https://grafana-loki.example.com")
        .tenant_id("team-a")
        .bearer_token(std::env::var("MY_LOKI_TOKEN").unwrap_or_default())
        .connect_timeout(Duration::from_secs(5))
        .request_timeout(Duration::from_secs(15))
        .total_timeout(Duration::from_secs(60))
        .retry(RetryPolicy::default().with_max_retries(5))
        .ca_cert_path("/etc/ssl/internal-ca.pem")
        .proxy("http://proxy.internal:3128")
        .header("x-team", "payments")
        .user_agent_suffix("my-app/1.4")
        .build()?;
    let custom = LokiClient::new(explicit)?;
    println!("{:?}", custom.config()); // bearer_token shown as [REDACTED]
    ```

=== "Go"

    ```go
    // Environment only
    client, err := loki.NewClientFromEnv()

    // Everything in code: zero values mean "use the default"
    cfg := loki.Config{
        CommonConfig: loki.CommonConfig{
            URL:             "https://grafana-loki.example.com",
            ConnectTimeout:  5 * time.Second,
            RequestTimeout:  15 * time.Second,
            TotalTimeout:    time.Minute,
            Retry:           &loki.RetryPolicy{MaxRetries: 5, InitialBackoff: 200 * time.Millisecond},
            TLS:             loki.TLSConfig{CACertPath: "/etc/ssl/internal-ca.pem"},
            Proxy:           "http://proxy.internal:3128",
            Headers:         map[string]string{"x-team": "payments"},
            UserAgentSuffix: "my-app/1.4",
        },
        TenantID:    "team-a",
        BearerToken: loki.NewSecret(os.Getenv("MY_LOKI_TOKEN")),
    }
    if err := cfg.Validate(); err != nil {
        log.Fatal(err) // errors.Is(err, loki.ErrConfig)
    }
    custom, err := loki.NewClient(cfg)

    // Environment with explicit overrides (explicit values win; nil env = process environment)
    merged, err := loki.ConfigFromEnvWith(loki.Config{
        CommonConfig: loki.CommonConfig{RequestTimeout: 5 * time.Second},
    }, nil)
    fmt.Println(merged) // credentials shown as [REDACTED]
    ```

=== "Python"

    ```python
    import os
    from grow_grafana_loki import RetryPolicy, TlsConfig, LokiClient, LokiConfig

    # Environment only
    client = LokiClient.from_env()

    # Environment with explicit overrides (explicit values win)
    config = LokiConfig.from_env(request_timeout=5.0)

    # Everything in code: durations are float seconds (datetime.timedelta also works)
    explicit = LokiConfig(
        url="https://grafana-loki.example.com",
        tenant_id="team-a",
        bearer_token=os.environ.get("MY_LOKI_TOKEN"),
        connect_timeout=5.0,
        request_timeout=15.0,
        total_timeout=60.0,
        retry=RetryPolicy(max_retries=5, initial_backoff=0.2),
        tls=TlsConfig(ca_cert_path="/etc/ssl/internal-ca.pem"),
        proxy="http://proxy.internal:3128",
        headers={"x-team": "payments"},
        user_agent_suffix="my-app/1.4",
    )
    custom = LokiClient(explicit)
    print(repr(explicit))  # bearer_token shown as [REDACTED]
    ```

=== "Java"

    ```java
    import java.time.Duration;
    import codes.grow.packages.loki.LokiClient;
    import codes.grow.packages.loki.LokiConfig;
    import codes.grow.packages.loki.core.RetryPolicy;

    // Environment only
    LokiClient client = LokiClient.fromEnv();

    // Everything in code
    LokiConfig config = LokiConfig.builder()
        .url("https://grafana-loki.example.com")
        .tenantId("team-a")
        .bearerToken(System.getenv("MY_LOKI_TOKEN"))
        .connectTimeout(Duration.ofSeconds(5))
        .requestTimeout(Duration.ofSeconds(15))
        .totalTimeout(Duration.ofMinutes(1))
        .retry(RetryPolicy.builder().maxRetries(5).initialBackoff(Duration.ofMillis(200)).build())
        .proxy("http://proxy.internal:3128")
        .header("x-team", "payments")
        .userAgentSuffix("my-app/1.4")
        .build();
    LokiClient custom = LokiClient.create(config);
    System.out.println(config); // bearerToken shown as [REDACTED]
    ```

## Retry policy

| Field | Default | Meaning |
| - | - | - |
| `maxRetries` | 3 | retries after the first attempt (so up to 4 attempts) |
| `initialBackoff` | 100 ms | backoff cap of the first retry |
| `maxBackoff` | 10 s | cap of every backoff |
| `multiplier` | 2 | growth of the cap per retry |
| `maxRetryAfter` | 60 s | longest server-requested `Retry-After` the client waits for |

Backoff for retry *n* (0-based) is **full jitter**: a random wait between 0 and
`min(maxBackoff, initialBackoff × multiplier^n)`.

Requests are retried on HTTP 429, 408, 502, 503, 504 and on network errors. **Idempotent**
requests (`GET HEAD OPTIONS PUT DELETE`, or marked idempotent) are also retried on HTTP 500.
Other requests (for example `POST`) are retried only on 429 and on connection errors that happened
before the request was sent, never after an uncertain send.

On 429 and 503 the client honours `Retry-After` (seconds or HTTP date) and waits
`max(retryAfter, backoff)`. When that wait exceeds `maxRetryAfter` or the remaining
`totalTimeout`, it stops at once with `RATE_LIMITED`/`UNAVAILABLE` and sets `retryAfter` on the
error. Set `maxRetries` to 0 to turn retries off.

## Timeouts and cancellation

- `connectTimeout` bounds connection setup, `requestTimeout` each attempt, `totalTimeout` the whole
  call including backoff sleeps. An exhausted timeout raises `TIMEOUT`.
- Cancellation is honoured during requests and backoff sleeps and raises `CANCELLED`:
  TypeScript `{ signal }` (`AbortSignal`), Rust dropping the future, Go `ctx`, Python `cancel=`
  (`CancelToken`) or task cancellation, Java `…Async` futures (`cancel(true)`).

## TLS and proxies

- Add a private CA with `tls.caCertPath` (or the PEM text in `tls.caCertPem`).
- Mutual TLS needs `tls.clientCertPath` and `tls.clientKeyPath` together.
- `tls.insecureSkipVerify` disables certificate checks. Use it for local tests only; the client
  logs a warning once.
- `HTTP_PROXY`/`HTTPS_PROXY` are **ignored** so that credentials never leave through an unexpected
  proxy. Set `proxy` (or `GROW_GRAFANA_LOKI_PROXY`) to use one.
