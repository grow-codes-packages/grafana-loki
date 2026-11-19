# Client and status

Create one client per process, reuse it everywhere and close it on shutdown. `close()` is
idempotent: it flushes the log clients created from this client and stops open tails. Calls on a
closed client fail with `CANCELLED`.

| Function | Loki API | Result |
| - | - | - |
| `fromEnv()` | — | client configured from `GROW_GRAFANA_LOKI_*` / `LOKI_*` ([Configuration](../configuration.md#grafana-loki-settings)) |
| `health()` | `GET /ready` | `HealthStatus{healthy, status, details}`; never throws for an unhealthy or unreachable server (`status` is Loki's text, e.g. `Ingester not ready: waiting for 15s after being ready`, or `unreachable`) |
| `ready()` | `GET /ready` | `true` when Loki answers 200 |
| `ping()` | `GET /loki/api/v1/labels?since=5m` | latency of an authenticated call; `AUTHENTICATION` when the tenant or gateway credentials are missing or wrong |
| `buildInfo()` | `GET /loki/api/v1/status/buildinfo` | `version`, `revision`, `branch`, `buildUser`, `buildDate`, `goVersion` |
| `serverVersion()` | same | the version string, e.g. `3.7.10` |
| `withTenant(id)` | — | a client for another tenant (`X-Scope-OrgID: id` on every request and the tail handshake) that shares the connections; the original client is unchanged; `VALIDATION` for an invalid id |
| `close()` | — | flushes log clients, stops tails, releases connections (children from `withTenant` leave the shared connections open) |

=== "TypeScript"

    ```ts
    import { LokiClient } from "@grow-codes/grafana-loki";

    const client = LokiClient.fromEnv();
    try {
      const health = await client.health(); // { healthy, status, details }
      if (!health.healthy) console.warn("Loki is not ready:", health.status);
      console.log(await client.ready(), await client.ping(), "ms");
      const info = await client.buildInfo();
      console.log(info.version === (await client.serverVersion()));

      const teamA = client.withTenant("team-a"); // X-Scope-OrgID: team-a
      console.log(await teamA.labels());
    } finally {
      await client.close();
    }
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::{LabelsOptions, LokiClient};

    let client = LokiClient::from_env()?;
    let health = client.health().await?;
    if !health.healthy {
        eprintln!("Loki is not ready: {}", health.status);
    }
    println!("{} {:?}", client.ready().await?, client.ping().await?);
    let info = client.build_info().await?;
    assert_eq!(info.version, client.server_version().await?);

    let team_a = client.with_tenant("team-a")?;
    println!("{:?}", team_a.labels(LabelsOptions::default()).await?);
    client.close().await;
    ```

=== "Go"

    ```go
    client, err := loki.NewClientFromEnv()
    if err != nil {
        return err
    }
    defer client.Close()

    health, err := client.Health(ctx)
    if err != nil {
        return err
    }
    if !health.Healthy {
        log.Println("Loki is not ready:", health.Status)
    }
    latency, err := client.Ping(ctx) // errors.Is(err, loki.ErrAuthentication)
    info, err := client.BuildInfo(ctx)
    version, err := client.ServerVersion(ctx)

    teamA, err := client.WithTenant("team-a")
    labels, err := teamA.Labels(ctx, loki.LabelsOptions{})
    ```

=== "Python"

    ```python
    from grow_grafana_loki import LokiClient

    with LokiClient.from_env() as client:
        health = client.health()
        if not health.healthy:
            print("Loki is not ready:", health.status)
        print(client.ready(), client.ping(), "ms")
        info = client.build_info()
        assert info.version == client.server_version()

        team_a = client.with_tenant("team-a")
        print(team_a.labels())
    ```

    `AsyncLokiClient` has the same methods (`async with AsyncLokiClient.from_env() as client:`,
    `await client.health()`, `await client.aclose()`).

=== "Java"

    ```java
    import codes.grow.packages.loki.LokiClient;

    try (LokiClient client = LokiClient.fromEnv()) {
        var health = client.health();
        if (!health.healthy()) {
            System.err.println("Loki is not ready: " + health.status());
        }
        System.out.println(client.ready() + " " + client.ping());
        var info = client.buildInfo();
        System.out.println(info.version().equals(client.serverVersion()));

        LokiClient teamA = client.withTenant("team-a");
        System.out.println(teamA.labels());
    }
    ```

    Every I/O method has a `…Async` variant returning `CompletableFuture`
    (`client.healthAsync()`, `client.pingAsync()`, …).

## Multi-tenancy

With `auth_enabled: true` Loki requires `X-Scope-OrgID` on every data API and answers
`401 no org id` without it (`AUTHENTICATION`). Configure `tenantId` once or derive per-tenant
clients with `withTenant(id)`. Tenant ids follow Loki's rules (at most 150 bytes, not `.`/`..`,
only `A-Z a-z 0-9 ! - _ . * ' ( )`). `a|b` queries several tenants at once and needs
`multiTenantQueries: true` in the config **and** `multi_tenant_queries_enabled` on the server;
pushing to `a|b` is always rejected by Loki (`400 multiple org IDs present`).
