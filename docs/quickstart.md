# Quickstart

Connect to Grafana Loki, check that it is healthy, make an authenticated call and close the client.
The client reads `GROW_GRAFANA_LOKI_URL` and `GROW_GRAFANA_LOKI_TOKEN` (and the vendor variables
listed in [Configuration](configuration.md)), or takes the values in code.

```sh
export GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100
export GROW_GRAFANA_LOKI_TOKEN=...   # never commit real tokens
```

=== "TypeScript"

    ```ts
    import { LokiClient } from "@grow-codes/grafana-loki";

    const client = LokiClient.fromEnv();
    try {
      const health = await client.health();
      console.log("healthy:", health.healthy, health.status);
      console.log("ping:", await client.ping(), "ms");
      console.log("server version:", await client.serverVersion());
    } finally {
      await client.close();
    }
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::LokiClient;

    #[tokio::main]
    async fn main() -> grow_grafana_loki::Result<()> {
        let client = LokiClient::from_env()?;
        let health = client.health().await?;
        println!("healthy: {} {}", health.healthy, health.status);
        println!("ping: {:?}", client.ping().await?);
        println!("server version: {}", client.server_version().await?);
        client.close().await;
        Ok(())
    }
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "fmt"
        "log"

        loki "github.com/grow-codes-packages/grafana-loki/go"
    )

    func main() {
        ctx := context.Background()
        client, err := loki.NewClientFromEnv()
        if err != nil {
            log.Fatal(err)
        }
        defer client.Close()

        health, err := client.Health(ctx)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("healthy:", health.Healthy, health.Status)

        latency, err := client.Ping(ctx)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("ping:", latency)

        version, err := client.ServerVersion(ctx)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("server version:", version)
    }
    ```

=== "Python"

    ```python
    from grow_grafana_loki import LokiClient

    with LokiClient.from_env() as client:
        health = client.health()
        print("healthy:", health.healthy, health.status)
        print("ping:", client.ping(), "ms")
        print("server version:", client.server_version())
    ```

    Async code uses `AsyncLokiClient` with the same methods:

    ```python
    import asyncio
    from grow_grafana_loki import AsyncLokiClient

    async def main() -> None:
        async with AsyncLokiClient.from_env() as client:
            print(await client.health())

    asyncio.run(main())
    ```

=== "Java"

    ```java
    import codes.grow.packages.loki.LokiClient;
    import codes.grow.packages.loki.core.HealthStatus;

    public final class Quickstart {
      public static void main(String[] args) {
        try (LokiClient client = LokiClient.fromEnv()) {
          HealthStatus health = client.health();
          System.out.println("healthy: " + health.healthy() + " " + health.status());
          System.out.println("ping: " + client.ping().toMillis() + " ms");
          System.out.println("server version: " + client.serverVersion());
        }
      }
    }
    ```

## Next steps

- [Configuration](configuration.md): timeouts, retries, TLS, proxy, environment variables.
- [Functions](functions.md): the Grafana Loki API of this SDK.
- [Error handling](errors.md): error codes and retries.
