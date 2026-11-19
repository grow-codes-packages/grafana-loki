# Quickstart

Connect to Grafana Loki, check that it is ready, push a log line, read it back and close the
client. The client reads `GROW_GRAFANA_LOKI_URL` and the optional tenant and credential variables
(and the vendor variables listed in [Configuration](configuration.md#grafana-loki-settings)), or
takes the values in code.

```sh
export GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100
export GROW_GRAFANA_LOKI_TENANT_ID=team-a        # only for a multi-tenant Loki (X-Scope-OrgID)
# behind a gateway / Grafana Cloud: GROW_GRAFANA_LOKI_USERNAME + GROW_GRAFANA_LOKI_PASSWORD
# or GROW_GRAFANA_LOKI_BEARER_TOKEN, from your secret store (never commit them)
```

=== "TypeScript"

    ```ts
    import { LogQL, LokiClient } from "@grow-codes/grafana-loki";

    const client = LokiClient.fromEnv();
    try {
      const health = await client.health();
      console.log("healthy:", health.healthy, health.status, await client.serverVersion());

      // Push one stream; labels outside the allowlist go to structured metadata.
      await client.push([
        {
          labels: { service_name: "checkout", level: "error" },
          entries: [{ line: "payment failed", structuredMetadata: { order_id: "o-1" } }],
        },
      ]);

      // Read it back with a safely built LogQL query.
      const logql = LogQL.selector({ service_name: "checkout" }).lineContains("payment").build();
      const result = await client.queryRange(logql, { sinceMs: 5 * 60_000, limit: 10 });
      for (const s of result.streams)
        for (const e of s.entries) console.log(e.timestamp, e.line, e.structuredMetadata);
    } finally {
      await client.close();
    }
    ```

=== "Rust"

    ```rust
    use std::time::Duration;
    use grow_grafana_loki::{Entry, LogQL, LokiClient, PushOptions, QueryRangeOptions, Stream};

    #[tokio::main]
    async fn main() -> grow_grafana_loki::Result<()> {
        let client = LokiClient::from_env()?;
        let health = client.health().await?;
        println!("healthy: {} {} {}", health.healthy, health.status, client.server_version().await?);

        // Push one stream; labels outside the allowlist go to structured metadata.
        let stream = Stream::new([("service_name", "checkout"), ("level", "error")])
            .entry(Entry::new("payment failed").metadata("order_id", "o-1"));
        client.push(&[stream], PushOptions::default()).await?;

        // Read it back with a safely built LogQL query.
        let logql = LogQL::selector([("service_name", "checkout")]).line_contains("payment").build()?;
        let opts = QueryRangeOptions { since: Some(Duration::from_secs(300)), limit: 10, ..Default::default() };
        for s in client.query_range(&logql, opts).await?.streams {
            for e in s.entries {
                println!("{:?} {} {:?}", e.timestamp, e.line, e.structured_metadata);
            }
        }
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
    	"time"

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

    	// Push one stream; labels outside the allowlist go to structured metadata.
    	err = client.Push(ctx, []loki.Stream{{
    		Labels:  map[string]string{"service_name": "checkout", "level": "error"},
    		Entries: []loki.Entry{{Line: "payment failed", StructuredMetadata: map[string]string{"order_id": "o-1"}}},
    	}}, loki.PushOptions{})
    	if err != nil {
    		log.Fatal(err) // errors.Is(err, loki.ErrAuthentication), loki.ErrValidation, ...
    	}

    	// Read it back with a safely built LogQL query.
    	logql, err := loki.Selector(map[string]string{"service_name": "checkout"}).LineContains("payment").Build()
    	if err != nil {
    		log.Fatal(err)
    	}
    	res, err := client.QueryRange(ctx, logql, loki.QueryRangeOptions{Since: 5 * time.Minute, Limit: 10})
    	if err != nil {
    		log.Fatal(err)
    	}
    	for _, s := range res.Streams {
    		for _, e := range s.Entries {
    			fmt.Println(e.Timestamp, e.Line, e.StructuredMetadata)
    		}
    	}
    }
    ```

=== "Python"

    ```python
    from grow_grafana_loki import LogQL, LokiClient, LokiEntry, LokiStream

    with LokiClient.from_env() as client:
        health = client.health()
        print("healthy:", health.healthy, health.status, client.server_version())

        # Push one stream; labels outside the allowlist go to structured metadata.
        client.push([
            LokiStream(
                {"service_name": "checkout", "level": "error"},
                [LokiEntry("payment failed", structured_metadata={"order_id": "o-1"})],
            )
        ])

        # Read it back with a safely built LogQL query.
        logql = LogQL.selector({"service_name": "checkout"}).line_contains("payment").build()
        for stream in client.query_range(logql, since_s=300, limit=10).streams:
            for entry in stream.entries:
                print(entry.timestamp, entry.line, entry.structured_metadata)
    ```

=== "Java"

    ```java
    import codes.grow.packages.loki.*;
    import java.time.Duration;
    import java.util.List;
    import java.util.Map;

    try (LokiClient client = LokiClient.fromEnv()) {
        var health = client.health();
        System.out.println("healthy: " + health.healthy() + " " + client.serverVersion());

        // Push one stream; labels outside the allowlist go to structured metadata.
        client.push(List.of(LokiStream.of(
            Map.of("service_name", "checkout", "level", "error"),
            LokiEntry.of(java.time.Instant.now(), "payment failed", Map.of("order_id", "o-1")))));

        // Read it back with a safely built LogQL query.
        String logql = LogQL.selector(Map.of("service_name", "checkout")).lineContains("payment").build();
        QueryResult result = client.queryRange(logql,
            QueryRangeOptions.builder().since(Duration.ofMinutes(5)).limit(10).build());
        for (StreamResult s : result.streams()) {
            for (StreamEntry e : s.entries()) {
                System.out.println(e.timestamp() + " " + e.line() + " " + e.structuredMetadata());
            }
        }
    }
    ```

Async code: Python `AsyncLokiClient` (`async with AsyncLokiClient.from_env() as client:` and
`await` on every call), Java `…Async` methods returning `CompletableFuture`, Rust's `blocking`
feature for synchronous code.

## Next steps

- [Configuration](configuration.md): timeouts, retries, TLS, proxy, environment variables.
- [Functions](functions.md): the Grafana Loki API of this SDK (push, query, tail, LogQL builder).
- [Logging](logging.md): batching log client, logger facade and framework adapters.
- [Error handling](errors.md): error codes and retries.
