# Loki log client and search

The logging category ([Logging](../logging.md)) works the same for every logging service. This
page lists what is specific to Loki: how the log clients are created, how records become Loki
streams, and how search, tail, streams and labels map onto the Loki API.

## Log clients

| | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| push API (JSON) | `client.logClient()` | `client.new_log_client(opts)` | `client.NewLogClient(opts)` | `client.log_client()` | `client.logClient()` |
| push API (protobuf) | `logClient({ protocol: "protobuf" })` | `new_log_client_with_exporter(Arc::new(LokiLogExporter::new(&client, PushFormat::Protobuf)), opts)` | `NewLogClientWithExporter(loki.NewLokiLogExporter(c, loki.PushFormatProtobuf), opts)` | `log_client(format="protobuf")` | `logClient(…protocol(LogProtocol.PROTOBUF))` |
| OTLP (`/otlp/v1/logs`) | `logClient({ protocol: "otlp" })` | `new_otlp_log_client(opts)` | `NewOTLPLogClient(opts)` | `otlp_log_client()` | `logClient(…protocol(LogProtocol.OTLP))` |
| exporters | `LokiLogExporter`, `LokiOtlpExporter` | `LokiLogExporter` (= `PushExporter`), `LokiOtlpExporter` (= `OtlpExporter`) | `LokiLogExporter` (= `PushExporter`), `LokiOtlpExporter` (= `OTLPExporter`) | `LokiLogExporter`, `LokiOtlpExporter` (+ `Async…`) | `LokiLogExporter`, `LokiOtlpExporter` |

`close()` of the `LokiClient` flushes and closes every log client created from it.

**Record → stream mapping** (push-API exporters): resource and attribute keys are normalized
(`service.name` → `service_name`); keys in the cardinality guard allowlist with non-empty scalar
values become index labels, plus `level` (the lower-case severity name) when allowed; everything
else becomes structured metadata, together with `severity_text`, `severity_number`, `trace_id`,
`span_id` and `trace_flags`. Records with the same final label set share a stream; the record body
is the log line. Loki adds `detected_level` itself. The allowlist comes from the client config
(`allowedLabels`); TypeScript also takes `allowedLabels` per log client.

A rejected push (4xx) rejects the whole batch; retryable failures (429, 5xx, network) are retried
by the batch processor.

=== "TypeScript"

    ```ts
    const logs = client.logClient({ resource: { "service.name": "checkout" } });
    const log = logs.logger("payments");
    log.error("card declined", { order_id: "o-1", password: "never-sent" }); // redacted
    await logs.flush();
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::LogClientOptions;
    use grow_grafana_loki::logging::{LogClient, LogEntry};

    let logs = client.new_log_client(
        LogClientOptions::default().resource_attribute("service.name", "checkout"),
    )?;
    logs.send(LogEntry::from("card declined"));
    logs.flush(None).await?;
    ```

=== "Go"

    ```go
    logs, err := client.NewLogClient(loki.LogClientOptions{
        Resource: map[string]loki.AttrValue{"service.name": "checkout"},
    })
    logger := logs.Logger("payments", nil)
    logger.Error(ctx, "card declined", "order_id", "o-1")
    err = logs.Flush(ctx)
    ```

=== "Python"

    ```python
    logs = client.log_client(resource={"service.name": "checkout"})
    log = logs.logger("payments")
    log.error("card declined", {"order_id": "o-1"})
    logs.flush()
    ```

=== "Java"

    ```java
    BatchingLogClient logs = client.logClient(
        LokiLogClientOptions.builder().resource(Map.of("service.name", "checkout")).build());
    logs.logger("payments").error("card declined", Map.of("order_id", "o-1"));
    logs.flush();
    ```

## Search, tail, streams and labels

| Function | Loki mapping |
| - | - |
| `search(query, {start, end, limit, order, cursor})` | `query_range` (categorized labels), `order` `desc` (newest first, default) → `backward`, `asc` → `forward`; entries of all streams are merge-sorted by time, then label set, then line. `limit` 1–5000 (default 100). A query that does not start with `{` is free text: `{service_name=~".+"} \|= "<text>"` (an empty query selects every stream). |
| pagination | `nextCursor` is an opaque cursor for `<last timestamp ns>:<entries at that timestamp already returned>`; pass it back as `cursor`. When Loki truncated a page inside one nanosecond, the SDK fetches that nanosecond completely (`start=T`, `end=T+1`), so pages never repeat or skip entries that share a timestamp. `total` is not available from Loki. |
| record mapping | `body` = line; `attributes` = structured metadata + parsed labels without the SDK's own keys; `resource` = stream labels with well-known keys mapped back (`service_name` → `service.name`, `service_namespace`, `service_version`, `service_instance_id`, `deployment_environment`, `host_name`); severity from `severity_number`, `severity_text`, `detected_level` or the `level` label; `traceId` / `spanId` / `traceFlags` from `trace_id` / `span_id` / `trace_flags`. |
| `tail(query, onRecord)` | the WebSocket tail ([Tail](tail.md)), live only by default, one call per record in time order |
| `listStreams()` | `series` with `{service_name=~".+"}` over the default window, as sorted `{a="b", c="d"}` strings |
| `listLabels()` | `labels` |
| `waitForLog(query, {timeout})` | polls `search`; Loki data is queryable right after the push |

=== "TypeScript"

    ```ts
    const query = client.logQueryClient();
    let page = await query.search('{service_name="checkout"} |= "declined"', { limit: 100 });
    while (page.nextCursor !== undefined) {
      page = await query.search('{service_name="checkout"} |= "declined"', {
        limit: 100,
        cursor: page.nextCursor,
      });
    }
    const record = await query.waitForLog("card declined", { timeoutMs: 10_000 });
    console.log(await query.listStreams(), await query.listLabels(), record.resource);
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::logging::{LogQueryClient, SearchOptions};

    let query = client.log_query();
    let page = query
        .search(r#"{service_name="checkout"} |= "declined""#, SearchOptions::default().limit(100))
        .await?;
    if let Some(cursor) = page.next_cursor {
        let next = query
            .search(r#"{service_name="checkout"} |= "declined""#, SearchOptions::default().limit(100).cursor(cursor))
            .await?;
        println!("{}", next.records.len());
    }
    println!("{:?} {:?}", query.list_streams().await?, query.list_labels().await?);
    ```

=== "Go"

    ```go
    lq := client.LogQuery()
    page, err := lq.Search(ctx, `{service_name="checkout"} |= "declined"`, loki.SearchOptions{Limit: 100})
    for err == nil && page.NextCursor != "" {
        page, err = lq.Search(ctx, `{service_name="checkout"} |= "declined"`,
            loki.SearchOptions{Limit: 100, Cursor: page.NextCursor})
    }
    rec, err := lq.WaitForLog(ctx, "card declined", loki.WaitForLogOptions{Timeout: 10 * time.Second})
    streams, err := lq.ListStreams(ctx)
    labels, err := lq.ListLabels(ctx)
    ```

=== "Python"

    ```python
    query = client.log_query()
    page = query.search('{service_name="checkout"} |= "declined"', limit=100)
    while page.next_cursor:
        page = query.search(
            '{service_name="checkout"} |= "declined"', limit=100, cursor=page.next_cursor
        )
    record = query.wait_for_log("card declined", timeout_s=10.0)
    print(query.list_streams(), query.list_labels(), record.resource)
    ```

=== "Java"

    ```java
    LokiLogQueryClient query = client.logQueryClient();
    LogSearchResult page = query.search("{service_name=\"checkout\"} |= \"declined\"",
        SearchOptions.builder().limit(100).build());
    while (page.nextCursor().isPresent()) {
        page = query.search("{service_name=\"checkout\"} |= \"declined\"",
            SearchOptions.builder().limit(100).cursor(page.nextCursor().get()).build());
    }
    System.out.println(query.listStreams() + " " + query.listLabels());
    ```
