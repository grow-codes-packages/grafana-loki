# Push

`push(streams)` writes log lines with `POST /loki/api/v1/push`. A stream is a label set plus its
entries; every entry has a timestamp (default: now), a line and optional **structured metadata**
(string → string, stored next to the line but not indexed).

| Option | Default | Meaning |
| - | - | - |
| format | JSON | `json` or `protobuf` (a Snappy-compressed `logproto.PushRequest`, encoded from the Apache-2.0 `push.proto` field numbers) |
| gzip | `false` | gzip the JSON body (protobuf is Snappy-compressed already) |

Behaviour (identical in every language):

1. **Cardinality guard**: labels outside `allowedLabels` move to every entry's structured metadata
   (an entry's own metadata wins), unless `disableLabelGuard` is set. Default allowlist:
   `service_name`, `service_namespace`, `deployment_environment`, `host_name`, `level`, `job`,
   `instance`, `namespace`, `pod`, `container` ([Configuration](../configuration.md#cardinality-guard)).
2. **Validation** before sending (`VALIDATION`): label names match `[a-zA-Z_][a-zA-Z0-9_]*` and do
   not start with `__`, label values are non-empty, metadata names are non-empty, lines are text.
3. Streams without entries are skipped; an empty list sends nothing.
4. Loki accepts or rejects the whole request. Entries older than `reject_old_samples_max_age`
   or too far in the future → `VALIDATION`. A multi-tenant Loki without tenant → `AUTHENTICATION`.
5. Retrying is safe: Loki drops entries identical to stored ones (same labels, timestamp and
   line), so push is retried on 429/5xx with the identical payload. Status 260 (ingestion blocked
   by the operator) is a non-retryable `UNAVAILABLE`; "Ingester is shutting down" (WAL disk at
   least 90 % full) is a retryable `UNAVAILABLE`.

=== "TypeScript"

    ```ts
    await client.push([
      {
        labels: { service_name: "checkout", level: "error" },
        entries: [
          { line: "payment failed", structuredMetadata: { order_id: "o-1" } },
          { timestamp: new Date(), line: "payment retried" }, // Date, bigint ns or ns string
        ],
      },
    ]);
    await client.push(streams, { gzip: true });
    await client.push(streams, { format: "protobuf" });
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::{Entry, PushOptions, Stream};

    let stream = Stream::new([("service_name", "checkout"), ("level", "error")])
        .entry(Entry::new("payment failed").metadata("order_id", "o-1"))
        .entry(Entry::new("payment retried").at(std::time::SystemTime::now()));
    client.push(&[stream.clone()], PushOptions::default()).await?;
    client.push(&[stream.clone()], PushOptions::json(true)).await?; // gzip
    client.push(&[stream], PushOptions::protobuf()).await?;
    ```

=== "Go"

    ```go
    streams := []loki.Stream{{
        Labels: map[string]string{"service_name": "checkout", "level": "error"},
        Entries: []loki.Entry{
            {Line: "payment failed", StructuredMetadata: map[string]string{"order_id": "o-1"}},
            {Line: "payment retried", Timestamp: time.Now()},
        },
    }}
    err := client.Push(ctx, streams, loki.PushOptions{})
    err = client.Push(ctx, streams, loki.PushOptions{Gzip: true})
    err = client.Push(ctx, streams, loki.PushOptions{Format: loki.PushFormatProtobuf})
    ```

=== "Python"

    ```python
    from grow_grafana_loki import LokiEntry, LokiStream

    streams = [
        LokiStream(
            {"service_name": "checkout", "level": "error"},
            [
                LokiEntry("payment failed", structured_metadata={"order_id": "o-1"}),
                LokiEntry("payment retried", timestamp=datetime.now(timezone.utc)),
            ],
        )
    ]
    client.push(streams)
    client.push(streams, gzip=True)
    client.push(streams, format="protobuf")
    ```

=== "Java"

    ```java
    import codes.grow.packages.loki.*;

    LokiStream stream = LokiStream.of(
        Map.of("service_name", "checkout", "level", "error"),
        LokiEntry.of(Instant.now(), "payment failed", Map.of("order_id", "o-1")),
        LokiEntry.of("payment retried"));
    client.push(List.of(stream));
    client.push(List.of(stream), new PushOptions(PushFormat.JSON, true)); // gzip
    client.push(List.of(stream), PushOptions.of(PushFormat.PROTOBUF));
    ```

## Cardinality guard in practice

```text
push   labels {service_name="api", request_id="r-1"}  line "GET /"
stored labels {service_name="api"}                     metadata {request_id="r-1"}
query  {service_name="api"} | request_id="r-1"
```

The guard is exposed as `CardinalityGuard` in every language (`allowed`, `isAllowed(name)`,
`apply(stream)`), together with `DEFAULT_ALLOWED_LABELS`, `normalizeLabelName` (`service.name` →
`service_name`) and the label-set formatter used by `listStreams()`.

## `pushOtlp(records)`

Sends `LogRecord`s as OTLP/JSON to Loki's native OTLP endpoint `POST /otlp/v1/logs`. Loki itself
decides the labels: `service.name` → `service_name`, `deployment.environment` and its other
default resource attributes become index labels; everything else (attributes, `trace_id`,
`span_id`, `severity_text`, …) becomes structured metadata with `.` replaced by `_`. Use it when
you want Loki's OTLP mapping instead of the SDK's guard; the OTLP log client
([Logging](logging.md)) batches the same call.

=== "TypeScript"

    ```ts
    await client.pushOtlp(
      [{ timestamp: new Date(), severity: Severity.Warn, severityText: "WARN", body: "slow request",
         attributes: { "http.route": "/pay" }, resource: { "service.name": "checkout" } }],
      { gzip: true },
    );
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::logging::{LogRecord, Severity};

    let mut record = LogRecord::new(Severity::Warn, "slow request");
    record.resource.insert("service.name".into(), "checkout".into());
    client.push_otlp(&[record], true).await?; // gzip
    ```

=== "Go"

    ```go
    err := client.PushOTLP(ctx, []loki.LogRecord{{
        Timestamp: time.Now(), Severity: loki.SeverityWarn, Body: "slow request",
        Resource: map[string]loki.AttrValue{"service.name": "checkout"},
    }}, loki.OTLPPushOptions{Gzip: true})
    ```

=== "Python"

    ```python
    from grow_grafana_loki import LogRecord, Severity

    client.push_otlp(
        [LogRecord(body="slow request", severity=Severity.WARN,
                   resource={"service.name": "checkout"})],
        gzip=True,
    )
    ```

=== "Java"

    ```java
    LogRecord record = LogRecord.builder()
        .severity(Severity.WARN)
        .body("slow request")
        .resource(Map.of("service.name", "checkout"))
        .build();
    client.pushOtlp(List.of(record), true); // gzip
    ```
