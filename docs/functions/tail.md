# Tail

`tail(logql, onEntries, options)` opens Loki's WebSocket live tail (`GET /loki/api/v1/tail`). The
auth, tenant and categorize-labels headers go on the handshake. The call returns once the
connection is open; every frame (the new entries of one or more streams) goes to the callback.

| Option | Default | Meaning |
| - | - | - |
| `start` | now ("live only") | replay entries from this time first (Loki would replay the last hour without it) |
| `limit` | Loki default 100 | maximum replayed entries |
| `delayFor` | 0 | let late entries arrive, 0–5 s (whole seconds on the wire) |

- Only log queries can be tailed: a metric query is rejected client side (`VALIDATION`), and so is
  `delayFor` above 5 s.
- Handshake errors are plain HTTP and map like every call: `401 no org id` → `AUTHENTICATION`,
  a LogQL parse error → `VALIDATION`. Loki keeps the TCP connection of a rejected upgrade open for
  120 s; the SDK closes it immediately in every language.
- Entries the server dropped because the client fell behind (`dropped_entries`) are reported
  separately (callback or counter) and do not end the tail.
- Stop with `stop()`; `close()` of the client stops every tail it opened. Reconnecting (with
  `start` = last seen timestamp) is up to the caller; the log query client's `tail` does the same
  per record ([Logging](logging.md)).

=== "TypeScript"

    ```ts
    const tail = await client.tail(
      '{service_name="checkout"} |= "error"',
      (streams) => {
        for (const s of streams) for (const e of s.entries) console.log(s.labels, e.line);
      },
      { delayForMs: 1000, onDropped: (d) => console.warn("dropped", d.length), onError: console.error },
    );
    // … later
    tail.stop();
    await tail.closed;
    ```

=== "Rust"

    ```rust
    use std::time::Duration;
    use grow_grafana_loki::{LiveTailOptions, TailFrame};

    let tail = client
        .tail(
            r#"{service_name="checkout"} |= "error""#,
            |frame: TailFrame| {
                for s in &frame.streams {
                    for e in &s.entries {
                        println!("{:?} {}", s.labels, e.line);
                    }
                }
            },
            LiveTailOptions { delay_for: Duration::from_secs(1), ..Default::default() },
        )
        .await?;
    // … later
    tail.stop().await;
    println!("dropped: {}", tail.dropped_count());
    ```

=== "Go"

    ```go
    tail, err := client.Tail(ctx, `{service_name="checkout"} |= "error"`, func(f loki.TailFrame) {
        for _, s := range f.Streams {
            for _, e := range s.Entries {
                fmt.Println(s.Labels, e.Line)
            }
        }
    }, loki.LiveTailOptions{DelayFor: time.Second})
    if err != nil {
        return err // errors.Is(err, loki.ErrAuthentication) for a missing tenant
    }
    defer tail.Stop()
    <-tail.Done() // ends on Stop, ctx done, server close or error (tail.Err())
    ```

=== "Python"

    ```python
    def on_frame(frame):
        for stream in frame.streams:
            for entry in stream.entries:
                print(stream.labels, entry.line)

    session = client.tail('{service_name="checkout"} |= "error"', on_frame, delay_for_s=1)
    try:
        session.wait(timeout_s=30)
    finally:
        session.stop()
    ```

    `AsyncLokiClient.tail` returns an `AsyncTailSession` (`await session.stop()`).

=== "Java"

    ```java
    try (LokiTail tail = client.tail(
            "{service_name=\"checkout\"} |= \"error\"",
            frame -> frame.streams().forEach(
                s -> s.entries().forEach(e -> System.out.println(s.labels() + " " + e.line()))),
            LiveTailOptions.builder().delayFor(Duration.ofSeconds(1)).build())) {
        Thread.sleep(30_000);
    } // close() = stop()
    ```
