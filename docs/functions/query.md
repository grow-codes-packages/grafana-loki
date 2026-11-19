# Query

All query functions send `X-Loki-Response-Encoding-Flags: categorize-labels`, so stream results
keep **index labels** (`labels`), **structured metadata** (`structuredMetadata`) and **parsed
labels** (`parsed`, from `| json`, `| logfmt`, …) apart. Times accept the language's time type
(and nanosecond values where the language has them); nanosecond precision is kept.

| Function | Loki API | Notes |
| - | - | - |
| `query(logql, {time, limit, direction})` | `GET /loki/api/v1/query` | instant **metric** queries (`vector` / `scalar`). A plain log selector is rejected client side with `VALIDATION` (Loki answers 400): use `queryRange` or `search` |
| `queryRange(logql, {start, end, since, step, interval, limit, direction})` | `GET /loki/api/v1/query_range` | `streams` for log queries, `matrix` for metric queries. `start` inclusive, `end` exclusive (default: the last hour), `limit` 1–5000 entries across all streams (default 100), `direction` `backward` (newest first, default) or `forward` |
| `labels({start, end, since, query})` | `GET /loki/api/v1/labels` | label names; a missing `data` field decodes as `[]` |
| `labelValues(name, {start, end, since, query})` | `GET /loki/api/v1/label/{name}/values` | the name is validated client side (Loki returns 200 + nothing for invalid names) |
| `series(matchers, {start, end, since})` | `GET /loki/api/v1/series` | at least one non-empty matcher is required (OR-ed); returns label sets |
| `indexStats(query, {start, end})` | `GET /loki/api/v1/index/stats` | `{streams, chunks, bytes, entries}` from the TSDB index only: zero until the ingester flushed its chunks; `bytes` is counted in KB per chunk |
| `volume(query, {start, end, limit, targetLabels, aggregateBy})` | `GET /loki/api/v1/index/volume` | a `vector` of bytes per series (`aggregateBy` `series`, default) or per label name (`labels`) |
| `volumeRange(query, {start, end, step, …})` | `GET /loki/api/v1/index/volume_range` | **always a `matrix`**: when Loki answers with a vector (every series has one point) the SDK converts it to a one-point-per-series matrix |
| `formatQuery(logql)` | `GET /loki/api/v1/format_query` | Loki's normalized LogQL text; `VALIDATION` with the parse error |

`volume` and `volumeRange` need `limits_config.volume_enabled: true` on the server; with the
image default config Loki answers with an empty result, which the SDK reports as "no data".

=== "TypeScript"

    ```ts
    const logs = await client.queryRange('{service_name="checkout"} |= "payment"', {
      sinceMs: 15 * 60_000,
      limit: 50,
      direction: "forward",
    });
    for (const s of logs.streams)
      for (const e of s.entries) console.log(e.timestampNs, e.line, e.structuredMetadata);

    const count = await client.query('sum(count_over_time({service_name="checkout"}[5m]))');
    console.log(count.vector[0]?.value);

    console.log(await client.labels(), await client.labelValues("level"));
    console.log(await client.series(['{service_name="checkout"}']));
    console.log(await client.indexStats('{service_name="checkout"}'));
    const vol = await client.volume('{service_name="checkout"}', {
      targetLabels: ["level"],
      aggregateBy: "labels",
    });
    const overTime = await client.volumeRange('{service_name="checkout"}', { stepMs: 60_000 });
    console.log(vol.vector, overTime.matrix);
    console.log(await client.formatQuery('{app="x"}|="err"|json|status>=500'));
    ```

=== "Rust"

    ```rust
    use std::time::Duration;
    use grow_grafana_loki::*;

    let logs = client
        .query_range(
            r#"{service_name="checkout"} |= "payment""#,
            QueryRangeOptions {
                since: Some(Duration::from_secs(900)),
                limit: 50,
                direction: Some(Direction::Forward),
                ..Default::default()
            },
        )
        .await?;
    for s in &logs.streams {
        for e in &s.entries {
            println!("{:?} {} {:?}", e.timestamp, e.line, e.structured_metadata);
        }
    }
    let count = client
        .query(r#"sum(count_over_time({service_name="checkout"}[5m]))"#, QueryOptions::default())
        .await?;
    println!("{:?}", count.vector.first().map(|s| s.value.value));

    let sel = r#"{service_name="checkout"}"#;
    println!("{:?}", client.labels(LabelsOptions::default()).await?);
    println!("{:?}", client.label_values("level", LabelsOptions::default()).await?);
    println!("{:?}", client.series(&[sel], SeriesOptions::default()).await?);
    println!("{:?}", client.index_stats(sel, IndexOptions::default()).await?);
    let vol = client
        .volume(sel, VolumeOptions {
            target_labels: vec!["level".into()],
            aggregate_by: Some(VolumeAggregation::Labels),
            ..Default::default()
        })
        .await?;
    let over_time = client
        .volume_range(sel, VolumeOptions { step: Some(Duration::from_secs(60)), ..Default::default() })
        .await?;
    println!("{:?} {:?}", vol.vector, over_time.matrix);
    println!("{}", client.format_query(r#"{app="x"}|="err"|json|status>=500"#).await?);
    ```

=== "Go"

    ```go
    logs, err := client.QueryRange(ctx, `{service_name="checkout"} |= "payment"`, loki.QueryRangeOptions{
        Since: 15 * time.Minute, Limit: 50, Direction: loki.DirectionForward,
    })
    for _, s := range logs.Streams {
        for _, e := range s.Entries {
            fmt.Println(e.Timestamp, e.Line, e.StructuredMetadata)
        }
    }
    count, err := client.Query(ctx, `sum(count_over_time({service_name="checkout"}[5m]))`, loki.QueryOptions{})

    sel := `{service_name="checkout"}`
    labels, err := client.Labels(ctx, loki.LabelsOptions{})
    levels, err := client.LabelValues(ctx, "level", loki.LabelsOptions{})
    series, err := client.Series(ctx, []string{sel}, loki.SeriesOptions{})
    stats, err := client.IndexStats(ctx, sel, loki.IndexOptions{})
    vol, err := client.Volume(ctx, sel, loki.VolumeOptions{TargetLabels: []string{"level"}, AggregateBy: "labels"})
    overTime, err := client.VolumeRange(ctx, sel, loki.VolumeOptions{Step: time.Minute}) // overTime.Matrix
    pretty, err := client.FormatQuery(ctx, `{app="x"}|="err"|json|status>=500`)
    ```

=== "Python"

    ```python
    logs = client.query_range(
        '{service_name="checkout"} |= "payment"', since_s=900, limit=50, direction="forward"
    )
    for stream in logs.streams:
        for entry in stream.entries:
            print(entry.timestamp_ns, entry.line, entry.structured_metadata)

    count = client.query('sum(count_over_time({service_name="checkout"}[5m]))')
    print(count.vector[0].value if count.vector else None)

    sel = '{service_name="checkout"}'
    print(client.labels(), client.label_values("level"), client.series([sel]))
    print(client.index_stats(sel))
    vol = client.volume(sel, target_labels=["level"], aggregate_by="labels")
    over_time = client.volume_range(sel, step_s=60)
    print(vol.vector, over_time.matrix)
    print(client.format_query('{app="x"}|="err"|json|status>=500'))
    ```

=== "Java"

    ```java
    QueryResult logs = client.queryRange(
        "{service_name=\"checkout\"} |= \"payment\"",
        QueryRangeOptions.builder()
            .since(Duration.ofMinutes(15)).limit(50).direction(Direction.FORWARD).build());
    for (StreamResult s : logs.streams()) {
        for (StreamEntry e : s.entries()) {
            System.out.println(e.timestamp() + " " + e.line() + " " + e.structuredMetadata());
        }
    }
    QueryResult count = client.query("sum(count_over_time({service_name=\"checkout\"}[5m]))");

    String sel = "{service_name=\"checkout\"}";
    List<String> labels = client.labels();
    List<String> levels = client.labelValues("level");
    List<Map<String, String>> series = client.series(List.of(sel));
    IndexStats stats = client.indexStats(sel);
    QueryResult vol = client.volume(sel, VolumeOptions.builder()
        .targetLabels(List.of("level")).aggregateBy(VolumeOptions.AggregateBy.LABELS).build());
    QueryResult overTime = client.volumeRange(sel,
        VolumeOptions.builder().step(Duration.ofMinutes(1)).build()); // overTime.matrix()
    String pretty = client.formatQuery("{app=\"x\"}|=\"err\"|json|status>=500");
    ```

    Java `labels`/`labelValues` take `LabelsOptions` (`LabelsOptions.last(Duration)` for a relative
    window), `series`/`indexStats` take a `TimeRange` (`TimeRange.last(Duration)`).

## Result model

| | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| type | `resultType` | `result_type` | `ResultType` | `result_type` | `resultType()` |
| log streams | `streams[].labels / entries[]` | `streams[].labels / entries[]` | `Streams[].Labels / Entries[]` | `streams[].labels / entries[]` | `streams().labels() / entries()` |
| entry | `timestampNs`, `timestamp`, `line`, `structuredMetadata`, `parsed` | `timestamp`, `line`, `structured_metadata`, `parsed` | `Timestamp`, `Line`, `StructuredMetadata`, `Parsed` | `timestamp_ns`, `timestamp`, `line`, `structured_metadata`, `parsed` | `timestampNanos()`, `timestamp()`, `line()`, `structuredMetadata()`, `parsed()` |
| vector | `vector[].metric / value` | `vector[].metric / value` | `Vector[].Metric / Value` | `vector[].metric / value` | `vector()` |
| matrix | `matrix[].metric / samples[]` | `matrix[].metric / values[]` | `Matrix[].Metric / Values[]` | `matrix[].metric / values[]` | `matrix().metric() / samples()` |
| raw stats | `stats` | `stats` | `Stats` | `stats` | `stats()` |
