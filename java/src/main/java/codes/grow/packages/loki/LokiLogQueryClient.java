package codes.grow.packages.loki;

import codes.grow.packages.loki.core.HttpTransport;
import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.LogQueryClient;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.LogSearchResult;
import codes.grow.packages.loki.logging.SearchOptions;
import codes.grow.packages.loki.logging.Severity;
import codes.grow.packages.loki.logging.SortOrder;
import codes.grow.packages.loki.logging.TailHandle;
import codes.grow.packages.loki.logging.TailOptions;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * {@link LogQueryClient} of the logging category on top of the Loki query API: {@code search}
 * ({@code query_range}, merge-sorted across streams, cursor pagination), {@code tail} (WebSocket),
 * {@code listStreams} ({@code series}), {@code listLabels} ({@code labels}) and {@code waitForLog}.
 *
 * <p>Records are normalized: the line becomes {@code body}; structured metadata and parsed labels
 * become {@code attributes} (the SDK's own keys map back to {@code severity}, {@code traceId},
 * {@code spanId}, {@code traceFlags} and {@code observedTimestamp}); stream labels become {@code
 * resource} ({@code service_name} → {@code service.name}, {@code deployment_environment} → {@code
 * deployment.environment}, …).
 */
public final class LokiLogQueryClient implements LogQueryClient {
  /**
   * Largest {@code limit} Loki accepts per query by default ({@code max_entries_limit_per_query}).
   */
  public static final int MAX_QUERY_LIMIT = 5000;

  /** Selector used for free-text searches and {@link #listStreams()}. */
  public static final String ALL_STREAMS_SELECTOR = "{service_name=~\".+\"}";

  private static final Map<String, String> RESOURCE_KEYS =
      Map.of(
          "service_name", "service.name",
          "service_namespace", "service.namespace",
          "service_version", "service.version",
          "service_instance_id", "service.instance.id",
          "deployment_environment", "deployment.environment",
          "host_name", "host.name");

  private static final Set<String> CONSUMED_METADATA =
      Set.of(
          "severity_text",
          "severity_number",
          "trace_id",
          "span_id",
          "trace_flags",
          "observed_timestamp");

  private static final Pattern DIGITS = Pattern.compile("\\d+");
  private static final Pattern TRACE_ID = Pattern.compile("[0-9a-f]{32}");
  private static final Pattern SPAN_ID = Pattern.compile("[0-9a-f]{16}");
  private static final Pattern CURSOR = Pattern.compile("(\\d{1,19}):(\\d{1,9})");

  private final LokiClient client;

  /**
   * Creates a query client.
   *
   * @param client client (and tenant) to query
   */
  public LokiLogQueryClient(LokiClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  /**
   * Turns a search text into LogQL: text starting with <code>&#123;</code> is LogQL as is; anything
   * else is a case-sensitive substring search over every stream ({@code {service_name=~".+"} |=
   * "<text>"}).
   *
   * @param query search text or LogQL
   * @return LogQL
   */
  public static String toLogQL(String query) {
    String trimmed = query.trim();
    if (trimmed.startsWith("{")) {
      return trimmed;
    }
    if (trimmed.isEmpty()) {
      return ALL_STREAMS_SELECTOR;
    }
    return ALL_STREAMS_SELECTOR + " |= " + LogQL.quote(trimmed);
  }

  /**
   * Encodes a search cursor ({@code <ns>:<entries at ns already returned>}, base64url).
   *
   * @param ns Unix nanoseconds of the last returned entry
   * @param skip entries with that timestamp already returned
   * @return the opaque cursor
   */
  static String encodeCursor(long ns, int skip) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString((ns + ":" + skip).getBytes(StandardCharsets.UTF_8));
  }

  /** Decodes a cursor: {ns, skip}. */
  static long[] decodeCursor(String cursor) {
    String text;
    try {
      text = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      throw new ValidationException("logs.search: invalid cursor");
    }
    var m = CURSOR.matcher(text);
    if (!m.matches()) {
      throw new ValidationException("logs.search: invalid cursor");
    }
    try {
      return new long[] {Long.parseLong(m.group(1)), Long.parseLong(m.group(2))};
    } catch (NumberFormatException e) {
      throw new ValidationException("logs.search: invalid cursor");
    }
  }

  /** One entry of the merged, sorted result. */
  record Merged(Map<String, String> labels, String labelKey, StreamEntry entry) {}

  /** Merge-sorts the entries of all streams by time (then labels, then line for stable ties). */
  static List<Merged> merge(List<StreamResult> streams, SortOrder order) {
    List<Merged> flat = new ArrayList<>();
    for (StreamResult s : streams) {
      String key = Labels.formatLabelSet(s.labels());
      for (StreamEntry e : s.entries()) {
        flat.add(new Merged(s.labels(), key, e));
      }
    }
    Comparator<Merged> byTime = Comparator.comparingLong(m -> m.entry().timestampNanos());
    if (order == SortOrder.DESC) {
      byTime = byTime.reversed();
    }
    flat.sort(byTime.thenComparing(Merged::labelKey).thenComparing(m -> m.entry().line()));
    return flat;
  }

  private static Severity severity(Map<String, String> md, Map<String, String> labels) {
    String number = md.get("severity_number");
    if (number != null && DIGITS.matcher(number).matches() && number.length() < 10) {
      return Severity.fromNumber(Integer.parseInt(number));
    }
    for (String text :
        new String[] {md.get("severity_text"), md.get("detected_level"), labels.get("level")}) {
      Optional<Severity> s = Severity.parse(text);
      if (s.isPresent()) {
        return s.get();
      }
    }
    return Severity.INFO;
  }

  /**
   * Converts one Loki entry to a normalized {@link LogRecord}.
   *
   * @param labels stream labels
   * @param entry entry
   * @return the record
   */
  public static LogRecord toLogRecord(Map<String, String> labels, StreamEntry entry) {
    Map<String, String> md = entry.structuredMetadata();
    Map<String, Object> attributes = new LinkedHashMap<>();
    md.forEach(
        (k, v) -> {
          if (!CONSUMED_METADATA.contains(k)) {
            attributes.put(k, v);
          }
        });
    attributes.putAll(entry.parsed());
    Map<String, Object> resource = new LinkedHashMap<>();
    labels.forEach((k, v) -> resource.put(RESOURCE_KEYS.getOrDefault(k, k), v));
    Severity sev = severity(md, labels);
    LogRecord.Builder b =
        LogRecord.builder()
            .timestamp(entry.timestamp())
            .severity(sev)
            .severityText(md.getOrDefault("severity_text", sev.text()))
            .body(entry.line())
            .resource(resource);
    String observed = md.get("observed_timestamp");
    if (observed != null && DIGITS.matcher(observed).matches() && observed.length() < 20) {
      b.observedTimestamp(Nanos.toInstant(Long.parseLong(observed)));
    }
    String traceId = md.get("trace_id");
    if (traceId != null && TRACE_ID.matcher(traceId.toLowerCase(Locale.ROOT)).matches()) {
      b.traceId(traceId.toLowerCase(Locale.ROOT));
      String spanId = md.get("span_id");
      if (spanId != null && SPAN_ID.matcher(spanId.toLowerCase(Locale.ROOT)).matches()) {
        b.spanId(spanId.toLowerCase(Locale.ROOT));
      }
      String flags = md.containsKey("trace_flags") ? md.get("trace_flags") : md.get("flags");
      if (flags != null && DIGITS.matcher(flags).matches() && flags.length() <= 3) {
        int f = Integer.parseInt(flags);
        if (f <= 255) {
          b.traceFlags(f);
          if (!md.containsKey("trace_flags")) {
            attributes.remove("flags");
          }
        }
      }
    }
    return b.attributes(attributes).build();
  }

  /**
   * Searches with {@code query_range}. {@code query} is LogQL (starting with <code>&#123;</code>)
   * or free text. Order {@code DESC} (default) is newest first. Pass {@code nextCursor} back as
   * {@code cursor} for the next page; the cursor is {@code <timestamp>:<entries at that timestamp
   * already returned>}, so pages never skip or repeat entries sharing a nanosecond. {@code total}
   * is not available from Loki.
   *
   * @param query LogQL or free text
   * @param options window, limit (1–5000), order, cursor
   * @return one page of records
   */
  @Override
  public LogSearchResult search(String query, SearchOptions options) {
    return HttpTransport.await(searchAsync(query, options), "logs.search");
  }

  @Override
  public CompletableFuture<LogSearchResult> searchAsync(String query, SearchOptions options) {
    try {
      return doSearch(query, options);
    } catch (LokiException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<LogSearchResult> doSearch(String query, SearchOptions options) {
    int limit = options.limit();
    if (limit > MAX_QUERY_LIMIT) {
      throw new ValidationException("logs.search: limit must be 1.." + MAX_QUERY_LIMIT);
    }
    SortOrder order = options.order();
    Instant start = options.start().orElse(null);
    Instant end = options.end().orElse(null);
    long[] cursor = options.cursor().map(LokiLogQueryClient::decodeCursor).orElse(null);
    if (cursor != null) {
      if (order == SortOrder.DESC) {
        end = Nanos.toInstant(cursor[0] + 1);
      } else {
        start = Nanos.toInstant(cursor[0]);
      }
    }
    int fetch = (int) Math.min(limit + (cursor == null ? 0 : cursor[1]), MAX_QUERY_LIMIT);
    String logql = toLogQL(query);
    Direction direction = order == SortOrder.ASC ? Direction.FORWARD : Direction.BACKWARD;
    QueryRangeOptions.Builder first =
        QueryRangeOptions.builder().start(start).end(end).limit(fetch).direction(direction);
    return client
        .queryRangeAsync(logql, first.build())
        .thenCompose(
            result -> {
              List<Merged> merged = merge(result.streams(), order);
              boolean truncated = merged.size() >= fetch;
              if (!truncated || merged.isEmpty()) {
                return CompletableFuture.completedFuture(page(merged, false, limit, cursor));
              }
              // Loki cut the result inside the boundary nanosecond in its own order: fetch that
              // nanosecond completely so the page and the cursor's skip count are deterministic.
              long boundary = merged.get(merged.size() - 1).entry().timestampNanos();
              QueryRangeOptions exact =
                  QueryRangeOptions.builder()
                      .start(Nanos.toInstant(boundary))
                      .end(Nanos.toInstant(boundary + 1))
                      .limit(MAX_QUERY_LIMIT)
                      .direction(direction)
                      .build();
              return client
                  .queryRangeAsync(logql, exact)
                  .thenApply(
                      ex -> {
                        List<StreamResult> all = new ArrayList<>();
                        for (StreamResult s : result.streams()) {
                          List<StreamEntry> keep = new ArrayList<>();
                          for (StreamEntry e : s.entries()) {
                            if (e.timestampNanos() != boundary) {
                              keep.add(e);
                            }
                          }
                          all.add(new StreamResult(s.labels(), keep));
                        }
                        all.addAll(ex.streams());
                        return page(merge(all, order), true, limit, cursor);
                      });
            });
  }

  private static LogSearchResult page(
      List<Merged> merged, boolean truncated, int limit, long[] cursor) {
    int offset = 0;
    if (cursor != null) {
      while (offset < cursor[1]
          && offset < merged.size()
          && merged.get(offset).entry().timestampNanos() == cursor[0]) {
        offset++;
      }
    }
    List<Merged> remaining = merged.subList(offset, merged.size());
    List<Merged> page = remaining.subList(0, Math.min(limit, remaining.size()));
    List<LogRecord> records = new ArrayList<>(page.size());
    for (Merged m : page) {
      records.add(toLogRecord(m.labels(), m.entry()));
    }
    Optional<String> next = Optional.empty();
    if ((truncated || remaining.size() > limit) && !page.isEmpty()) {
      long lastNs = page.get(page.size() - 1).entry().timestampNanos();
      long atLast = page.stream().filter(m -> m.entry().timestampNanos() == lastNs).count();
      if (cursor != null && cursor[0] == lastNs) {
        atLast += cursor[1];
      }
      next = Optional.of(encodeCursor(lastNs, (int) atLast));
    }
    return new LogSearchResult(records, next, OptionalLong.empty());
  }

  /**
   * Live tail over the WebSocket API. {@code options.start()} defaults to now, so only new records
   * arrive. Connection errors and abnormal closes go to {@code options.onError()}.
   *
   * @param query LogQL selector or free text
   * @param onRecord receives every record (oldest first within a frame)
   * @param options start and error callback ({@code interval} is not used: Loki streams)
   * @return a handle; {@code stop()} closes the socket
   */
  @Override
  public TailHandle tail(String query, Consumer<LogRecord> onRecord, TailOptions options) {
    AtomicBoolean stopped = new AtomicBoolean();
    AtomicReference<LokiTail> tail = new AtomicReference<>();
    Consumer<Throwable> report =
        e -> {
          if (!stopped.get()) {
            options.onError().accept(e);
          }
        };
    LiveTailOptions live =
        LiveTailOptions.builder()
            .start(options.start().orElseGet(Instant::now))
            .onError(report::accept)
            .build();
    client
        .tailAsync(
            toLogQL(query),
            frame -> {
              for (Merged m : merge(frame.streams(), SortOrder.ASC)) {
                if (stopped.get()) {
                  return;
                }
                onRecord.accept(toLogRecord(m.labels(), m.entry()));
              }
            },
            live)
        .whenComplete(
            (t, err) -> {
              if (err != null) {
                report.accept(LokiClient.sdkError(err, "logs.tail"));
                return;
              }
              tail.set(t);
              if (stopped.get()) {
                t.stop();
              }
            });
    return () -> {
      stopped.set(true);
      LokiTail t = tail.get();
      if (t != null) {
        t.stop();
      }
    };
  }

  /**
   * Every stream of the default window as a label-set string ({@code {a="b", c="d"}}), sorted.
   *
   * @return stream label sets
   */
  @Override
  public List<String> listStreams() {
    return HttpTransport.await(listStreamsAsync(), "logs.listStreams");
  }

  @Override
  public CompletableFuture<List<String>> listStreamsAsync() {
    return client
        .seriesAsync(List.of(ALL_STREAMS_SELECTOR), TimeRange.defaults())
        .thenApply(
            series -> {
              TreeSet<String> out = new TreeSet<>();
              for (Map<String, String> s : series) {
                out.add(Labels.formatLabelSet(s));
              }
              return List.copyOf(out);
            });
  }

  /**
   * Label names of the default window.
   *
   * @return label names
   */
  @Override
  public List<String> listLabels() {
    return HttpTransport.await(listLabelsAsync(), "logs.listLabels");
  }

  @Override
  public CompletableFuture<List<String>> listLabelsAsync() {
    return client.labelsAsync(LabelsOptions.defaults());
  }
}
