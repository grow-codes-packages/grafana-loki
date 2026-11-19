package codes.grow.packages.loki;

import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.logging.ExportOptions;
import codes.grow.packages.loki.logging.ExportResult;
import codes.grow.packages.loki.logging.LogExporter;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.Severity;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * {@link LogExporter} of the logging category that writes records with {@code POST
 * /loki/api/v1/push}.
 *
 * <p>Mapping ({@link #recordsToStreams}): resource and attribute keys are normalized ({@code
 * service.name} → {@code service_name}); allowlisted keys become index labels, plus {@code level}
 * (lower-case severity) when allowed; everything else, and {@code trace_id}, {@code span_id},
 * {@code trace_flags}, {@code severity_text}, {@code severity_number} and {@code
 * observed_timestamp}, becomes structured metadata. The body is the log line. Loki accepts or
 * rejects a whole push, so a result is either all accepted or all rejected (with the error).
 */
public final class LokiLogExporter implements LogExporter {
  /** Structured metadata key of the trace id. */
  public static final String METADATA_TRACE_ID = "trace_id";

  /** Structured metadata key of the span id. */
  public static final String METADATA_SPAN_ID = "span_id";

  /** Structured metadata key of the trace flags. */
  public static final String METADATA_TRACE_FLAGS = "trace_flags";

  /** Structured metadata key of the severity text. */
  public static final String METADATA_SEVERITY_TEXT = "severity_text";

  /** Structured metadata key of the OTel severity number. */
  public static final String METADATA_SEVERITY_NUMBER = "severity_number";

  /** Structured metadata key of the observed timestamp (Unix nanoseconds). */
  public static final String METADATA_OBSERVED_TIMESTAMP = "observed_timestamp";

  private final LokiClient client;
  private final PushFormat format;
  private final CardinalityGuard guard;

  /**
   * Creates an exporter.
   *
   * @param client client (and tenant) to push with
   * @param format wire format
   * @param guard cardinality guard
   */
  public LokiLogExporter(LokiClient client, PushFormat format, CardinalityGuard guard) {
    this.client = Objects.requireNonNull(client, "client");
    this.format = Objects.requireNonNull(format, "format");
    this.guard = Objects.requireNonNull(guard, "guard");
  }

  /**
   * Converts records into Loki streams; records with the same final label set share a stream.
   *
   * @param records records
   * @param guard cardinality guard
   * @return streams, in first-seen order of their label sets
   */
  public static List<LokiStream> recordsToStreams(List<LogRecord> records, CardinalityGuard guard) {
    Map<String, Map<String, String>> labelsByKey = new LinkedHashMap<>();
    Map<String, List<LokiEntry>> entriesByKey = new LinkedHashMap<>();
    for (LogRecord r : records) {
      Map<String, String> labels = new LinkedHashMap<>();
      Map<String, String> md = new LinkedHashMap<>();
      guard.split(r.resource(), labels, md);
      guard.split(r.attributes(), labels, md);
      Severity sev = r.severity();
      if (guard.isAllowed("level") && !labels.containsKey("level")) {
        labels.put("level", sev.text().toLowerCase(Locale.ROOT));
      }
      md.put(METADATA_SEVERITY_TEXT, r.severityText());
      md.put(METADATA_SEVERITY_NUMBER, Integer.toString(sev.number()));
      r.traceId().ifPresent(t -> md.put(METADATA_TRACE_ID, t.toLowerCase(Locale.ROOT)));
      r.spanId().ifPresent(s -> md.put(METADATA_SPAN_ID, s.toLowerCase(Locale.ROOT)));
      r.traceFlags().ifPresent(f -> md.put(METADATA_TRACE_FLAGS, Integer.toString(f)));
      r.observedTimestamp()
          .ifPresent(
              t ->
                  md.put(
                      METADATA_OBSERVED_TIMESTAMP,
                      Long.toString(Nanos.of(t, "observedTimestamp"))));
      md.values().removeIf(String::isEmpty); // Loki drops empty metadata values anyway
      String key = Labels.formatLabelSet(labels);
      labelsByKey.putIfAbsent(key, labels);
      entriesByKey
          .computeIfAbsent(key, k -> new ArrayList<>())
          .add(new LokiEntry(r.timestamp(), r.body(), md));
    }
    List<LokiStream> out = new ArrayList<>();
    labelsByKey.forEach((k, labels) -> out.add(new LokiStream(labels, entriesByKey.get(k))));
    return out;
  }

  @Override
  public CompletableFuture<ExportResult> export(List<LogRecord> records, ExportOptions options) {
    if (records.isEmpty()) {
      return CompletableFuture.completedFuture(ExportResult.success(0));
    }
    List<LokiStream> streams;
    try {
      streams = recordsToStreams(records, guard);
    } catch (LokiException e) {
      return CompletableFuture.completedFuture(ExportResult.failure(0, records.size(), e));
    }
    return toResult(
        client.pushAsync(streams, new PushOptions(format, options.gzip()), false), records.size());
  }

  /** Maps a push outcome to an export result (SDK errors reject the whole batch). */
  static CompletableFuture<ExportResult> toResult(CompletableFuture<Void> push, int count) {
    return push.handle(
        (v, err) -> {
          if (err == null) {
            return ExportResult.success(count);
          }
          return ExportResult.failure(0, count, LokiClient.sdkError(err, "logs.export"));
        });
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return CompletableFuture.completedFuture(null);
  }
}
