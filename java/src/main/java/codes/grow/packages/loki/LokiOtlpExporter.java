package codes.grow.packages.loki;

import codes.grow.packages.loki.logging.ExportOptions;
import codes.grow.packages.loki.logging.ExportResult;
import codes.grow.packages.loki.logging.LogExporter;
import codes.grow.packages.loki.logging.LogRecord;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * {@link LogExporter} that writes OTLP/JSON to {@code POST /otlp/v1/logs}. Loki then decides which
 * resource attributes become labels (its OTLP defaults: {@code service.name}, {@code
 * deployment.environment}, …) and stores the rest as structured metadata.
 */
public final class LokiOtlpExporter implements LogExporter {
  private final LokiClient client;

  /**
   * Creates an exporter.
   *
   * @param client client (and tenant) to push with
   */
  public LokiOtlpExporter(LokiClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  @Override
  public CompletableFuture<ExportResult> export(List<LogRecord> records, ExportOptions options) {
    if (records.isEmpty()) {
      return CompletableFuture.completedFuture(ExportResult.success(0));
    }
    return LokiLogExporter.toResult(client.pushOtlpAsync(records, options.gzip()), records.size());
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return CompletableFuture.completedFuture(null);
  }
}
