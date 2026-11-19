package codes.grow.packages.loki;

import java.time.Instant;
import java.util.Map;

/**
 * One log entry returned by a query or the tail stream.
 *
 * @param timestampNanos Unix nanoseconds (lossless)
 * @param timestamp the same time as an {@link Instant}
 * @param line the log line
 * @param structuredMetadata structured metadata of the entry (includes Loki's {@code
 *     detected_level})
 * @param parsed labels extracted by parser stages ({@code | json}, {@code | logfmt}, …)
 */
public record StreamEntry(
    long timestampNanos,
    Instant timestamp,
    String line,
    Map<String, String> structuredMetadata,
    Map<String, String> parsed) {
  /**
   * Copies the maps.
   *
   * @param timestampNanos Unix nanoseconds
   * @param timestamp time
   * @param line line
   * @param structuredMetadata metadata
   * @param parsed parsed labels
   */
  public StreamEntry {
    structuredMetadata = Map.copyOf(structuredMetadata);
    parsed = Map.copyOf(parsed);
  }
}
