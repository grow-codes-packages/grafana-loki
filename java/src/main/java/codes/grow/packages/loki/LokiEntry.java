package codes.grow.packages.loki;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * One log line of a {@link LokiStream}.
 *
 * @param timestamp entry time (nanosecond precision is kept); {@code null} means "now" at push time
 * @param line the log line
 * @param structuredMetadata per-entry metadata (string → string, stored but not indexed)
 */
public record LokiEntry(Instant timestamp, String line, Map<String, String> structuredMetadata) {
  /**
   * Validates and copies the fields.
   *
   * @param timestamp entry time or {@code null}
   * @param line the log line
   * @param structuredMetadata metadata or {@code null}
   */
  public LokiEntry {
    Objects.requireNonNull(line, "line");
    structuredMetadata = structuredMetadata == null ? Map.of() : Map.copyOf(structuredMetadata);
  }

  /**
   * An entry stamped "now" at push time.
   *
   * @param line the log line
   * @return the entry
   */
  public static LokiEntry of(String line) {
    return new LokiEntry(null, line, null);
  }

  /**
   * An entry with a timestamp.
   *
   * @param timestamp entry time
   * @param line the log line
   * @return the entry
   */
  public static LokiEntry of(Instant timestamp, String line) {
    return new LokiEntry(timestamp, line, null);
  }

  /**
   * An entry with a timestamp and structured metadata.
   *
   * @param timestamp entry time
   * @param line the log line
   * @param structuredMetadata metadata
   * @return the entry
   */
  public static LokiEntry of(
      Instant timestamp, String line, Map<String, String> structuredMetadata) {
    return new LokiEntry(timestamp, line, structuredMetadata);
  }
}
