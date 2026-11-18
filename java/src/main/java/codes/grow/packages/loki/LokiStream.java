package codes.grow.packages.loki;

import java.util.List;
import java.util.Map;

/**
 * A Loki log stream ({@code LokiStream}): one label set and its entries, the input of {@link
 * LokiClient#push}.
 *
 * @param labels index labels; keep them few and low-cardinality (names {@code
 *     [a-zA-Z_][a-zA-Z0-9_]*}, values non-empty)
 * @param entries entries of the stream (any order inside Loki's ingest window)
 */
public record LokiStream(Map<String, String> labels, List<LokiEntry> entries) {
  /**
   * Copies the fields.
   *
   * @param labels labels or {@code null}
   * @param entries entries or {@code null}
   */
  public LokiStream {
    labels = labels == null ? Map.of() : Map.copyOf(labels);
    entries = entries == null ? List.of() : List.copyOf(entries);
  }

  /**
   * Creates a stream.
   *
   * @param labels index labels
   * @param entries entries
   * @return the stream
   */
  public static LokiStream of(Map<String, String> labels, LokiEntry... entries) {
    return new LokiStream(labels, List.of(entries));
  }
}
