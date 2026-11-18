package codes.grow.packages.loki;

import java.util.Map;

/**
 * An entry the tail stream dropped because the client fell behind.
 *
 * @param labels labels of the dropped entry
 * @param timestampNanos Unix nanoseconds of the dropped entry
 */
public record DroppedEntry(Map<String, String> labels, long timestampNanos) {
  /**
   * Copies the labels.
   *
   * @param labels labels
   * @param timestampNanos time
   */
  public DroppedEntry {
    labels = Map.copyOf(labels);
  }
}
