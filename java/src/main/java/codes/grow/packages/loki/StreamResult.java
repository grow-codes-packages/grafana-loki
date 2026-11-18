package codes.grow.packages.loki;

import java.util.List;
import java.util.Map;

/**
 * One stream of a {@code streams} result.
 *
 * @param labels index labels of the stream (all labels when the server did not categorize them)
 * @param entries entries in the requested direction
 */
public record StreamResult(Map<String, String> labels, List<StreamEntry> entries) {
  /**
   * Copies the fields.
   *
   * @param labels labels
   * @param entries entries
   */
  public StreamResult {
    labels = Map.copyOf(labels);
    entries = List.copyOf(entries);
  }
}
