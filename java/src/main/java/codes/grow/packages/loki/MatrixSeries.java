package codes.grow.packages.loki;

import java.util.List;
import java.util.Map;

/**
 * One series of a {@code matrix} result.
 *
 * @param metric series labels
 * @param samples samples in time order
 */
public record MatrixSeries(Map<String, String> metric, List<Sample> samples) {
  /**
   * Copies the fields.
   *
   * @param metric labels
   * @param samples samples
   */
  public MatrixSeries {
    metric = Map.copyOf(metric);
    samples = List.copyOf(samples);
  }
}
