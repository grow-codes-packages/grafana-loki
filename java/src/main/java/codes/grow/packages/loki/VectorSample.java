package codes.grow.packages.loki;

import java.time.Instant;
import java.util.Map;

/**
 * One series of a {@code vector} result.
 *
 * @param metric series labels
 * @param timestamp sample time
 * @param value sample value
 */
public record VectorSample(Map<String, String> metric, Instant timestamp, double value) {
  /**
   * Copies the labels.
   *
   * @param metric labels
   * @param timestamp time
   * @param value value
   */
  public VectorSample {
    metric = Map.copyOf(metric);
  }
}
