package codes.grow.packages.loki;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Result of {@link LokiClient#query}, {@link LokiClient#queryRange}, {@link LokiClient#volume} and
 * {@link LokiClient#volumeRange}. Only the list matching {@link #resultType()} is filled.
 *
 * @param resultType result type reported by Loki
 * @param streams log streams ({@code streams} results)
 * @param vector samples ({@code vector} results)
 * @param matrix series ({@code matrix} results)
 * @param scalar value ({@code scalar} results)
 * @param stats raw query statistics ({@code data.stats}), empty when absent
 */
public record QueryResult(
    ResultType resultType,
    List<StreamResult> streams,
    List<VectorSample> vector,
    List<MatrixSeries> matrix,
    Optional<Sample> scalar,
    Map<String, Object> stats) {
  /**
   * Copies the fields.
   *
   * @param resultType type
   * @param streams streams
   * @param vector vector
   * @param matrix matrix
   * @param scalar scalar
   * @param stats statistics
   */
  public QueryResult {
    streams = List.copyOf(streams);
    vector = List.copyOf(vector);
    matrix = List.copyOf(matrix);
    scalar = scalar == null ? Optional.empty() : scalar;
    stats = stats == null ? Map.of() : java.util.Collections.unmodifiableMap(stats);
  }
}
