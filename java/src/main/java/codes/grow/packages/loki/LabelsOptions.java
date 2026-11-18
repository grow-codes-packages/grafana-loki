package codes.grow.packages.loki;

import java.time.Instant;
import java.util.Optional;

/**
 * Options of {@link LokiClient#labels} and {@link LokiClient#labelValues}.
 *
 * @param start window start, or empty
 * @param end window end, or empty
 * @param query stream selector that scopes the result, or empty
 */
public record LabelsOptions(
    Optional<Instant> start, Optional<Instant> end, Optional<String> query) {
  /**
   * Normalizes {@code null} fields to empty.
   *
   * @param start start
   * @param end end
   * @param query selector
   */
  public LabelsOptions {
    start = start == null ? Optional.empty() : start;
    end = end == null ? Optional.empty() : end;
    query = query == null ? Optional.empty() : query;
  }

  /**
   * The server's default window (last 6 hours), all streams.
   *
   * @return default options
   */
  public static LabelsOptions defaults() {
    return new LabelsOptions(null, null, null);
  }

  /**
   * A window.
   *
   * @param start start or {@code null}
   * @param end end or {@code null}
   * @return the options
   */
  public static LabelsOptions of(Instant start, Instant end) {
    return new LabelsOptions(Optional.ofNullable(start), Optional.ofNullable(end), null);
  }

  /**
   * A copy scoped to the streams matching a selector.
   *
   * @param selector stream selector such as {@code {app="x"}}
   * @return the options
   */
  public LabelsOptions withQuery(String selector) {
    return new LabelsOptions(start, end, Optional.ofNullable(selector));
  }
}
