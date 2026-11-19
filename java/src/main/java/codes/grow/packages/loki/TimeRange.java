package codes.grow.packages.loki;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * A time window for {@link LokiClient#series}, {@link LokiClient#indexStats} and the label calls.
 *
 * @param start inclusive start, or empty (server default: one hour before end)
 * @param end end, or empty (now)
 */
public record TimeRange(Optional<Instant> start, Optional<Instant> end) {
  /**
   * Normalizes {@code null} fields to empty.
   *
   * @param start start
   * @param end end
   */
  public TimeRange {
    start = start == null ? Optional.empty() : start;
    end = end == null ? Optional.empty() : end;
  }

  /**
   * The server's default window.
   *
   * @return an empty range
   */
  public static TimeRange defaults() {
    return new TimeRange(Optional.empty(), Optional.empty());
  }

  /**
   * A window from {@code start} to {@code end}.
   *
   * @param start start or {@code null}
   * @param end end or {@code null}
   * @return the range
   */
  public static TimeRange of(Instant start, Instant end) {
    return new TimeRange(Optional.ofNullable(start), Optional.ofNullable(end));
  }

  /**
   * The last {@code window} up to now.
   *
   * @param window positive duration
   * @return the range
   */
  public static TimeRange last(Duration window) {
    Instant now = Instant.now();
    return of(now.minus(window), now);
  }
}
