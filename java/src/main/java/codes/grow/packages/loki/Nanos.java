package codes.grow.packages.loki;

import codes.grow.packages.loki.core.ValidationException;
import java.time.Duration;
import java.time.Instant;

/** Time helpers for the Loki HTTP API (Unix nanoseconds). */
final class Nanos {
  private static final long NS = 1_000_000_000L;

  private Nanos() {}

  /** Converts an instant to Unix nanoseconds; rejects times before 1970 or after 2262. */
  static long of(Instant t, String field) {
    if (t.getEpochSecond() < 0 || t.getEpochSecond() >= Long.MAX_VALUE / NS) {
      throw new ValidationException(field + ": time is outside the range Loki accepts");
    }
    return t.getEpochSecond() * NS + t.getNano();
  }

  /** Converts Unix nanoseconds to an instant. */
  static Instant toInstant(long ns) {
    return Instant.ofEpochSecond(Math.floorDiv(ns, NS), Math.floorMod(ns, NS));
  }

  /** Converts float Unix seconds (metric samples) to an instant (microsecond precision). */
  static Instant fromSeconds(double seconds) {
    long micros = Math.round(seconds * 1_000_000d);
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
  }

  /** Renders a positive duration as a Loki duration in milliseconds ({@code "1500ms"}). */
  static String duration(Duration d, String field) {
    if (d.isZero() || d.isNegative()) {
      throw new ValidationException(field + ": must be a positive duration");
    }
    long ms = d.toMillis();
    return (ms == 0 ? 1 : ms) + "ms";
  }
}
