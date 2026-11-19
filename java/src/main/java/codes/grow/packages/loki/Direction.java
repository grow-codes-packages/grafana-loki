package codes.grow.packages.loki;

import java.util.Locale;

/** Sort order of log query results. */
public enum Direction {
  /** Oldest first. */
  FORWARD,
  /** Newest first (Loki's default). */
  BACKWARD;

  /**
   * Returns the query parameter value ({@code forward} / {@code backward}).
   *
   * @return the wire value
   */
  public String wireValue() {
    return name().toLowerCase(Locale.ROOT);
  }
}
