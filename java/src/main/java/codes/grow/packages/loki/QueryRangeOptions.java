package codes.grow.packages.loki;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;

/** Options of {@link LokiClient#queryRange} (log and metric range queries). */
public final class QueryRangeOptions {
  private final Instant start;
  private final Instant end;
  private final Duration since;
  private final Duration step;
  private final Duration interval;
  private final Integer limit;
  private final Direction direction;

  private QueryRangeOptions(Builder b) {
    this.start = b.start;
    this.end = b.end;
    this.since = b.since;
    this.step = b.step;
    this.interval = b.interval;
    this.limit = b.limit;
    this.direction = b.direction;
  }

  /**
   * Default options (last hour, Loki's default limit 100, newest first).
   *
   * @return the options
   */
  public static QueryRangeOptions defaults() {
    return builder().build();
  }

  /**
   * Starts a builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the inclusive start.
   *
   * @return start, or empty (one hour before end)
   */
  public Optional<Instant> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns the exclusive end.
   *
   * @return end, or empty (now)
   */
  public Optional<Instant> end() {
    return Optional.ofNullable(end);
  }

  /**
   * Returns the relative start used when no start is set.
   *
   * @return since, or empty
   */
  public Optional<Duration> since() {
    return Optional.ofNullable(since);
  }

  /**
   * Returns the metric query resolution.
   *
   * @return step, or empty
   */
  public Optional<Duration> step() {
    return Optional.ofNullable(step);
  }

  /**
   * Returns the stream sampling interval (log queries).
   *
   * @return interval, or empty
   */
  public Optional<Duration> interval() {
    return Optional.ofNullable(interval);
  }

  /**
   * Returns the maximum number of entries across all streams.
   *
   * @return limit, or empty (server default 100)
   */
  public OptionalInt limit() {
    return limit == null ? OptionalInt.empty() : OptionalInt.of(limit);
  }

  /**
   * Returns the direction.
   *
   * @return direction, or empty (backward)
   */
  public Optional<Direction> direction() {
    return Optional.ofNullable(direction);
  }

  /** Builder of {@link QueryRangeOptions}. */
  public static final class Builder {
    private Instant start;
    private Instant end;
    private Duration since;
    private Duration step;
    private Duration interval;
    private Integer limit;
    private Direction direction;

    private Builder() {}

    /**
     * Sets the inclusive start.
     *
     * @param start start
     * @return this builder
     */
    public Builder start(Instant start) {
      this.start = start;
      return this;
    }

    /**
     * Sets the exclusive end.
     *
     * @param end end
     * @return this builder
     */
    public Builder end(Instant end) {
      this.end = end;
      return this;
    }

    /**
     * Sets the relative start ({@code end - since}) used when no start is set.
     *
     * @param since positive duration
     * @return this builder
     */
    public Builder since(Duration since) {
      this.since = since;
      return this;
    }

    /**
     * Sets the metric query resolution.
     *
     * @param step positive duration
     * @return this builder
     */
    public Builder step(Duration step) {
      this.step = step;
      return this;
    }

    /**
     * Sets the stream sampling interval.
     *
     * @param interval positive duration
     * @return this builder
     */
    public Builder interval(Duration interval) {
      this.interval = interval;
      return this;
    }

    /**
     * Sets the maximum number of entries (1–5000).
     *
     * @param limit limit
     * @return this builder
     */
    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets the direction.
     *
     * @param direction direction
     * @return this builder
     */
    public Builder direction(Direction direction) {
      this.direction = direction;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public QueryRangeOptions build() {
      return new QueryRangeOptions(this);
    }
  }
}
