package codes.grow.packages.loki;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Options of {@link LokiClient#query} (instant metric queries).
 *
 * <p>{@code limit} and {@code direction} are kept for API parity: Loki runs only metric queries as
 * instant queries, where they have no effect.
 */
public final class QueryOptions {
  private final Instant time;
  private final Integer limit;
  private final Direction direction;

  private QueryOptions(Builder b) {
    this.time = b.time;
    this.limit = b.limit;
    this.direction = b.direction;
  }

  /**
   * Default options (evaluation time = now).
   *
   * @return the options
   */
  public static QueryOptions defaults() {
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
   * Returns the evaluation time.
   *
   * @return the time, or empty for now
   */
  public Optional<Instant> time() {
    return Optional.ofNullable(time);
  }

  /**
   * Returns the entry limit.
   *
   * @return the limit, or empty
   */
  public OptionalInt limit() {
    return limit == null ? OptionalInt.empty() : OptionalInt.of(limit);
  }

  /**
   * Returns the direction.
   *
   * @return the direction, or empty
   */
  public Optional<Direction> direction() {
    return Optional.ofNullable(direction);
  }

  /** Builder of {@link QueryOptions}. */
  public static final class Builder {
    private Instant time;
    private Integer limit;
    private Direction direction;

    private Builder() {}

    /**
     * Sets the evaluation time.
     *
     * @param time time
     * @return this builder
     */
    public Builder time(Instant time) {
      this.time = time;
      return this;
    }

    /**
     * Sets the entry limit (≥ 1).
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
    public QueryOptions build() {
      return new QueryOptions(this);
    }
  }
}
