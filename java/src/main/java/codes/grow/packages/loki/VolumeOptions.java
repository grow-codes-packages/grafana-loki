package codes.grow.packages.loki;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;

/** Options of {@link LokiClient#volume} and {@link LokiClient#volumeRange}. */
public final class VolumeOptions {
  /** How volumes are grouped. */
  public enum AggregateBy {
    /** By series of the matched labels (Loki's default). */
    SERIES,
    /** By label name. */
    LABELS;

    /**
     * Returns the query parameter value.
     *
     * @return {@code series} or {@code labels}
     */
    public String wireValue() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private final Instant start;
  private final Instant end;
  private final Integer limit;
  private final List<String> targetLabels;
  private final AggregateBy aggregateBy;
  private final Duration step;

  private VolumeOptions(Builder b) {
    this.start = b.start;
    this.end = b.end;
    this.limit = b.limit;
    this.targetLabels = List.copyOf(b.targetLabels);
    this.aggregateBy = b.aggregateBy;
    this.step = b.step;
  }

  /**
   * Default options.
   *
   * @return the options
   */
  public static VolumeOptions defaults() {
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
   * Returns the window start.
   *
   * @return start, or empty
   */
  public Optional<Instant> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns the window end.
   *
   * @return end, or empty
   */
  public Optional<Instant> end() {
    return Optional.ofNullable(end);
  }

  /**
   * Returns the maximum number of series.
   *
   * @return limit, or empty
   */
  public OptionalInt limit() {
    return limit == null ? OptionalInt.empty() : OptionalInt.of(limit);
  }

  /**
   * Returns the labels to aggregate on.
   *
   * @return immutable list (empty = all matched labels)
   */
  public List<String> targetLabels() {
    return targetLabels;
  }

  /**
   * Returns the grouping.
   *
   * @return grouping, or empty (series)
   */
  public Optional<AggregateBy> aggregateBy() {
    return Optional.ofNullable(aggregateBy);
  }

  /**
   * Returns the resolution of {@link LokiClient#volumeRange}.
   *
   * @return step, or empty
   */
  public Optional<Duration> step() {
    return Optional.ofNullable(step);
  }

  /** Builder of {@link VolumeOptions}. */
  public static final class Builder {
    private Instant start;
    private Instant end;
    private Integer limit;
    private List<String> targetLabels = List.of();
    private AggregateBy aggregateBy;
    private Duration step;

    private Builder() {}

    /**
     * Sets the window start.
     *
     * @param start start
     * @return this builder
     */
    public Builder start(Instant start) {
      this.start = start;
      return this;
    }

    /**
     * Sets the window end.
     *
     * @param end end
     * @return this builder
     */
    public Builder end(Instant end) {
      this.end = end;
      return this;
    }

    /**
     * Sets the maximum number of series.
     *
     * @param limit limit (≥ 1)
     * @return this builder
     */
    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets the labels to aggregate on.
     *
     * @param labels label names
     * @return this builder
     */
    public Builder targetLabels(List<String> labels) {
      this.targetLabels = labels == null ? List.of() : List.copyOf(labels);
      return this;
    }

    /**
     * Sets the grouping.
     *
     * @param aggregateBy grouping
     * @return this builder
     */
    public Builder aggregateBy(AggregateBy aggregateBy) {
      this.aggregateBy = aggregateBy;
      return this;
    }

    /**
     * Sets the resolution of {@link LokiClient#volumeRange}.
     *
     * @param step positive duration
     * @return this builder
     */
    public Builder step(Duration step) {
      this.step = step;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public VolumeOptions build() {
      return new VolumeOptions(this);
    }
  }
}
