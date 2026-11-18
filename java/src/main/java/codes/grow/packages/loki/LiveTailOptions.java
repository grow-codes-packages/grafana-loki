package codes.grow.packages.loki;

import codes.grow.packages.loki.core.LokiException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

/** Options of {@link LokiClient#tail}. */
public final class LiveTailOptions {
  private final Instant start;
  private final Integer limit;
  private final Duration delayFor;
  private final Consumer<List<DroppedEntry>> onDropped;
  private final Consumer<LokiException> onError;

  private LiveTailOptions(Builder b) {
    this.start = b.start;
    this.limit = b.limit;
    this.delayFor = b.delayFor;
    this.onDropped = b.onDropped;
    this.onError = b.onError;
  }

  /**
   * Default options: Loki replays the last hour (up to 100 entries), then streams new entries.
   *
   * @return the options
   */
  public static LiveTailOptions defaults() {
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
   * Returns the replay start.
   *
   * @return start, or empty (Loki replays the last hour)
   */
  public Optional<Instant> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns the maximum number of replayed entries.
   *
   * @return limit, or empty (Loki default 100)
   */
  public OptionalInt limit() {
    return limit == null ? OptionalInt.empty() : OptionalInt.of(limit);
  }

  /**
   * Returns the server-side delay.
   *
   * @return delay (0–5 s, sent as whole seconds), or empty
   */
  public Optional<Duration> delayFor() {
    return Optional.ofNullable(delayFor);
  }

  /**
   * Returns the callback for entries the server dropped because the client fell behind.
   *
   * @return the callback (never null)
   */
  public Consumer<List<DroppedEntry>> onDropped() {
    return onDropped;
  }

  /**
   * Returns the callback for errors after the connection was established (bad frame, abnormal
   * close).
   *
   * @return the callback (never null)
   */
  public Consumer<LokiException> onError() {
    return onError;
  }

  /** Builder of {@link LiveTailOptions}. */
  public static final class Builder {
    private Instant start;
    private Integer limit;
    private Duration delayFor;
    private Consumer<List<DroppedEntry>> onDropped = d -> {};
    private Consumer<LokiException> onError = e -> {};

    private Builder() {}

    /**
     * Replays entries from this time ({@code Instant.now()} = live only).
     *
     * @param start start
     * @return this builder
     */
    public Builder start(Instant start) {
      this.start = start;
      return this;
    }

    /**
     * Sets the maximum number of replayed entries.
     *
     * @param limit limit (≥ 1)
     * @return this builder
     */
    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets the server-side delay (0–5 s).
     *
     * @param delayFor delay
     * @return this builder
     */
    public Builder delayFor(Duration delayFor) {
      this.delayFor = delayFor;
      return this;
    }

    /**
     * Sets the dropped-entries callback.
     *
     * @param onDropped callback
     * @return this builder
     */
    public Builder onDropped(Consumer<List<DroppedEntry>> onDropped) {
      this.onDropped = onDropped == null ? d -> {} : onDropped;
      return this;
    }

    /**
     * Sets the error callback.
     *
     * @param onError callback
     * @return this builder
     */
    public Builder onError(Consumer<LokiException> onError) {
      this.onError = onError == null ? e -> {} : onError;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public LiveTailOptions build() {
      return new LiveTailOptions(this);
    }
  }
}
