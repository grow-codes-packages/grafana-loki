package codes.grow.packages.loki;

import codes.grow.packages.loki.logging.BatchOptions;
import codes.grow.packages.loki.logging.RedactionProcessor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Options of {@link LokiClient#logClient(LokiLogClientOptions)}. */
public final class LokiLogClientOptions {
  private final LogProtocol protocol;
  private final BatchOptions batchOptions;
  private final Map<String, Object> resource;
  private final RedactionProcessor redaction;
  private final List<String> allowedLabels;

  private LokiLogClientOptions(Builder b) {
    this.protocol = b.protocol;
    this.batchOptions = b.batchOptions;
    this.resource = Map.copyOf(b.resource);
    this.redaction = b.redaction;
    this.allowedLabels = b.allowedLabels == null ? null : List.copyOf(b.allowedLabels);
  }

  /**
   * Default options: JSON push, default batching and redaction, no resource.
   *
   * @return the options
   */
  public static LokiLogClientOptions defaults() {
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
   * Returns the protocol.
   *
   * @return the protocol
   */
  public LogProtocol protocol() {
    return protocol;
  }

  /**
   * Returns the batch options.
   *
   * @return the options
   */
  public BatchOptions batchOptions() {
    return batchOptions;
  }

  /**
   * Returns the resource added to every record ({@code service.name}, …).
   *
   * @return immutable map
   */
  public Map<String, Object> resource() {
    return resource;
  }

  /**
   * Returns the redaction processor.
   *
   * @return the processor, or {@code null} when redaction is disabled
   */
  public RedactionProcessor redaction() {
    return redaction;
  }

  /**
   * Returns the cardinality guard allowlist override.
   *
   * @return the list, or empty to use {@link LokiConfig#allowedLabels()}
   */
  public Optional<List<String>> allowedLabels() {
    return Optional.ofNullable(allowedLabels);
  }

  /** Builder of {@link LokiLogClientOptions}. */
  public static final class Builder {
    private LogProtocol protocol = LogProtocol.JSON;
    private BatchOptions batchOptions = BatchOptions.defaults();
    private Map<String, ?> resource = Map.of();
    private RedactionProcessor redaction = RedactionProcessor.defaults();
    private List<String> allowedLabels;

    private Builder() {}

    /**
     * Sets the protocol (default {@link LogProtocol#JSON}).
     *
     * @param protocol protocol
     * @return this builder
     */
    public Builder protocol(LogProtocol protocol) {
      this.protocol = Objects.requireNonNull(protocol, "protocol");
      return this;
    }

    /**
     * Sets the batch options.
     *
     * @param options options
     * @return this builder
     */
    public Builder batchOptions(BatchOptions options) {
      this.batchOptions = Objects.requireNonNull(options, "options");
      return this;
    }

    /**
     * Sets the resource added to every record.
     *
     * @param resource resource attributes
     * @return this builder
     */
    public Builder resource(Map<String, ?> resource) {
      this.resource = Objects.requireNonNull(resource, "resource");
      return this;
    }

    /**
     * Sets the redaction processor ({@code null} disables redaction).
     *
     * @param redaction processor
     * @return this builder
     */
    public Builder redaction(RedactionProcessor redaction) {
      this.redaction = redaction;
      return this;
    }

    /**
     * Overrides the cardinality guard allowlist of the client configuration.
     *
     * @param names label names
     * @return this builder
     */
    public Builder allowedLabels(List<String> names) {
      this.allowedLabels = names;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public LokiLogClientOptions build() {
      return new LokiLogClientOptions(this);
    }
  }
}
