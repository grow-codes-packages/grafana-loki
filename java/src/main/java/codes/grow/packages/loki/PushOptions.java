package codes.grow.packages.loki;

import java.util.Objects;

/**
 * Options of {@link LokiClient#push}.
 *
 * @param format wire format (default {@link PushFormat#JSON})
 * @param gzip gzip a JSON body ({@code Content-Encoding: gzip}); protobuf bodies are always
 *     Snappy-compressed and ignore it
 */
public record PushOptions(PushFormat format, boolean gzip) {
  /**
   * Validates the fields.
   *
   * @param format wire format, {@code null} for JSON
   * @param gzip whether to gzip JSON bodies
   */
  public PushOptions {
    format = format == null ? PushFormat.JSON : format;
  }

  /**
   * Uncompressed JSON.
   *
   * @return default options
   */
  public static PushOptions defaults() {
    return new PushOptions(PushFormat.JSON, false);
  }

  /**
   * Options with a wire format.
   *
   * @param format wire format
   * @return the options
   */
  public static PushOptions of(PushFormat format) {
    return new PushOptions(Objects.requireNonNull(format, "format"), false);
  }

  /**
   * A copy with gzip enabled or disabled.
   *
   * @param enabled whether to gzip JSON bodies
   * @return the options
   */
  public PushOptions withGzip(boolean enabled) {
    return new PushOptions(format, enabled);
  }
}
