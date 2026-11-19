package codes.grow.packages.loki;

import codes.grow.packages.loki.core.ErrorCode;
import codes.grow.packages.loki.core.LokiException;

/**
 * A non-transient server-side failure with code {@link ErrorCode#UNAVAILABLE} and {@code
 * retryable=false}.
 *
 * <p>Raised for push status 260 (ingestion administratively blocked) and for responses the SDK
 * cannot decode. A plain HTTP 500 is a retryable {@code UnavailableException} (SPEC 2.1), and so is
 * a 500/503 whose text says {@code Ingester is shutting down} (WAL disk at least 90 % full).
 */
public final class LokiServerException extends LokiException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates the exception.
   *
   * @param message redacted message
   * @param httpStatus HTTP status or {@code null}
   * @param requestId request id or {@code null}
   * @param cause cause or {@code null}
   */
  public LokiServerException(
      String message, Integer httpStatus, String requestId, Throwable cause) {
    super(ErrorCode.UNAVAILABLE, message, httpStatus, requestId, false, null, cause);
  }
}
