package codes.grow.packages.loki;

import codes.grow.packages.loki.core.ErrorCode;
import codes.grow.packages.loki.core.LokiException;

/**
 * A non-transient server-side failure with code {@link ErrorCode#UNAVAILABLE} and {@code
 * retryable=false}.
 *
 * <p>Raised for Loki's HTTP 500 answers that are not transient (for example multi-tenant queries on
 * a server where they are disabled) and for push status 260 (ingestion administratively blocked). A
 * 500/503 whose text says {@code Ingester is shutting down} (WAL disk ≥ 90 % full) is a retryable
 * {@code UnavailableException} instead.
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
