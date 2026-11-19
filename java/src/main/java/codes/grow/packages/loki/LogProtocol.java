package codes.grow.packages.loki;

/** How a {@link LokiClient#logClient(LokiLogClientOptions) log client} ships records. */
public enum LogProtocol {
  /** JSON push to {@code /loki/api/v1/push} (default). */
  JSON,
  /** Snappy-compressed protobuf push to {@code /loki/api/v1/push}. */
  PROTOBUF,
  /** OTLP/JSON to {@code /otlp/v1/logs}; Loki chooses the labels. */
  OTLP
}
