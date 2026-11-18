package codes.grow.packages.loki;

/** Wire format of {@link LokiClient#push}. */
public enum PushFormat {
  /** {@code application/json} (default; optionally gzip). */
  JSON,
  /**
   * Snappy-compressed {@code logproto.PushRequest} ({@code application/x-protobuf}), encoded from
   * the field numbers of Loki's Apache-2.0 {@code pkg/push/push.proto}.
   */
  PROTOBUF
}
