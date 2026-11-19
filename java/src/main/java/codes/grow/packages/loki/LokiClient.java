package codes.grow.packages.loki;

import codes.grow.packages.loki.core.AuthenticationException;
import codes.grow.packages.loki.core.CancelledException;
import codes.grow.packages.loki.core.ConfigException;
import codes.grow.packages.loki.core.EnvReader;
import codes.grow.packages.loki.core.ErrorCode;
import codes.grow.packages.loki.core.HealthStatus;
import codes.grow.packages.loki.core.Hooks;
import codes.grow.packages.loki.core.HttpTransport;
import codes.grow.packages.loki.core.Json;
import codes.grow.packages.loki.core.Logger;
import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.core.Redaction;
import codes.grow.packages.loki.core.RetryPolicy;
import codes.grow.packages.loki.core.Secret;
import codes.grow.packages.loki.core.TimeoutException;
import codes.grow.packages.loki.core.TlsOptions;
import codes.grow.packages.loki.core.TransportRequest;
import codes.grow.packages.loki.core.TransportResponse;
import codes.grow.packages.loki.core.UnavailableException;
import codes.grow.packages.loki.core.UnsupportedException;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.BatchingLogClient;
import codes.grow.packages.loki.logging.LogExporter;
import codes.grow.packages.loki.logging.LogRecord;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.WebSocket;
import java.net.http.WebSocketHandshakeException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * Client for Grafana Loki: push (JSON, protobuf + Snappy, OTLP), LogQL queries, labels, series,
 * index stats and volume, WebSocket tail, status, multi-tenancy and the logging category ({@link
 * #logClient()}, {@link #logQueryClient()}).
 *
 * <p>Implements SPEC section 2.4. Thread-safe; create one per endpoint and close it when done.
 *
 * <pre>{@code
 * try (LokiClient client = LokiClient.fromEnv()) {
 *   client.push(List.of(LokiStream.of(Map.of("service_name", "api"), LokiEntry.of("hello"))));
 *   QueryResult r = client.queryRange("{service_name=\"api\"} |= \"hello\"");
 * }
 * }</pre>
 *
 * <p>Every I/O method has a blocking form and an {@code …Async} form returning {@link
 * CompletableFuture}. Blocking forms raise {@link LokiException} subclasses; async forms fail with
 * them.
 */
public final class LokiClient implements AutoCloseable {
  /** Tenant header. */
  public static final String TENANT_HEADER = "X-Scope-OrgID";

  /** Header that switches query responses to categorized labels. */
  public static final String ENCODING_FLAGS_HEADER = "X-Loki-Response-Encoding-Flags";

  private static final Pattern SHUTTING_DOWN =
      Pattern.compile("ingester is shutting down", Pattern.CASE_INSENSITIVE);
  private static final int STATUS_INGESTION_BLOCKED = 260;

  private final LokiConfig config;
  private final HttpTransport transport;
  private final boolean ownsTransport;
  private final CardinalityGuard guard;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final List<AutoCloseable> resources = new ArrayList<>();
  private final Set<LokiTail> tails = ConcurrentHashMap.newKeySet();

  private LokiClient(LokiConfig config, HttpTransport transport, boolean ownsTransport) {
    this.config = config;
    this.transport = transport;
    this.ownsTransport = ownsTransport;
    this.guard = new CardinalityGuard(config.allowedLabels());
  }

  /**
   * Creates a client.
   *
   * @param config validated configuration
   * @return a new client
   * @throws ConfigException when TLS or proxy settings are invalid
   */
  public static LokiClient create(LokiConfig config) {
    config.validate();
    return new LokiClient(config, new HttpTransport(config.toTransportConfig()), true);
  }

  /**
   * Creates a client from {@code GROW_GRAFANA_LOKI_*} and {@code LOKI_*} environment variables.
   *
   * @return a new client
   */
  public static LokiClient fromEnv() {
    return create(LokiConfig.fromEnv());
  }

  /**
   * Starts a client builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the configuration.
   *
   * @return the configuration
   */
  public LokiConfig config() {
    return config;
  }

  /**
   * Returns the HTTP transport, for service modules built on this client.
   *
   * @return the transport
   */
  public HttpTransport transport() {
    return transport;
  }

  /**
   * Returns the tenant sent as {@code X-Scope-OrgID}.
   *
   * @return the tenant id, or empty
   */
  public java.util.Optional<String> tenantId() {
    return config.tenantId();
  }

  /**
   * Returns the cardinality guard built from {@link LokiConfig#allowedLabels()}.
   *
   * @return the guard
   */
  public CardinalityGuard guard() {
    return guard;
  }

  /**
   * Registers a resource (batch processor, log client) closed with the client.
   *
   * @param resource the resource
   */
  public void register(AutoCloseable resource) {
    synchronized (resources) {
      resources.add(resource);
    }
  }

  /**
   * Returns a client for another tenant: it shares this client's connections and sends {@code
   * X-Scope-OrgID: id} on every request and on the tail handshake. This client is unchanged;
   * closing the child does not close the shared connections.
   *
   * @param id tenant id
   * @return the tenant client
   * @throws ValidationException when the id is invalid
   */
  public LokiClient withTenant(String id) {
    LokiConfig.tenantIdProblem(id, config.multiTenantQueries())
        .ifPresent(
            p -> {
              throw new ValidationException("withTenant: " + p);
            });
    return new LokiClient(config.toBuilder().tenantId(id).build(), transport, false);
  }

  // ------------------------------------------------------------------ plumbing

  /** Converts any throwable into an SDK exception (unwrapping futures). */
  static LokiException sdkError(Throwable t, String operation) {
    return HttpTransport.asSdkException(t, operation);
  }

  /**
   * Maps Loki-specific cases onto the SPEC classes. Every HTTP 500 stays a retryable {@link
   * UnavailableException} (SPEC 2.1); a 500/503 whose text says {@code Ingester is shutting down}
   * (WAL disk at least 90 % full, or the ingester stopping; decision 6) becomes a retryable {@code
   * UnavailableException} with a hint about the cause.
   */
  static LokiException mapError(LokiException e) {
    String message = e.getMessage();
    if (message == null || !SHUTTING_DOWN.matcher(message).find()) {
      return e;
    }
    if (!message.contains("WAL")) {
      message += " (the Loki ingester is stopping or its WAL disk is at least 90 % full)";
    }
    return new UnavailableException(
        message,
        e.httpStatus().isPresent() ? e.httpStatus().getAsInt() : null,
        e.requestId().orElse(null),
        e.retryAfter().orElse(null),
        e.getCause());
  }

  private static <T> CompletableFuture<T> call(Supplier<CompletableFuture<T>> body) {
    try {
      return body.get();
    } catch (LokiException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private TransportRequest.Builder authorized(TransportRequest.Builder b) {
    config.authorizationHeader().ifPresent(a -> b.header("Authorization", a));
    config.tenantId().ifPresent(t -> b.header(TENANT_HEADER, t));
    return b;
  }

  private void checkOpen(String op) {
    if (closed.get()) {
      throw new CancelledException(op + ": client is closed");
    }
  }

  private CompletableFuture<TransportResponse> send(TransportRequest.Builder b, String op) {
    checkOpen(op);
    return transport
        .sendAsync(authorized(b.operation(op)).build())
        .exceptionallyCompose(e -> CompletableFuture.failedFuture(mapError(sdkError(e, op))));
  }

  private CompletableFuture<JsonNode> getJson(
      String path, String op, Map<String, Object> query, boolean categorize) {
    TransportRequest.Builder b = TransportRequest.get(path);
    query.forEach(
        (k, v) -> {
          if (v instanceof List) {
            ((List<?>) v).forEach(item -> b.query(k, item));
          } else {
            b.query(k, v);
          }
        });
    if (categorize) {
      b.header(ENCODING_FLAGS_HEADER, "categorize-labels");
    }
    return send(b, op).thenApply(TransportResponse::jsonNode);
  }

  private static String nanos(Instant t, String field) {
    return t == null ? null : Long.toString(Nanos.of(t, field));
  }

  private static void checkQuery(String logql, String op) {
    if (logql == null || logql.isBlank()) {
      throw new ValidationException(op + ": query must not be empty");
    }
  }

  private static void checkLimit(java.util.OptionalInt limit, String op, int max) {
    if (limit.isPresent() && (limit.getAsInt() < 1 || limit.getAsInt() > max)) {
      throw new ValidationException(op + ": limit must be between 1 and " + max);
    }
  }

  // -------------------------------------------------------------------- status

  /**
   * Readiness: {@code GET /ready} (with the configured auth and tenant headers). {@code 200 ready}
   * → healthy {@code ok}; {@code 503} → unhealthy with Loki's text (e.g. {@code Ingester not ready:
   * waiting for 15s after being ready}); network errors → {@code unreachable}. Never throws except
   * for cancellation and configuration errors.
   *
   * @return the health status
   * @throws CancelledException when interrupted or closed
   */
  public HealthStatus health() {
    return HttpTransport.await(healthAsync(), "health");
  }

  /**
   * Asynchronous {@link #health()}.
   *
   * @return a future with the health status
   */
  public CompletableFuture<HealthStatus> healthAsync() {
    return call(
        () -> {
          checkOpen("health");
          TransportRequest req =
              authorized(
                      TransportRequest.get("/ready")
                          .operation("health")
                          .acceptAnyStatus()
                          .maxRetries(0))
                  .build();
          return transport
              .sendAsync(req)
              .handle(
                  (r, err) -> {
                    if (err == null) {
                      return healthOf(r);
                    }
                    LokiException e = sdkError(err, "health");
                    if (e instanceof CancelledException || e instanceof ConfigException) {
                      throw e;
                    }
                    return HealthStatus.unreachable(String.valueOf(e.getMessage()));
                  });
        });
  }

  private HealthStatus healthOf(TransportResponse r) {
    if (r.status() >= 200 && r.status() < 300) {
      return HealthStatus.ok();
    }
    String status = "unhealthy";
    boolean text =
        r.header("content-type").orElse("").toLowerCase(Locale.ROOT).startsWith("text/plain");
    String body = r.text().replaceAll("\\s+", " ").trim();
    if (text && !body.isEmpty()) {
      status =
          Redaction.truncate(
              Redaction.redactText(body, config.knownSecrets()), Redaction.MAX_VENDOR_TEXT);
    }
    return new HealthStatus(false, status, Map.of("httpStatus", Integer.toString(r.status())));
  }

  /**
   * Returns whether {@code GET /ready} answers 200 (same probe as {@link #health()}).
   *
   * @return true when ready
   */
  public boolean ready() {
    return health().healthy();
  }

  /**
   * Asynchronous {@link #ready()}.
   *
   * @return a future with the readiness
   */
  public CompletableFuture<Boolean> readyAsync() {
    return healthAsync().thenApply(HealthStatus::healthy);
  }

  /**
   * Authenticated round trip: {@code GET /loki/api/v1/labels?since=5m} with auth and tenant
   * headers.
   *
   * @return the latency
   * @throws AuthenticationException without a tenant (multi-tenant Loki) or with wrong gateway
   *     credentials
   */
  public Duration ping() {
    return HttpTransport.await(pingAsync(), "ping");
  }

  /**
   * Asynchronous {@link #ping()}.
   *
   * @return a future with the latency
   */
  public CompletableFuture<Duration> pingAsync() {
    long start = System.nanoTime();
    return call(
        () ->
            send(TransportRequest.get("/loki/api/v1/labels").query("since", "5m"), "ping")
                .thenApply(r -> Duration.ofNanos(System.nanoTime() - start)));
  }

  /**
   * Build information: {@code GET /loki/api/v1/status/buildinfo}.
   *
   * @return the build information
   */
  public BuildInfo buildInfo() {
    return HttpTransport.await(buildInfoAsync(), "buildInfo");
  }

  /**
   * Asynchronous {@link #buildInfo()}.
   *
   * @return a future with the build information
   */
  public CompletableFuture<BuildInfo> buildInfoAsync() {
    return call(
        () ->
            getJson("/loki/api/v1/status/buildinfo", "buildInfo", Map.of(), false)
                .thenApply(Decoders::buildInfo));
  }

  /**
   * Returns the server version ({@code buildInfo().version()}, e.g. {@code 3.7.10}).
   *
   * @return the version string
   * @throws UnsupportedException when the server reports none
   */
  public String serverVersion() {
    return HttpTransport.await(serverVersionAsync(), "serverVersion");
  }

  /**
   * Asynchronous {@link #serverVersion()}.
   *
   * @return a future with the version
   */
  public CompletableFuture<String> serverVersionAsync() {
    return buildInfoAsync()
        .thenApply(
            info -> {
              if (info.version().isEmpty()) {
                throw new UnsupportedException("serverVersion: the server reports no version");
              }
              return info.version();
            });
  }

  // ---------------------------------------------------------------------- push

  /**
   * Pushes streams as JSON ({@code POST /loki/api/v1/push}).
   *
   * @param streams streams (streams without entries are skipped)
   * @throws ValidationException for invalid labels or timestamps (client or server side)
   * @see #push(List, PushOptions)
   */
  public void push(List<LokiStream> streams) {
    push(streams, PushOptions.defaults());
  }

  /**
   * Pushes streams ({@code POST /loki/api/v1/push}). JSON by default (optionally gzip); {@link
   * PushFormat#PROTOBUF} sends a Snappy-compressed {@code PushRequest}. The cardinality guard moves
   * labels outside {@link LokiConfig#allowedLabels()} to structured metadata (unless {@link
   * LokiConfig#disableLabelGuard()}), then labels are validated. Entries without a timestamp get
   * "now". The whole request succeeds or fails. Retrying is safe: Loki drops entries identical to
   * stored ones, so the push is retried on 5xx and network errors. Status 260 (ingestion blocked)
   * raises a non-retryable {@link LokiServerException}.
   *
   * @param streams streams (streams without entries are skipped)
   * @param options wire format and compression
   */
  public void push(List<LokiStream> streams, PushOptions options) {
    HttpTransport.await(pushAsync(streams, options), "push");
  }

  /**
   * Asynchronous {@link #push(List, PushOptions)}.
   *
   * @param streams streams
   * @param options wire format and compression
   * @return a future completing when Loki accepted the push
   */
  public CompletableFuture<Void> pushAsync(List<LokiStream> streams, PushOptions options) {
    return pushAsync(streams, options, !config.disableLabelGuard());
  }

  CompletableFuture<Void> pushAsync(
      List<LokiStream> streams, PushOptions options, boolean guarded) {
    return call(
        () -> {
          List<LokiStream> prepared = prepare(streams, guarded);
          if (prepared.isEmpty()) {
            return CompletableFuture.completedFuture(null);
          }
          TransportRequest.Builder b = TransportRequest.post("/loki/api/v1/push").idempotent(true);
          if (options.format() == PushFormat.PROTOBUF) {
            b.body(Protobuf.pushRequest(prepared), "application/x-protobuf");
          } else {
            byte[] json = Json.encode(pushBody(prepared));
            if (options.gzip()) {
              b.body(gzip(json), "application/json").header("Content-Encoding", "gzip");
            } else {
              b.body(json, "application/json");
            }
          }
          return send(b, "push")
              .thenApply(
                  r -> {
                    if (r.status() == STATUS_INGESTION_BLOCKED) {
                      throw new LokiServerException(
                          "push: ingestion is blocked by the Loki administrator (HTTP 260)",
                          STATUS_INGESTION_BLOCKED,
                          r.requestId().orElse(null),
                          null);
                    }
                    return null;
                  });
        });
  }

  private List<LokiStream> prepare(List<LokiStream> streams, boolean guarded) {
    Instant now = Instant.now();
    List<LokiStream> out = new ArrayList<>();
    for (int i = 0; i < streams.size(); i++) {
      LokiStream s = streams.get(i);
      if (s.entries().isEmpty()) {
        continue;
      }
      if (guarded) {
        s = guard.apply(s);
      }
      try {
        s.labels().forEach(Labels::validateLabel);
      } catch (ValidationException e) {
        throw new ValidationException("push: stream " + i + ": " + e.getMessage());
      }
      List<LokiEntry> entries = new ArrayList<>(s.entries().size());
      for (LokiEntry e : s.entries()) {
        if (e.structuredMetadata().containsKey("")) {
          throw new ValidationException(
              "push: stream " + i + ": structured metadata names must not be empty");
        }
        Instant ts = e.timestamp() == null ? now : e.timestamp();
        Nanos.of(ts, "push: timestamp");
        entries.add(new LokiEntry(ts, e.line(), e.structuredMetadata()));
      }
      out.add(new LokiStream(s.labels(), entries));
    }
    return out;
  }

  private static Map<String, Object> pushBody(List<LokiStream> streams) {
    List<Object> out = new ArrayList<>();
    for (LokiStream s : streams) {
      List<Object> values = new ArrayList<>();
      for (LokiEntry e : s.entries()) {
        String ts = Long.toString(Nanos.of(e.timestamp(), "timestamp"));
        values.add(
            e.structuredMetadata().isEmpty()
                ? List.of(ts, e.line())
                : List.of(ts, e.line(), e.structuredMetadata()));
      }
      Map<String, Object> stream = new LinkedHashMap<>();
      stream.put("stream", s.labels());
      stream.put("values", values);
      out.add(stream);
    }
    return Map.of("streams", out);
  }

  static byte[] gzip(byte[] data) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
      gz.write(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e); // in-memory streams do not fail
    }
    return bos.toByteArray();
  }

  /**
   * Pushes records as OTLP/JSON to {@code POST /otlp/v1/logs} (Loki's OTLP endpoint). Loki promotes
   * its default resource attributes ({@code service.name}, {@code deployment.environment}, …) to
   * labels and stores everything else as structured metadata.
   *
   * @param records records (an empty list sends nothing)
   */
  public void pushOtlp(List<LogRecord> records) {
    pushOtlp(records, false);
  }

  /**
   * Pushes records as OTLP/JSON, optionally gzip-compressed.
   *
   * @param records records
   * @param gzip whether to gzip the body
   */
  public void pushOtlp(List<LogRecord> records, boolean gzip) {
    HttpTransport.await(pushOtlpAsync(records, gzip), "pushOtlp");
  }

  /**
   * Asynchronous {@link #pushOtlp(List, boolean)}.
   *
   * @param records records
   * @param gzip whether to gzip the body
   * @return a future completing when Loki accepted the records
   */
  public CompletableFuture<Void> pushOtlpAsync(List<LogRecord> records, boolean gzip) {
    String op = "pushOtlp";
    return call(
        () -> {
          if (records.isEmpty()) {
            return CompletableFuture.completedFuture(null);
          }
          byte[] json = Json.encode(Otlp.request(records));
          TransportRequest.Builder b = TransportRequest.post("/otlp/v1/logs").idempotent(true);
          if (gzip) {
            b.body(gzip(json), "application/json").header("Content-Encoding", "gzip");
          } else {
            b.body(json, "application/json");
          }
          // Loki answers OTLP errors with a protobuf google.rpc.Status body: accept the final 4xx
          // statuses to decode the message text.
          for (int s = 400; s < 500; s++) {
            if (s != 408 && s != 429) {
              b.acceptStatus(s);
            }
          }
          return send(b, op)
              .thenApply(
                  r -> {
                    if (r.status() >= 400) {
                      throw otlpError(op, r);
                    }
                    return null;
                  });
        });
  }

  private LokiException otlpError(String op, TransportResponse r) {
    byte[] body = r.body();
    String text = Protobuf.rpcStatusMessage(body);
    if (text == null) {
      text = new String(body, StandardCharsets.UTF_8);
    }
    String message =
        HttpTransport.errorMessage(
            op,
            r.status(),
            Map.of("content-type", List.of("text/plain")),
            text.getBytes(StandardCharsets.UTF_8),
            config.knownSecrets());
    return LokiException.of(
        ErrorCode.forStatus(r.status()),
        message,
        r.status(),
        r.requestId().orElse(null),
        null,
        null);
  }

  // --------------------------------------------------------------------- query

  /**
   * Instant metric query at the current time.
   *
   * @param logql metric query
   * @return the result (vector or scalar)
   * @see #query(String, QueryOptions)
   */
  public QueryResult query(String logql) {
    return query(logql, QueryOptions.defaults());
  }

  /**
   * Instant metric query: {@code GET /loki/api/v1/query}. A log selector ({@code {...}} with or
   * without a pipeline) is rejected with {@link ValidationException}: Loki does not run log queries
   * as instant queries; use {@link #queryRange} instead.
   *
   * @param logql metric query
   * @param options evaluation time (limit and direction are kept for API parity)
   * @return the result
   */
  public QueryResult query(String logql, QueryOptions options) {
    return HttpTransport.await(queryAsync(logql, options), "query");
  }

  /**
   * Asynchronous {@link #query(String, QueryOptions)}.
   *
   * @param logql metric query
   * @param options options
   * @return a future with the result
   */
  public CompletableFuture<QueryResult> queryAsync(String logql, QueryOptions options) {
    return call(
        () -> {
          checkQuery(logql, "query");
          if (logql.trim().startsWith("{")) {
            throw new ValidationException(
                "query: log queries cannot run as instant queries; use queryRange or search");
          }
          checkLimit(options.limit(), "query", Integer.MAX_VALUE);
          Map<String, Object> q = new LinkedHashMap<>();
          q.put("query", logql);
          q.put("time", nanos(options.time().orElse(null), "time"));
          q.put("limit", options.limit().isPresent() ? options.limit().getAsInt() : null);
          q.put("direction", options.direction().map(Direction::wireValue).orElse(null));
          return getJson("/loki/api/v1/query", "query", q, true).thenApply(Decoders::queryResult);
        });
  }

  /**
   * Range query over the last hour (newest first, server default limit 100).
   *
   * @param logql log or metric query
   * @return the result
   * @see #queryRange(String, QueryRangeOptions)
   */
  public QueryResult queryRange(String logql) {
    return queryRange(logql, QueryRangeOptions.defaults());
  }

  /**
   * Range query (logs or metrics): {@code GET /loki/api/v1/query_range}. Streams carry indexed
   * labels; structured metadata and parsed labels are per entry (the categorize-labels encoding is
   * requested). {@code end} is exclusive.
   *
   * @param logql log or metric query
   * @param options window, step, limit (1–5000), direction
   * @return the result (streams or matrix)
   */
  public QueryResult queryRange(String logql, QueryRangeOptions options) {
    return HttpTransport.await(queryRangeAsync(logql, options), "queryRange");
  }

  /**
   * Asynchronous {@link #queryRange(String, QueryRangeOptions)}.
   *
   * @param logql query
   * @param options options
   * @return a future with the result
   */
  public CompletableFuture<QueryResult> queryRangeAsync(String logql, QueryRangeOptions options) {
    return call(
        () -> {
          checkQuery(logql, "queryRange");
          checkLimit(options.limit(), "queryRange", LokiLogQueryClient.MAX_QUERY_LIMIT);
          Map<String, Object> q = new LinkedHashMap<>();
          q.put("query", logql);
          q.put("start", nanos(options.start().orElse(null), "start"));
          q.put("end", nanos(options.end().orElse(null), "end"));
          q.put("since", options.since().map(d -> Nanos.duration(d, "since")).orElse(null));
          q.put("step", options.step().map(d -> Nanos.duration(d, "step")).orElse(null));
          q.put(
              "interval", options.interval().map(d -> Nanos.duration(d, "interval")).orElse(null));
          q.put("limit", options.limit().isPresent() ? options.limit().getAsInt() : null);
          q.put("direction", options.direction().map(Direction::wireValue).orElse(null));
          return getJson("/loki/api/v1/query_range", "queryRange", q, true)
              .thenApply(Decoders::queryResult);
        });
  }

  /**
   * Label names of the server's default window.
   *
   * @return label names (empty when nothing matches)
   */
  public List<String> labels() {
    return labels(LabelsOptions.defaults());
  }

  /**
   * Label names: {@code GET /loki/api/v1/labels}. Structured metadata keys are not labels.
   *
   * @param options window and scoping selector
   * @return label names (empty when nothing matches)
   */
  public List<String> labels(LabelsOptions options) {
    return HttpTransport.await(labelsAsync(options), "labels");
  }

  /**
   * Asynchronous {@link #labels(LabelsOptions)}.
   *
   * @param options options
   * @return a future with the label names
   */
  public CompletableFuture<List<String>> labelsAsync(LabelsOptions options) {
    return call(
        () ->
            getJson("/loki/api/v1/labels", "labels", labelsQuery(options), false)
                .thenApply(Decoders::stringList));
  }

  private static Map<String, Object> labelsQuery(LabelsOptions options) {
    Map<String, Object> q = new LinkedHashMap<>();
    q.put("start", nanos(options.start().orElse(null), "start"));
    q.put("end", nanos(options.end().orElse(null), "end"));
    q.put("query", options.query().orElse(null));
    return q;
  }

  /**
   * Values of one label in the server's default window.
   *
   * @param name label name
   * @return values
   */
  public List<String> labelValues(String name) {
    return labelValues(name, LabelsOptions.defaults());
  }

  /**
   * Values of one label: {@code GET /loki/api/v1/label/{name}/values}. The name is validated client
   * side (Loki answers unknown and invalid names with an empty result).
   *
   * @param name label name
   * @param options window and scoping selector
   * @return values (empty when nothing matches)
   */
  public List<String> labelValues(String name, LabelsOptions options) {
    return HttpTransport.await(labelValuesAsync(name, options), "labelValues");
  }

  /**
   * Asynchronous {@link #labelValues(String, LabelsOptions)}.
   *
   * @param name label name
   * @param options options
   * @return a future with the values
   */
  public CompletableFuture<List<String>> labelValuesAsync(String name, LabelsOptions options) {
    return call(
        () -> {
          try {
            Labels.validateLabelName(name);
          } catch (ValidationException e) {
            throw new ValidationException("labelValues: " + e.getMessage());
          }
          return getJson(
                  "/loki/api/v1/label/" + name + "/values",
                  "labelValues",
                  labelsQuery(options),
                  false)
              .thenApply(Decoders::stringList);
        });
  }

  /**
   * Series matching any of the selectors in the server's default window.
   *
   * @param matchers stream selectors (at least one)
   * @return label sets
   */
  public List<Map<String, String>> series(List<String> matchers) {
    return series(matchers, TimeRange.defaults());
  }

  /**
   * Series matching any of the selectors: {@code GET /loki/api/v1/series} (repeated {@code
   * match[]}, OR). At least one non-blank matcher is required.
   *
   * @param matchers stream selectors
   * @param range time window
   * @return label sets
   */
  public List<Map<String, String>> series(List<String> matchers, TimeRange range) {
    return HttpTransport.await(seriesAsync(matchers, range), "series");
  }

  /**
   * Asynchronous {@link #series(List, TimeRange)}.
   *
   * @param matchers stream selectors
   * @param range time window
   * @return a future with the label sets
   */
  public CompletableFuture<List<Map<String, String>>> seriesAsync(
      List<String> matchers, TimeRange range) {
    return call(
        () -> {
          if (matchers.isEmpty() || matchers.stream().anyMatch(m -> m == null || m.isBlank())) {
            throw new ValidationException("series: at least one non-empty matcher is required");
          }
          Map<String, Object> q = new LinkedHashMap<>();
          q.put("match[]", matchers);
          q.put("start", nanos(range.start().orElse(null), "start"));
          q.put("end", nanos(range.end().orElse(null), "end"));
          return getJson("/loki/api/v1/series", "series", q, false).thenApply(Decoders::series);
        });
  }

  /**
   * Index statistics in the server's default window.
   *
   * @param query stream selector
   * @return the statistics
   */
  public IndexStats indexStats(String query) {
    return indexStats(query, TimeRange.defaults());
  }

  /**
   * Approximate index statistics: {@code GET /loki/api/v1/index/stats}. Reads the TSDB index only,
   * so data still in the ingester counts after the next chunk flush.
   *
   * @param query stream selector
   * @param range time window
   * @return the statistics
   */
  public IndexStats indexStats(String query, TimeRange range) {
    return HttpTransport.await(indexStatsAsync(query, range), "indexStats");
  }

  /**
   * Asynchronous {@link #indexStats(String, TimeRange)}.
   *
   * @param query stream selector
   * @param range time window
   * @return a future with the statistics
   */
  public CompletableFuture<IndexStats> indexStatsAsync(String query, TimeRange range) {
    return call(
        () -> {
          checkQuery(query, "indexStats");
          Map<String, Object> q = new LinkedHashMap<>();
          q.put("query", query);
          q.put("start", nanos(range.start().orElse(null), "start"));
          q.put("end", nanos(range.end().orElse(null), "end"));
          return getJson("/loki/api/v1/index/stats", "indexStats", q, false)
              .thenApply(Decoders::indexStats);
        });
  }

  private static Map<String, Object> volumeQuery(
      String op, String query, VolumeOptions options, boolean withStep) {
    checkQuery(query, op);
    checkLimit(options.limit(), op, Integer.MAX_VALUE);
    Map<String, Object> q = new LinkedHashMap<>();
    q.put("query", query);
    q.put("start", nanos(options.start().orElse(null), "start"));
    q.put("end", nanos(options.end().orElse(null), "end"));
    q.put("limit", options.limit().isPresent() ? options.limit().getAsInt() : null);
    q.put(
        "targetLabels",
        options.targetLabels().isEmpty() ? null : String.join(",", options.targetLabels()));
    q.put(
        "aggregateBy",
        options.aggregateBy().map(VolumeOptions.AggregateBy::wireValue).orElse(null));
    if (withStep) {
      q.put("step", options.step().map(d -> Nanos.duration(d, "step")).orElse(null));
    }
    return q;
  }

  /**
   * Ingested bytes per series in the server's default window.
   *
   * @param query stream selector
   * @return a vector result
   */
  public QueryResult volume(String query) {
    return volume(query, VolumeOptions.defaults());
  }

  /**
   * Ingested bytes per series or label: {@code GET /loki/api/v1/index/volume} (vector result).
   * Needs {@code limits_config.volume_enabled: true}; otherwise Loki returns an empty vector ("no
   * data", not an error).
   *
   * @param query stream selector
   * @param options window, limit, target labels, grouping
   * @return a vector result
   */
  public QueryResult volume(String query, VolumeOptions options) {
    return HttpTransport.await(volumeAsync(query, options), "volume");
  }

  /**
   * Asynchronous {@link #volume(String, VolumeOptions)}.
   *
   * @param query stream selector
   * @param options options
   * @return a future with the result
   */
  public CompletableFuture<QueryResult> volumeAsync(String query, VolumeOptions options) {
    return call(
        () ->
            getJson(
                    "/loki/api/v1/index/volume",
                    "volume",
                    volumeQuery("volume", query, options, false),
                    false)
                .thenApply(Decoders::queryResult));
  }

  /**
   * Volume over time in the server's default window.
   *
   * @param query stream selector
   * @return a matrix result
   */
  public QueryResult volumeRange(String query) {
    return volumeRange(query, VolumeOptions.defaults());
  }

  /**
   * Volume over time: {@code GET /loki/api/v1/index/volume_range}. Always a matrix: a {@code
   * vector} answer (every series has one point) is converted to one-sample series.
   *
   * @param query stream selector
   * @param options window, step, limit, target labels, grouping
   * @return a matrix result
   */
  public QueryResult volumeRange(String query, VolumeOptions options) {
    return HttpTransport.await(volumeRangeAsync(query, options), "volumeRange");
  }

  /**
   * Asynchronous {@link #volumeRange(String, VolumeOptions)}.
   *
   * @param query stream selector
   * @param options options
   * @return a future with the result
   */
  public CompletableFuture<QueryResult> volumeRangeAsync(String query, VolumeOptions options) {
    return call(
        () ->
            getJson(
                    "/loki/api/v1/index/volume_range",
                    "volumeRange",
                    volumeQuery("volumeRange", query, options, true),
                    false)
                .thenApply(Decoders::queryResult)
                .thenApply(LokiClient::vectorToMatrix));
  }

  private static QueryResult vectorToMatrix(QueryResult r) {
    if (r.resultType() != ResultType.VECTOR) {
      return r;
    }
    List<MatrixSeries> matrix = new ArrayList<>();
    for (VectorSample v : r.vector()) {
      matrix.add(new MatrixSeries(v.metric(), List.of(new Sample(v.timestamp(), v.value()))));
    }
    return new QueryResult(
        ResultType.MATRIX, List.of(), List.of(), matrix, java.util.Optional.empty(), r.stats());
  }

  /**
   * Normalizes a LogQL query on the server ({@code GET /loki/api/v1/format_query}); useful to check
   * builder output.
   *
   * @param logql query
   * @return the normalized query
   * @throws ValidationException when the query does not parse
   */
  public String formatQuery(String logql) {
    return HttpTransport.await(formatQueryAsync(logql), "formatQuery");
  }

  /**
   * Asynchronous {@link #formatQuery(String)}.
   *
   * @param logql query
   * @return a future with the normalized query
   */
  public CompletableFuture<String> formatQueryAsync(String logql) {
    return call(
        () -> {
          checkQuery(logql, "formatQuery");
          return getJson("/loki/api/v1/format_query", "formatQuery", Map.of("query", logql), false)
              .thenApply(
                  body -> {
                    JsonNode data = body.get("data");
                    if (data == null || !data.isTextual()) {
                      throw Decoders.bad("formatQuery data is not a string");
                    }
                    return data.asText();
                  });
        });
  }

  // ---------------------------------------------------------------------- tail

  /**
   * Live tail with default options (Loki replays the last hour first).
   *
   * @param logql log query (selector + pipeline)
   * @param onFrame receives every frame with entries
   * @return the open stream
   * @see #tail(String, Consumer, LiveTailOptions)
   */
  public LokiTail tail(String logql, Consumer<TailFrame> onFrame) {
    return tail(logql, onFrame, LiveTailOptions.defaults());
  }

  /**
   * Live tail over WebSocket ({@code /loki/api/v1/tail}). Returns once connected; handshake errors
   * raise ({@link AuthenticationException}, {@link ValidationException}, …). Auth, tenant and
   * categorize headers go on the handshake. Only log queries (starting with a selector) can be
   * tailed. Stop with {@link LokiTail#stop()} or {@link #close()}.
   *
   * @param logql log query
   * @param onFrame receives every frame with entries (on the WebSocket thread)
   * @param options replay start, limit, delay, callbacks
   * @return the open stream
   */
  public LokiTail tail(String logql, Consumer<TailFrame> onFrame, LiveTailOptions options) {
    return HttpTransport.await(tailAsync(logql, onFrame, options), "tail");
  }

  /**
   * Asynchronous {@link #tail(String, Consumer, LiveTailOptions)}.
   *
   * @param logql log query
   * @param onFrame frame callback
   * @param options options
   * @return a future with the open stream
   */
  public CompletableFuture<LokiTail> tailAsync(
      String logql, Consumer<TailFrame> onFrame, LiveTailOptions options) {
    String op = "tail";
    return call(
        () -> {
          checkQuery(logql, op);
          if (!logql.trim().startsWith("{")) {
            throw new ValidationException(
                op + ": only log queries (starting with a selector) can be tailed");
          }
          checkLimit(options.limit(), op, Integer.MAX_VALUE);
          Long delay = null;
          if (options.delayFor().isPresent()) {
            Duration d = options.delayFor().get();
            if (d.isNegative() || d.compareTo(Duration.ofSeconds(5)) > 0) {
              throw new ValidationException(op + ": delayFor must be between 0 and 5 seconds");
            }
            delay = Math.round(d.toMillis() / 1000.0);
          }
          checkOpen(op);
          Map<String, List<String>> q = new LinkedHashMap<>();
          q.put("query", List.of(logql));
          q.put("start", List.of(nanos(options.start().orElseGet(Instant::now), "start")));
          options.limit().ifPresent(l -> q.put("limit", List.of(Integer.toString(l))));
          if (delay != null) {
            q.put("delay_for", List.of(Long.toString(delay)));
          }
          String http = transport.resolve("/loki/api/v1/tail", q);
          URI uri =
              URI.create(
                  (http.startsWith("https:") ? "wss" : "ws") + http.substring(http.indexOf(':')));
          return connect(uri, onFrame, options);
        });
  }

  private CompletableFuture<LokiTail> connect(
      URI uri, Consumer<TailFrame> onFrame, LiveTailOptions options) {
    HttpClient.Builder hb = HttpClient.newBuilder().connectTimeout(config.connectTimeout());
    if (config.tls().isCustom()) {
      hb.sslContext(config.tls().sslContext());
    }
    if (config.proxy().isPresent()) {
      URI p = URI.create(config.proxy().get());
      int port = p.getPort() > 0 ? p.getPort() : ("https".equals(p.getScheme()) ? 443 : 80);
      hb.proxy(ProxySelector.of(InetSocketAddress.createUnresolved(p.getHost(), port)));
    } else {
      hb.proxy(HttpClient.Builder.NO_PROXY);
    }
    HttpClient http = hb.build();
    WebSocket.Builder wb = http.newWebSocketBuilder().connectTimeout(config.connectTimeout());
    Map<String, String> headers = new LinkedHashMap<>(config.headers());
    config.authorizationHeader().ifPresent(a -> headers.put("Authorization", a));
    config.tenantId().ifPresent(t -> headers.put(TENANT_HEADER, t));
    headers.put(ENCODING_FLAGS_HEADER, "categorize-labels");
    headers.put("User-Agent", transport.config().userAgent());
    headers.forEach(wb::header);
    LokiTail tail = new LokiTail();
    List<String> secrets = config.knownSecrets();
    LokiTail.Listener listener =
        tail.new Listener(onFrame, options, s -> Redaction.redactText(s, secrets));
    return wb.buildAsync(uri, listener)
        .handle(
            (ws, err) -> {
              if (err != null) {
                // Loki keeps the TCP connection of a rejected upgrade open for 120 s: release
                // this one-off client (and its connection) instead of waiting for GC.
                CompletableFuture.runAsync(() -> closeQuietly(http));
                throw handshakeError(err, secrets);
              }
              tail.attach(ws);
              tails.add(tail);
              tail.closed().whenComplete((v, e) -> tails.remove(tail));
              if (closed.get()) {
                tail.stop();
              }
              return tail;
            });
  }

  /** Closes a one-off WebSocket client on Java 21+ ({@code HttpClient} is AutoCloseable there). */
  static void closeQuietly(HttpClient http) {
    if (http instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception ignored) {
        // best effort: the JDK also closes the connection when the client is collected
      }
    }
  }

  private LokiException handshakeError(Throwable err, List<String> secrets) {
    String op = "tail";
    Throwable c = err;
    while ((c instanceof CompletionException || c instanceof ExecutionException)
        && c.getCause() != null) {
      c = c.getCause();
    }
    for (Throwable t = c; t != null; t = t.getCause()) {
      if (t instanceof WebSocketHandshakeException) {
        var resp = ((WebSocketHandshakeException) t).getResponse();
        Object body = resp.body();
        byte[] bytes =
            body instanceof byte[]
                ? (byte[]) body
                : body instanceof String ? ((String) body).getBytes(StandardCharsets.UTF_8) : null;
        int status = resp.statusCode();
        String message =
            HttpTransport.errorMessage(
                op, status, Map.of("content-type", List.of("text/plain")), bytes, secrets);
        return mapError(
            LokiException.of(ErrorCode.forStatus(status), message, status, null, null, null));
      }
      if (t instanceof HttpConnectTimeoutException) {
        return new TimeoutException(op + ": connect timed out", null, null, null, null);
      }
    }
    return new UnavailableException(
        op + ": connection failed (" + c.getClass().getSimpleName() + ")", null, null, null, null);
  }

  // ------------------------------------------------------------------- logging

  /**
   * A batching log client (JSON push, default batching and redaction).
   *
   * @return the log client, closed with this client
   * @see #logClient(LokiLogClientOptions)
   */
  public BatchingLogClient logClient() {
    return logClient(LokiLogClientOptions.defaults());
  }

  /**
   * A batching {@link codes.grow.packages.loki.logging.LogClient} that writes to this Loki (and
   * tenant). Records go through redaction and, for push protocols, the cardinality guard; {@link
   * LogProtocol#OTLP} uses {@code /otlp/v1/logs} instead. {@link #close()} flushes and closes every
   * log client created here.
   *
   * @param options protocol, batching, resource, redaction, allowlist
   * @return the log client
   */
  public BatchingLogClient logClient(LokiLogClientOptions options) {
    CardinalityGuard g = options.allowedLabels().map(CardinalityGuard::new).orElse(guard);
    LogExporter exporter =
        options.protocol() == LogProtocol.OTLP
            ? new LokiOtlpExporter(this)
            : new LokiLogExporter(
                this,
                options.protocol() == LogProtocol.PROTOBUF ? PushFormat.PROTOBUF : PushFormat.JSON,
                g);
    BatchingLogClient client =
        BatchingLogClient.builder(exporter)
            .batchOptions(options.batchOptions())
            .resource(options.resource())
            .redaction(options.redaction())
            .build();
    register(client);
    return client;
  }

  /**
   * A {@link codes.grow.packages.loki.logging.LogQueryClient} (search, tail, listStreams,
   * listLabels, waitForLog) on this Loki and tenant.
   *
   * @return the query client
   */
  public LokiLogQueryClient logQueryClient() {
    return new LokiLogQueryClient(this);
  }

  // --------------------------------------------------------------------- close

  /**
   * Returns whether {@link #close()} was called.
   *
   * @return true once closed
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Flushes and closes log clients created by {@link #logClient()}, stops open tails and releases
   * connections (children from {@link #withTenant} leave the shared connections open). Idempotent.
   */
  @Override
  public void close() {
    List<AutoCloseable> copy;
    synchronized (resources) {
      if (closed.get()) {
        return;
      }
      copy = new ArrayList<>(resources);
      resources.clear();
    }
    // Log clients flush through this client, so they close before requests are refused.
    for (AutoCloseable r : copy) {
      try {
        r.close();
      } catch (Exception ignored) {
        // best effort: close must not throw
      }
    }
    closed.set(true);
    for (LokiTail t : List.copyOf(tails)) {
      t.stop();
    }
    if (ownsTransport) {
      transport.close();
    }
  }

  @Override
  public String toString() {
    return "LokiClient{" + config + "}";
  }

  /** Builder that configures and creates a client in one chain. */
  public static final class Builder {
    private final LokiConfig.Builder config = LokiConfig.builder();

    private Builder() {}

    /**
     * Fills unset fields from an environment reader.
     *
     * @param env reader, e.g. {@link EnvReader#system()}
     * @return this builder
     */
    public Builder fromEnv(EnvReader env) {
      config.fromEnv(env);
      return this;
    }

    /**
     * Sets the endpoint base URL.
     *
     * @param url URL
     * @return this builder
     */
    public Builder url(String url) {
      config.url(url);
      return this;
    }

    /**
     * Sets the tenant ({@code X-Scope-OrgID}).
     *
     * @param tenantId tenant id
     * @return this builder
     */
    public Builder tenantId(String tenantId) {
      config.tenantId(tenantId);
      return this;
    }

    /**
     * Sets Basic-auth credentials (gateways, Grafana Cloud).
     *
     * @param username user
     * @param password password or access token
     * @return this builder
     */
    public Builder basicAuth(String username, String password) {
      config.username(username).password(password);
      return this;
    }

    /**
     * Sets Basic-auth credentials.
     *
     * @param username user
     * @param password password or access token
     * @return this builder
     */
    public Builder basicAuth(String username, Secret password) {
      config.username(username).password(password);
      return this;
    }

    /**
     * Sets the Bearer token for gateways.
     *
     * @param token token
     * @return this builder
     */
    public Builder bearerToken(String token) {
      config.bearerToken(token);
      return this;
    }

    /**
     * Sets the Bearer token for gateways.
     *
     * @param token token
     * @return this builder
     */
    public Builder bearerToken(Secret token) {
      config.bearerToken(token);
      return this;
    }

    /**
     * Allows tenant ids joined with {@code |} (multi-tenant queries).
     *
     * @param allow whether to allow them
     * @return this builder
     */
    public Builder multiTenantQueries(boolean allow) {
      config.multiTenantQueries(allow);
      return this;
    }

    /**
     * Sets the cardinality guard allowlist.
     *
     * @param names label names
     * @return this builder
     */
    public Builder allowedLabels(List<String> names) {
      config.allowedLabels(names);
      return this;
    }

    /**
     * Disables the cardinality guard for {@link LokiClient#push}.
     *
     * @param disable whether to disable it
     * @return this builder
     */
    public Builder disableLabelGuard(boolean disable) {
      config.disableLabelGuard(disable);
      return this;
    }

    /**
     * Sets the connect timeout.
     *
     * @param d duration
     * @return this builder
     */
    public Builder connectTimeout(Duration d) {
      config.connectTimeout(d);
      return this;
    }

    /**
     * Sets the per-attempt timeout.
     *
     * @param d duration
     * @return this builder
     */
    public Builder requestTimeout(Duration d) {
      config.requestTimeout(d);
      return this;
    }

    /**
     * Sets the total timeout.
     *
     * @param d duration
     * @return this builder
     */
    public Builder totalTimeout(Duration d) {
      config.totalTimeout(d);
      return this;
    }

    /**
     * Sets the retry policy.
     *
     * @param retry policy
     * @return this builder
     */
    public Builder retry(RetryPolicy retry) {
      config.retry(retry);
      return this;
    }

    /**
     * Sets the TLS options.
     *
     * @param tls options
     * @return this builder
     */
    public Builder tls(TlsOptions tls) {
      config.tls(tls);
      return this;
    }

    /**
     * Sets the proxy URL.
     *
     * @param proxy URL
     * @return this builder
     */
    public Builder proxy(String proxy) {
      config.proxy(proxy);
      return this;
    }

    /**
     * Adds a header.
     *
     * @param name name
     * @param value value
     * @return this builder
     */
    public Builder header(String name, String value) {
      config.header(name, value);
      return this;
    }

    /**
     * Sets the user agent suffix.
     *
     * @param suffix suffix
     * @return this builder
     */
    public Builder userAgentSuffix(String suffix) {
      config.userAgentSuffix(suffix);
      return this;
    }

    /**
     * Sets hooks.
     *
     * @param hooks hooks
     * @return this builder
     */
    public Builder hooks(Hooks hooks) {
      config.hooks(hooks);
      return this;
    }

    /**
     * Sets the logger.
     *
     * @param logger logger
     * @return this builder
     */
    public Builder logger(Logger logger) {
      config.logger(logger);
      return this;
    }

    /**
     * Builds the configuration and the client.
     *
     * @return a new client
     * @throws ConfigException when the configuration is invalid
     */
    public LokiClient build() {
      return create(config.build());
    }
  }
}
