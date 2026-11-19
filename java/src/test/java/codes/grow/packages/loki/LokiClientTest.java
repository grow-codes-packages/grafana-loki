package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.FakeLoki.Reply;
import codes.grow.packages.loki.core.AuthenticationException;
import codes.grow.packages.loki.core.CancelledException;
import codes.grow.packages.loki.core.EnvReader;
import codes.grow.packages.loki.core.ErrorCode;
import codes.grow.packages.loki.core.HealthStatus;
import codes.grow.packages.loki.core.Hooks;
import codes.grow.packages.loki.core.Json;
import codes.grow.packages.loki.core.Logger;
import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.core.RateLimitException;
import codes.grow.packages.loki.core.RetryPolicy;
import codes.grow.packages.loki.core.Secret;
import codes.grow.packages.loki.core.TlsOptions;
import codes.grow.packages.loki.core.UnavailableException;
import codes.grow.packages.loki.core.UnsupportedException;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.Severity;
import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.compress.snappy.SnappyDecompressor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LokiClientTest {
  static final Instant T0 = Instant.ofEpochSecond(1_795_034_767L, 64_713_072L);

  private FakeLoki loki;
  private LokiClient client;

  @BeforeEach
  void start() {
    loki = new FakeLoki();
    client = client(b -> {});
  }

  @AfterEach
  void stop() {
    client.close();
    loki.close();
  }

  private LokiClient client(java.util.function.Consumer<LokiClient.Builder> customize) {
    LokiClient.Builder b =
        LokiClient.builder()
            .url(loki.url())
            .retry(
                RetryPolicy.builder()
                    .initialBackoff(Duration.ofMillis(1))
                    .maxBackoff(Duration.ofMillis(2))
                    .build());
    customize.accept(b);
    return b.build();
  }

  static byte[] gunzip(byte[] b) throws IOException {
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(b))) {
      return in.readAllBytes();
    }
  }

  static byte[] unsnappy(byte[] b) {
    SnappyDecompressor d = new SnappyDecompressor();
    byte[] out = new byte[SnappyDecompressor.getUncompressedLength(b, 0)];
    d.decompress(b, 0, b.length, out, 0, out.length);
    return out;
  }

  // ------------------------------------------------------------------ status

  @Test
  void healthVariants() {
    loki.enqueue("ready.ok")
        .enqueue(Reply.text(503, "Ingester not ready: waiting for 15s after being ready\n"))
        .enqueue(Reply.json(401, "{}"))
        .enqueue(Reply.text(500, " "));
    assertThat(client.health()).isEqualTo(HealthStatus.ok());
    HealthStatus warming = client.health();
    assertThat(warming.healthy()).isFalse();
    assertThat(warming.status()).isEqualTo("Ingester not ready: waiting for 15s after being ready");
    assertThat(warming.details()).containsEntry("httpStatus", "503");
    assertThat(client.health().status()).isEqualTo("unhealthy");
    assertThat(client.health().status()).isEqualTo("unhealthy");
    assertThat(loki.requests()).hasSize(4); // never retried
    assertThat(loki.requests().get(0).path()).isEqualTo("/ready");
    loki.enqueue("ready.ok");
    assertThat(client.ready()).isTrue();
    loki.enqueue(Reply.text(503, "x"));
    assertThat(client.readyAsync().join()).isFalse();
  }

  @Test
  void healthUnreachableAndClosed() throws Exception {
    int port;
    try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
      port = s.getLocalPort();
    }
    try (LokiClient c =
        LokiClient.builder()
            .url("http://127.0.0.1:" + port)
            .connectTimeout(Duration.ofMillis(500))
            .build()) {
      HealthStatus h = c.health();
      assertThat(h.status()).isEqualTo("unreachable");
      assertThat(h.healthy()).isFalse();
    }
    LokiClient closed = client(b -> {});
    closed.close();
    assertThatThrownBy(closed::health).isInstanceOf(CancelledException.class);
    assertThatThrownBy(closed::ping).isInstanceOf(CancelledException.class);
    assertThat(closed.isClosed()).isTrue();
    closed.close(); // idempotent
    // a transport closed under a running call reports cancellation through health()
    LokiClient c2 = client(b -> {});
    c2.transport().close();
    assertThatThrownBy(c2::health).isInstanceOf(CancelledException.class);
  }

  @Test
  void pingSendsAuthAndTenant() {
    LokiClient c = client(b -> b.tenantId("team-a").basicAuth("user", "pw-123456"));
    loki.enqueue("ping.ok-tenant");
    assertThat(c.ping()).isPositive();
    FakeLoki.Request r = loki.last();
    assertThat(r.path()).isEqualTo("/loki/api/v1/labels");
    assertThat(r.param("since")).isEqualTo("5m");
    assertThat(r.header("X-Scope-OrgID")).isEqualTo("team-a");
    assertThat(r.header("Authorization")).startsWith("Basic ");
    assertThat(r.header("User-Agent")).startsWith("grow-codes-grafana-loki/").endsWith("(java)");
    loki.enqueue("ping.no-tenant");
    assertThatThrownBy(c::ping)
        .isInstanceOf(AuthenticationException.class)
        .hasMessageContaining("no org id")
        .hasMessageContaining("HTTP 401");
    loki.enqueue("ping.gateway-bad-credentials");
    assertThatThrownBy(() -> c.pingAsync().get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(AuthenticationException.class);
    c.close();
  }

  @Test
  void buildInfoAndVersion() {
    loki.enqueue("buildInfo.ok").enqueue("buildInfo.ok");
    BuildInfo info = client.buildInfo();
    assertThat(info.version()).isEqualTo("3.7.10");
    assertThat(info.revision()).isNotEmpty();
    assertThat(info.branch()).isEqualTo("release-3.7.x");
    assertThat(info.goVersion()).isEmpty();
    assertThat(client.serverVersion()).isEqualTo("3.7.10");
    loki.enqueue(Reply.json(200, "{}"));
    assertThatThrownBy(client::serverVersion).isInstanceOf(UnsupportedException.class);
    loki.enqueue(Reply.text(200, "not json"));
    assertThatThrownBy(client::buildInfo).isInstanceOf(ValidationException.class);
  }

  @Test
  void withTenantSharesTransport() {
    LokiClient a = client.withTenant("tenant-a");
    assertThat(a.tenantId()).contains("tenant-a");
    assertThat(client.tenantId()).isEmpty();
    assertThat(a.transport()).isSameAs(client.transport());
    assertThat(a.config().url()).isEqualTo(client.config().url());
    assertThatThrownBy(() -> client.withTenant("a|b"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("withTenant");
    assertThatThrownBy(() -> client.withTenant("bad/x")).isInstanceOf(ValidationException.class);
    LokiClient mt = client(b -> b.multiTenantQueries(true));
    assertThat(mt.withTenant("a|b").tenantId()).contains("a|b");
    mt.close();
    loki.enqueue("labels.ok");
    assertThat(a.labels()).contains("service_name");
    assertThat(loki.last().header("X-Scope-OrgID")).isEqualTo("tenant-a");
    a.close(); // the child does not close the shared transport
    assertThat(client.transport().isClosed()).isFalse();
    loki.enqueue("labels.ok");
    assertThat(client.labels()).isNotEmpty();
    assertThat(loki.last().header("X-Scope-OrgID")).isNull();
    assertThat(client.toString()).startsWith("LokiClient{LokiConfig{");
    assertThat(client.guard().isAllowed("service_name")).isTrue();
  }

  @Test
  void factoriesAndBuilder() {
    LokiClient c =
        LokiClient.builder()
            .fromEnv(EnvReader.of(Map.of(EnvReader.DEFAULT_PREFIX + "URL", loki.url())))
            .bearerToken(Secret.of("tok-123456"))
            .allowedLabels(List.of("app"))
            .disableLabelGuard(false)
            .connectTimeout(Duration.ofSeconds(1))
            .requestTimeout(Duration.ofSeconds(1))
            .totalTimeout(Duration.ofSeconds(5))
            .tls(TlsOptions.defaults())
            .header("X-Extra", "1")
            .userAgentSuffix("suite")
            .hooks(Hooks.NONE)
            .logger(Logger.NOOP)
            .build();
    assertThat(c.config().url()).isEqualTo(loki.url());
    loki.enqueue("labels.ok");
    c.labels();
    assertThat(loki.last().header("Authorization")).isEqualTo("Bearer tok-123456");
    assertThat(loki.last().header("X-Extra")).isEqualTo("1");
    c.close();
    LokiClient basic =
        LokiClient.builder().url(loki.url()).basicAuth("u", Secret.of("p")).proxy(null).build();
    assertThat(basic.config().username()).contains("u");
    basic.close();
    LokiClient created = LokiClient.create(LokiConfig.builder().url(loki.url()).build());
    AutoCloseable res =
        () -> {
          throw new IllegalStateException("boom");
        };
    created.register(res);
    created.close(); // a failing resource does not break close
    assertThat(created.isClosed()).isTrue();
    // fromEnv() reads the real environment; without GROW_GRAFANA_LOKI_URL it targets the default
    try (LokiClient env = LokiClient.fromEnv()) {
      assertThat(env.config().url()).isNotEmpty();
    }
    assertThat(LokiConfig.fromEnv().url()).isNotEmpty();
  }

  // -------------------------------------------------------------------- push

  @Test
  void pushJson() {
    loki.enqueue("push.json");
    client.push(
        List.of(
            LokiStream.of(
                Map.of("service_name", "api", "level", "info"),
                LokiEntry.of(T0, "hello"),
                LokiEntry.of(T0.plusNanos(1), "with md", Map.of("request_id", "r-1"))),
            new LokiStream(Map.of("service_name", "empty"), List.of())));
    FakeLoki.Request r = loki.last();
    assertThat(r.method()).isEqualTo("POST");
    assertThat(r.path()).isEqualTo("/loki/api/v1/push");
    assertThat(r.header("Content-Type")).isEqualTo("application/json");
    JsonNode body = Json.tree(r.body());
    assertThat(body.get("streams")).hasSize(1);
    JsonNode s = body.get("streams").get(0);
    assertThat(s.get("stream").get("service_name").asText()).isEqualTo("api");
    assertThat(s.get("values").get(0).get(0).asText()).isEqualTo("1795034767064713072");
    assertThat(s.get("values").get(0)).hasSize(2);
    assertThat(s.get("values").get(1).get(2).get("request_id").asText()).isEqualTo("r-1");
    // nothing to send → no request
    int before = loki.requests().size();
    client.push(List.of());
    client.push(List.of(new LokiStream(null, null)));
    assertThat(loki.requests()).hasSize(before);
  }

  @Test
  void pushGuardMovesLabelsToMetadata() {
    loki.enqueue("push.json").enqueue("push.json");
    client.push(
        List.of(LokiStream.of(Map.of("service_name", "api", "run", "r1"), LokiEntry.of("x"))));
    JsonNode s = Json.tree(loki.last().body()).get("streams").get(0);
    assertThat(s.get("stream").has("run")).isFalse();
    assertThat(s.get("values").get(0).get(2).get("run").asText()).isEqualTo("r1");
    assertThat(Long.parseLong(s.get("values").get(0).get(0).asText())).isPositive();
    LokiClient raw = client(b -> b.disableLabelGuard(true));
    raw.push(List.of(LokiStream.of(Map.of("run", "r1"), LokiEntry.of("x"))));
    assertThat(
            Json.tree(loki.last().body()).get("streams").get(0).get("stream").get("run").asText())
        .isEqualTo("r1");
    assertThatThrownBy(
            () -> raw.push(List.of(LokiStream.of(Map.of("bad-label", "v"), LokiEntry.of("x")))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("stream 0");
    assertThatThrownBy(
            () -> raw.push(List.of(LokiStream.of(Map.of("__name", "v"), LokiEntry.of("x")))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("reserved");
    assertThatThrownBy(() -> raw.push(List.of(LokiStream.of(Map.of("app", ""), LokiEntry.of("x")))))
        .hasMessageContaining("non-empty");
    raw.close();
    assertThatThrownBy(
            () ->
                client.push(
                    List.of(
                        LokiStream.of(
                            Map.of("service_name", "a"), LokiEntry.of(T0, "x", Map.of("", "v"))))))
        .hasMessageContaining("structured metadata");
    assertThatThrownBy(
            () ->
                client.push(
                    List.of(
                        LokiStream.of(
                            Map.of("service_name", "a"),
                            LokiEntry.of(Instant.ofEpochSecond(-5), "x")))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("timestamp");
  }

  @Test
  void pushGzipAndProtobuf() throws Exception {
    loki.enqueue("push.json-gzip").enqueue("push.protobuf-snappy");
    LokiStream stream =
        LokiStream.of(
            Map.of("service_name", "api"),
            LokiEntry.of(T0, "hello", Map.of("b", "2", "a", "1")),
            LokiEntry.of(Instant.ofEpochSecond(0, 5), "epoch"),
            LokiEntry.of(Instant.ofEpochSecond(7), "whole"));
    client.push(List.of(stream), PushOptions.defaults().withGzip(true));
    FakeLoki.Request gz = loki.last();
    assertThat(gz.header("Content-Encoding")).isEqualTo("gzip");
    assertThat(Json.tree(gunzip(gz.body())).get("streams").get(0).get("values")).hasSize(3);
    client.pushAsync(List.of(stream), PushOptions.of(PushFormat.PROTOBUF).withGzip(true)).get();
    FakeLoki.Request pb = loki.last();
    assertThat(pb.header("Content-Type")).isEqualTo("application/x-protobuf");
    assertThat(pb.header("Content-Encoding")).isNull();
    byte[] raw = unsnappy(pb.body());
    String text = new String(raw, StandardCharsets.UTF_8);
    assertThat(text).contains("{service_name=\"api\"}").contains("hello").contains("epoch");
    assertThat(ProtobufTest.decodePush(raw))
        .containsExactly(
            "{service_name=\"api\"}|1795034767.64713072|hello|a=1,b=2",
            "{service_name=\"api\"}|0.5|epoch|",
            "{service_name=\"api\"}|7.0|whole|");
    assertThat(new PushOptions(null, false).format()).isEqualTo(PushFormat.JSON);
  }

  @Test
  void pushErrors() {
    loki.enqueue("push.invalid-label-name");
    LokiStream s = LokiStream.of(Map.of("service_name", "a"), LokiEntry.of("x"));
    assertThatThrownBy(() -> client.push(List.of(s)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("couldn't parse labels");
    loki.enqueue(Reply.empty(260));
    assertThatThrownBy(() -> client.push(List.of(s)))
        .isInstanceOfSatisfying(
            LokiServerException.class,
            e -> {
              assertThat(e.code()).isEqualTo(ErrorCode.UNAVAILABLE);
              assertThat(e.retryable()).isFalse();
              assertThat(e.httpStatus()).hasValue(260);
            });
    // WAL disk-full throttling: retryable, retried by the transport, then UnavailableException
    Reply shutting = Reply.text(500, "rpc error: code = Unknown desc = Ingester is shutting down");
    for (int i = 0; i < 4; i++) {
      loki.enqueue(shutting);
    }
    assertThatThrownBy(() -> client.push(List.of(s)))
        .isInstanceOfSatisfying(
            UnavailableException.class,
            e -> {
              assertThat(e.retryable()).isTrue();
              assertThat(e.getMessage()).contains("Ingester is shutting down");
              assertThat(e.getMessage()).contains("WAL disk is at least 90 % full");
            });
    loki.enqueue(shutting).enqueue("push.json");
    client.push(List.of(s)); // recovers on retry
    // every other 500 is a retryable UnavailableException too (SPEC 2.1)
    for (int i = 0; i < 4; i++) {
      loki.enqueue("queryRange.mt-pipe-tenants");
    }
    assertThatThrownBy(() -> client.queryRange("{a=\"b\"}"))
        .isInstanceOfSatisfying(
            UnavailableException.class,
            e -> {
              assertThat(e.retryable()).isTrue();
              assertThat(e.httpStatus()).hasValue(500);
              assertThat(e.getMessage()).contains("multiple org IDs present");
            });
    // Loki's own ingestion limit text
    loki.enqueue(
        Reply.text(429, "Ingestion rate limit exceeded for user fake (limit: 1 bytes/sec)")
            .header("Retry-After", "3600"));
    assertThatThrownBy(() -> client.push(List.of(s)))
        .isInstanceOfSatisfying(
            RateLimitException.class,
            e -> assertThat(e.retryAfter()).hasValue(Duration.ofHours(1)));
  }

  @Test
  void pushOtlp() throws Exception {
    LogRecord r =
        LogRecord.builder()
            .timestamp(T0)
            .observedTimestamp(T0.plusSeconds(1))
            .severity(Severity.ERROR)
            .body("otlp body")
            .attributes(
                Map.of(
                    "request.id",
                    "req-1",
                    "status",
                    503,
                    "ratio",
                    0.5,
                    "ok",
                    true,
                    "tags",
                    List.of("a", 1L),
                    "nested",
                    Map.of("k", "v")))
            .resource(Map.of("service.name", "api"))
            .traceId("4BF92F3577B34DA6A3CE929D0E0E4736")
            .spanId("00F067AA0BA902B7")
            .traceFlags(1)
            .build();
    LogRecord plain =
        LogRecord.builder()
            .timestamp(T0)
            .body("plain")
            .resource(Map.of("service.name", "other"))
            .build();
    loki.enqueue("otlpLogs.json").enqueue("otlpLogs.json");
    client.pushOtlp(List.of(r, plain));
    FakeLoki.Request req = loki.last();
    assertThat(req.path()).isEqualTo("/otlp/v1/logs");
    JsonNode body = Json.tree(req.body());
    assertThat(body.get("resourceLogs")).hasSize(2);
    JsonNode lr = body.get("resourceLogs").get(0).get("scopeLogs").get(0).get("logRecords").get(0);
    assertThat(lr.get("timeUnixNano").asText()).isEqualTo("1795034767064713072");
    assertThat(lr.get("observedTimeUnixNano").asText()).isEqualTo("1795034768064713072");
    assertThat(lr.get("severityNumber").asInt()).isEqualTo(17);
    assertThat(lr.get("traceId").asText()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
    assertThat(lr.get("spanId").asText()).isEqualTo("00f067aa0ba902b7");
    assertThat(lr.get("flags").asInt()).isEqualTo(1);
    assertThat(lr.get("attributes").toString())
        .contains("\"intValue\":\"503\"")
        .contains("\"doubleValue\":0.5")
        .contains("\"boolValue\":true")
        .contains("arrayValue")
        .contains("kvlistValue");
    client.pushOtlp(List.of(plain), true);
    assertThat(loki.last().header("Content-Encoding")).isEqualTo("gzip");
    assertThat(gunzip(loki.last().body()).length).isPositive();
    int n = loki.requests().size();
    client.pushOtlp(List.of());
    assertThat(loki.requests()).hasSize(n);
    loki.enqueue("otlpLogs.empty-json");
    assertThatThrownBy(() -> client.pushOtlp(List.of(plain)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("at least one valid stream is required")
        .hasMessageContaining("HTTP 422");
    loki.enqueue("otlpLogs.invalid-body");
    assertThatThrownBy(() -> client.pushOtlpAsync(List.of(plain), false).get())
        .hasCauseInstanceOf(ValidationException.class)
        .hasMessageContaining("illegal wireType");
    loki.enqueue("otlpLogs.mt-no-tenant");
    assertThatThrownBy(() -> client.pushOtlp(List.of(plain)))
        .isInstanceOf(AuthenticationException.class)
        .hasMessageContaining("no org id");
  }

  // ------------------------------------------------------------------- query

  @Test
  void queryInstant() {
    loki.enqueue("query.metric-vector");
    QueryResult r =
        client.query(
            "sum by (level) (count_over_time({run=\"x\"}[5m]))",
            QueryOptions.builder().time(T0).limit(10).direction(Direction.FORWARD).build());
    assertThat(r.resultType()).isEqualTo(ResultType.VECTOR);
    assertThat(r.vector()).isNotEmpty();
    assertThat(r.stats()).isNotEmpty();
    FakeLoki.Request req = loki.last();
    assertThat(req.path()).isEqualTo("/loki/api/v1/query");
    assertThat(req.param("time")).isEqualTo("1795034767064713072");
    assertThat(req.param("limit")).isEqualTo("10");
    assertThat(req.param("direction")).isEqualTo("forward");
    assertThat(req.header("X-Loki-Response-Encoding-Flags")).isEqualTo("categorize-labels");
    loki.enqueue("query.scalar-expr");
    QueryResult scalar = client.query("vector(1)+1");
    assertThat(scalar.resultType()).isIn(ResultType.VECTOR, ResultType.SCALAR);
    assertThat(loki.last().param("time")).isNull();
    loki.enqueue(
        Reply.json(200, "{\"data\":{\"resultType\":\"scalar\",\"result\":[1795034767.5,\"2\"]}}"));
    QueryResult s = client.query("vector(1)+1");
    assertThat(s.scalar()).hasValueSatisfying(v -> assertThat(v.value()).isEqualTo(2.0));
    assertThatThrownBy(() -> client.query("{app=\"x\"}"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("instant");
    assertThatThrownBy(() -> client.query(" ")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> client.query(null)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> client.query("vector(1)", QueryOptions.builder().limit(0).build()))
        .hasMessageContaining("limit");
    loki.enqueue("query.parse-error");
    assertThatThrownBy(() -> client.queryAsync("sum(", QueryOptions.defaults()).join())
        .hasCauseInstanceOf(ValidationException.class);
  }

  @Test
  void queryRangeStreamsAndMatrix() {
    loki.enqueue("queryRange.categorized-labels");
    QueryResult r =
        client.queryRange(
            "{run=\"x\"}",
            QueryRangeOptions.builder()
                .start(T0)
                .end(T0.plusSeconds(1))
                .since(Duration.ofMinutes(5))
                .step(Duration.ofSeconds(30))
                .interval(Duration.ofNanos(10))
                .limit(5000)
                .direction(Direction.BACKWARD)
                .build());
    assertThat(r.resultType()).isEqualTo(ResultType.STREAMS);
    assertThat(r.streams()).isNotEmpty();
    StreamEntry e = r.streams().get(0).entries().get(0);
    assertThat(e.timestamp()).isEqualTo(Nanos.toInstant(e.timestampNanos()));
    assertThat(r.streams().stream().flatMap(st -> st.entries().stream()))
        .anyMatch(x -> !x.structuredMetadata().isEmpty());
    FakeLoki.Request req = loki.last();
    assertThat(req.param("start")).isEqualTo("1795034767064713072");
    assertThat(req.param("end")).isEqualTo("1795034768064713072");
    assertThat(req.param("step")).isEqualTo("30000ms");
    assertThat(req.param("since")).isEqualTo("300000ms");
    assertThat(req.param("interval")).isEqualTo("1ms");
    assertThat(req.param("direction")).isEqualTo("backward");
    loki.enqueue("queryRange.metric-matrix");
    QueryResult m = client.queryRange("sum(count_over_time({run=\"x\"}[1m]))");
    assertThat(m.resultType()).isEqualTo(ResultType.MATRIX);
    assertThat(m.matrix().get(0).samples()).isNotEmpty();
    loki.enqueue("queryRange.json-where");
    assertThat(client.queryRange("{a=\"b\"} | json").streams().get(0).labels())
        .containsEntry("status", "503"); // uncategorized answer: parsed labels merged into stream
    loki.enqueue("queryRange.empty");
    assertThat(client.queryRange("{a=\"b\"}").streams()).isEmpty();
    assertThatThrownBy(
            () -> client.queryRange("{a=\"b\"}", QueryRangeOptions.builder().limit(5001).build()))
        .hasMessageContaining("limit");
    assertThatThrownBy(
            () ->
                client.queryRange(
                    "{a=\"b\"}", QueryRangeOptions.builder().step(Duration.ZERO).build()))
        .hasMessageContaining("step");
    loki.enqueue("queryRange.end-before-start");
    assertThatThrownBy(() -> client.queryRange("{a=\"b\"}"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("end timestamp");
  }

  @Test
  void labelsSeriesAndValues() {
    loki.enqueue("labels.ok");
    assertThat(
            client.labels(
                LabelsOptions.of(T0, T0.plusSeconds(60)).withQuery("{service_name=\"x\"}")))
        .contains("service_name");
    assertThat(loki.last().param("query")).isEqualTo("{service_name=\"x\"}");
    loki.enqueue(Reply.json(200, "{\"status\":\"success\"}"));
    assertThat(client.labelsAsync(LabelsOptions.defaults()).join()).isEmpty();
    loki.enqueue("labelValues.ok");
    assertThat(client.labelValues("level")).isNotEmpty();
    assertThat(loki.last().path()).isEqualTo("/loki/api/v1/label/level/values");
    loki.enqueue("labelValues.unknown-label");
    assertThat(client.labelValues("does_not_exist", LabelsOptions.of(null, T0))).isEmpty();
    assertThat(loki.last().param("start")).isNull();
    loki.enqueue("labelValues.unknown-label");
    assertThat(client.labelValues("x", LabelsOptions.last(Duration.ofMinutes(1)))).isEmpty();
    assertThat(loki.last().param("start")).isNotNull();
    assertThatThrownBy(() -> client.labelValues("bad-name"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("labelValues");
    loki.enqueue("series.ok");
    List<Map<String, String>> series =
        client.series(List.of("{service_name=\"a\"}", "{env=\"b\"}"), TimeRange.of(T0, null));
    assertThat(series).isNotEmpty();
    assertThat(loki.last().query().get("match[]")).hasSize(2);
    loki.enqueue(Reply.json(200, "{\"status\":\"success\"}"));
    assertThat(client.series(List.of("{a=\"b\"}"))).isEmpty();
    assertThatThrownBy(() -> client.series(List.of())).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> client.series(List.of(" "))).isInstanceOf(ValidationException.class);
    loki.enqueue(Reply.json(200, "{\"data\":{}}"));
    assertThatThrownBy(() -> client.series(List.of("{a=\"b\"}")))
        .isInstanceOf(LokiServerException.class);
    loki.enqueue(Reply.json(200, "{\"data\":{}}"));
    assertThatThrownBy(client::labels).isInstanceOf(LokiServerException.class);
    loki.enqueue("series.invalid-matcher");
    assertThatThrownBy(
            () -> client.seriesAsync(List.of("{"), TimeRange.last(Duration.ofHours(1))).join())
        .hasCauseInstanceOf(ValidationException.class);
  }

  @Test
  void indexStatsVolumeAndFormat() {
    loki.enqueue("indexStats.after-flush");
    IndexStats stats =
        client.indexStats("{service_name=\"a\"}", TimeRange.of(T0, T0.plusSeconds(1)));
    assertThat(stats.streams()).isPositive();
    assertThat(stats.entries()).isPositive();
    loki.enqueue("indexStats.before-flush");
    assertThat(client.indexStats("{service_name=\"a\"}").streams()).isZero();
    assertThatThrownBy(() -> client.indexStats("")).isInstanceOf(ValidationException.class);
    loki.enqueue("volume.target-labels");
    QueryResult v =
        client.volume(
            "{service_name=\"a\"}",
            VolumeOptions.builder()
                .start(T0)
                .end(T0.plusSeconds(10))
                .limit(5)
                .targetLabels(List.of("level", "env"))
                .aggregateBy(VolumeOptions.AggregateBy.LABELS)
                .build());
    assertThat(v.resultType()).isEqualTo(ResultType.VECTOR);
    FakeLoki.Request req = loki.last();
    assertThat(req.path()).isEqualTo("/loki/api/v1/index/volume");
    assertThat(req.param("targetLabels")).isEqualTo("level,env");
    assertThat(req.param("aggregateBy")).isEqualTo("labels");
    assertThat(req.param("limit")).isEqualTo("5");
    assertThat(req.param("step")).isNull();
    loki.enqueue("volume.ok");
    assertThat(client.volume("{service_name=\"a\"}").vector()).isNotEmpty();
    loki.enqueue("volumeRange.before-flush");
    QueryResult range =
        client.volumeRange(
            "{service_name=\"a\"}",
            VolumeOptions.builder().step(Duration.ofSeconds(60)).targetLabels(null).build());
    assertThat(range.resultType()).isEqualTo(ResultType.MATRIX);
    assertThat(loki.last().param("step")).isEqualTo("60000ms");
    loki.enqueue(FakeLoki.fixture("query.metric-vector"));
    QueryResult converted = client.volumeRange("{service_name=\"a\"}");
    assertThat(converted.resultType()).isEqualTo(ResultType.MATRIX);
    assertThat(converted.matrix().get(0).samples()).hasSize(1);
    assertThat(converted.vector()).isEmpty();
    assertThatThrownBy(() -> client.volume("{a=\"b\"}", VolumeOptions.builder().limit(0).build()))
        .hasMessageContaining("limit");
    loki.enqueue("formatQuery.ok");
    assertThat(client.formatQuery("{app=\"x\"} |= \"err\" | json | status >= 500"))
        .isEqualTo("{app=\"x\"} |= \"err\" | json | status>=500");
    loki.enqueue("formatQuery.parse-error");
    assertThatThrownBy(() -> client.formatQuery("{"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("parse error");
    loki.enqueue(Reply.json(200, "{\"status\":\"success\"}"));
    assertThatThrownBy(() -> client.formatQuery("{a=\"b\"}"))
        .isInstanceOf(LokiServerException.class);
    assertThatThrownBy(() -> client.formatQuery("")).isInstanceOf(ValidationException.class);
  }

  @Test
  void errorMapping() {
    LokiException nf = LokiException.of(ErrorCode.NOT_FOUND, "x", 404, null, null, null);
    assertThat(LokiClient.mapError(nf)).isSameAs(nf);
    LokiException plain500 =
        LokiException.of(ErrorCode.UNAVAILABLE, "boom (HTTP 500)", 500, "rid", null, null);
    assertThat(LokiClient.mapError(plain500)).isSameAs(plain500);
    assertThat(plain500.retryable()).isTrue();
    LokiException wal =
        LokiException.of(
            ErrorCode.UNAVAILABLE,
            "push: Ingester is shutting down (HTTP 503)",
            503,
            "rid",
            java.time.Duration.ofSeconds(1),
            null);
    assertThat(LokiClient.mapError(wal))
        .isInstanceOf(UnavailableException.class)
        .satisfies(
            e -> {
              assertThat(e.retryable()).isTrue();
              assertThat(e.requestId()).contains("rid");
              assertThat(e.httpStatus()).hasValue(503);
              assertThat(e.retryAfter()).contains(java.time.Duration.ofSeconds(1));
              assertThat(e.getMessage()).contains("WAL disk");
            });
    LokiException walHint =
        LokiException.of(
            ErrorCode.UNAVAILABLE, "Ingester is shutting down WAL", null, null, null, null);
    assertThat(LokiClient.mapError(walHint).getMessage())
        .isEqualTo("Ingester is shutting down WAL");
    LokiClient.closeQuietly(java.net.http.HttpClient.newHttpClient());
    LokiException net = LokiException.of(ErrorCode.UNAVAILABLE, "net", null, null, null, null);
    assertThat(LokiClient.mapError(net)).isSameAs(net);
    assertThat(LokiClient.sdkError(new IllegalStateException("x"), "op"))
        .isInstanceOf(UnavailableException.class);
    loki.enqueue("http.not-found");
    assertThatThrownBy(client::labels)
        .isInstanceOf(codes.grow.packages.loki.core.NotFoundException.class);
  }
}
