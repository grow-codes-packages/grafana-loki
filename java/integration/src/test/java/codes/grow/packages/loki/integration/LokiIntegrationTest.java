package codes.grow.packages.loki.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import codes.grow.packages.loki.CardinalityGuard;
import codes.grow.packages.loki.Direction;
import codes.grow.packages.loki.IndexStats;
import codes.grow.packages.loki.LabelsOptions;
import codes.grow.packages.loki.Labels;
import codes.grow.packages.loki.LiveTailOptions;
import codes.grow.packages.loki.LogProtocol;
import codes.grow.packages.loki.LogQL;
import codes.grow.packages.loki.LokiClient;
import codes.grow.packages.loki.LokiEntry;
import codes.grow.packages.loki.LokiLogClientOptions;
import codes.grow.packages.loki.LokiLogExporter;
import codes.grow.packages.loki.LokiLogQueryClient;
import codes.grow.packages.loki.LokiStream;
import codes.grow.packages.loki.LokiTail;
import codes.grow.packages.loki.PushFormat;
import codes.grow.packages.loki.PushOptions;
import codes.grow.packages.loki.QueryOptions;
import codes.grow.packages.loki.QueryRangeOptions;
import codes.grow.packages.loki.QueryResult;
import codes.grow.packages.loki.ResultType;
import codes.grow.packages.loki.StreamEntry;
import codes.grow.packages.loki.StreamResult;
import codes.grow.packages.loki.TailFrame;
import codes.grow.packages.loki.TimeRange;
import codes.grow.packages.loki.VolumeOptions;
import codes.grow.packages.loki.core.AuthenticationException;
import codes.grow.packages.loki.core.EnvReader;
import codes.grow.packages.loki.core.HealthStatus;
import codes.grow.packages.loki.core.Hooks;
import codes.grow.packages.loki.core.RateLimitException;
import codes.grow.packages.loki.core.RetryPolicy;
import codes.grow.packages.loki.core.UnavailableException;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.BatchOptions;
import codes.grow.packages.loki.logging.BatchingLogClient;
import codes.grow.packages.loki.logging.LogInput;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.LogSearchResult;
import codes.grow.packages.loki.logging.SearchOptions;
import codes.grow.packages.loki.logging.Severity;
import codes.grow.packages.loki.logging.SortOrder;
import codes.grow.packages.loki.logging.TailHandle;
import codes.grow.packages.loki.logging.TailOptions;
import codes.grow.packages.loki.logging.WaitForLogOptions;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Calls every public function of the Java package against the real Loki from {@code
 * docker/compose.yml} with a round-trip assertion (write, then read back).
 */
class LokiIntegrationTest {
  private static Map<String, String> env;

  @BeforeAll
  static void environment() throws IOException {
    env = ConformanceTest.environment();
  }

  private static String env(String name) {
    String v = env.get(name);
    assumeTrue(v != null && !v.isEmpty(), name + " is not set; use docker/compose.yml");
    return v;
  }

  private static String run() {
    return "j" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
  }

  private static LokiClient client() {
    env("GROW_GRAFANA_LOKI_URL");
    return LokiClient.builder().fromEnv(EnvReader.of(env)).build();
  }

  /** Polls a range query until it returns at least {@code min} entries. */
  private static List<StreamEntry> waitEntries(LokiClient c, String logql, int min)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    List<StreamEntry> out = List.of();
    while (System.nanoTime() < deadline) {
      out = new ArrayList<>();
      for (StreamResult s :
          c.queryRange(logql, QueryRangeOptions.builder().limit(1000).build()).streams()) {
        out.addAll(s.entries());
      }
      if (out.size() >= min) {
        return out;
      }
      Thread.sleep(250);
    }
    return out;
  }

  private static void flush(String base) throws Exception {
    HttpResponse<Void> r =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(base.replaceAll("/+$", "") + "/flush"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.discarding());
    assertEquals(204, r.statusCode(), "POST /flush");
  }

  @Test
  void coreStatus() {
    LokiClient c = client();
    HealthStatus h = c.health();
    assertTrue(h.healthy(), h.toString());
    assertEquals("ok", h.status());
    assertTrue(c.healthAsync().join().healthy());
    assertTrue(c.ready());
    assertTrue(c.readyAsync().join());
    assertFalse(c.ping().isNegative());
    assertFalse(c.pingAsync().join().isNegative());
    String expected = env("GROW_GRAFANA_LOKI_EXPECTED_VERSION");
    assertEquals(expected, c.serverVersion());
    assertEquals(expected, c.serverVersionAsync().join());
    assertEquals(expected, c.buildInfo().version());
    assertFalse(c.buildInfoAsync().join().revision().isEmpty());
    c.close();
    c.close();
    assertTrue(c.isClosed());
  }

  @Test
  void pushAndQuery() throws Exception {
    String run = run();
    Instant t0 = Instant.now().minusSeconds(5).plusNanos(123);
    try (LokiClient c = client()) {
      // JSON push with structured metadata; "run" is moved to metadata by the cardinality guard
      c.push(
          List.of(
              LokiStream.of(
                  Map.of("service_name", "java-it", "level", "info", "run", run),
                  LokiEntry.of(t0, "hello " + run + " first", Map.of("request_id", "req-" + run)),
                  LokiEntry.of(t0.plusMillis(1), "{\"msg\":\"json " + run + "\",\"status\":503}"))));
      // gzip JSON and async protobuf
      c.push(
          List.of(
              LokiStream.of(
                  Map.of("service_name", "java-it", "level", "error"),
                  new LokiEntry(t0.plusMillis(2), "gzip err " + run, Map.of("run", run)))),
          PushOptions.defaults().withGzip(true));
      c.pushAsync(
              List.of(
                  LokiStream.of(
                      Map.of("service_name", "java-it", "level", "warn"),
                      new LokiEntry(t0.plusMillis(3), "proto " + run, Map.of("run", run, "pb", "1")))),
              PushOptions.of(PushFormat.PROTOBUF))
          .join();
      String sel = "{service_name=\"java-it\"} | run=\"" + run + "\"";
      List<StreamEntry> all = waitEntries(c, sel, 4);
      assertEquals(4, all.size(), "pushed entries read back");
      StreamEntry proto =
          all.stream().filter(e -> e.line().startsWith("proto")).findFirst().orElseThrow();
      assertEquals("1", proto.structuredMetadata().get("pb"));
      assertEquals(t0.plusMillis(3), proto.timestamp());
      StreamEntry first =
          all.stream().filter(e -> e.line().endsWith("first")).findFirst().orElseThrow();
      assertEquals("req-" + run, first.structuredMetadata().get("request_id"));

      // queryRange: forward order, limit, line filter built with LogQL
      String builder =
          LogQL.selector(Map.of("service_name", "java-it"))
              .where("run", "=", run)
              .lineContains("hello")
              .build();
      assertEquals(builder.replace(" ", ""), c.formatQuery(builder).replace(" ", ""));
      assertFalse(c.formatQueryAsync(builder).join().isEmpty());
      QueryResult hello =
          c.queryRange(
              builder,
              QueryRangeOptions.builder()
                  .start(t0.minusSeconds(1))
                  .end(Instant.now().plusSeconds(1))
                  .limit(10)
                  .direction(Direction.FORWARD)
                  .build());
      assertEquals(ResultType.STREAMS, hello.resultType());
      assertEquals(1, hello.streams().stream().mapToInt(s -> s.entries().size()).sum());
      String json =
          LogQL.selector(Map.of("service_name", "java-it"))
              .where("run", "=", run)
              .json()
              .where("status", ">=", 500)
              .build();
      QueryResult jr = c.queryRangeAsync(json, QueryRangeOptions.defaults()).join();
      StreamEntry parsed = jr.streams().get(0).entries().get(0);
      assertEquals("503", parsed.parsed().get("status"));
      assertThrows(
          ValidationException.class, () -> c.formatQuery("{service_name=\"java-it\""));

      // metric queries
      String metric = "sum(count_over_time({service_name=\"java-it\"} | run=\"" + run + "\" [5m]))";
      QueryResult vec = c.query(metric);
      assertEquals(ResultType.VECTOR, vec.resultType());
      assertEquals(4.0, vec.vector().get(0).value());
      QueryResult vec2 =
          c.queryAsync(metric, QueryOptions.builder().time(Instant.now()).build()).join();
      assertEquals(4.0, vec2.vector().get(0).value());
      QueryResult matrix =
          c.queryRange(
              metric,
              QueryRangeOptions.builder()
                  .start(t0.minusSeconds(60))
                  .end(Instant.now().plusSeconds(60))
                  .step(Duration.ofSeconds(30))
                  .build());
      assertEquals(ResultType.MATRIX, matrix.resultType());
      assertFalse(matrix.matrix().isEmpty());
      assertThrows(ValidationException.class, () -> c.query(sel));

      // labels, label values, series (structured metadata keys are not labels)
      List<String> labels = c.labels();
      assertTrue(labels.contains("service_name"), labels.toString());
      assertFalse(labels.contains("request_id"));
      assertTrue(c.labelsAsync(LabelsOptions.defaults()).join().contains("level"));
      List<String> levels =
          c.labelValues(
              "level",
              LabelsOptions.of(t0.minusSeconds(60), Instant.now().plusSeconds(1))
                  .withQuery("{service_name=\"java-it\"}"));
      assertTrue(levels.containsAll(List.of("info", "error", "warn")), levels.toString());
      assertTrue(c.labelValuesAsync("service_name", LabelsOptions.defaults()).join().contains("java-it"));
      assertTrue(c.labelValues("does_not_exist").isEmpty());
      List<Map<String, String>> series = c.series(List.of("{service_name=\"java-it\"}"));
      assertTrue(series.stream().anyMatch(s -> "warn".equals(s.get("level"))), series.toString());
      assertFalse(
          c.seriesAsync(List.of("{service_name=\"java-it\"}"), TimeRange.last(Duration.ofHours(1)))
              .join()
              .isEmpty());

      // volume (ingester data) and index statistics (after a flush)
      // volume / volumeRange can briefly lag behind the push: poll them (30 s budget).
      long volumeDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      QueryResult volume;
      do {
        volume =
            c.volume(
                "{service_name=\"java-it\"}",
                VolumeOptions.builder()
                    .targetLabels(List.of("level"))
                    .aggregateBy(VolumeOptions.AggregateBy.LABELS)
                    .build());
        if (!volume.vector().isEmpty()) {
          break;
        }
        Thread.sleep(250);
      } while (System.nanoTime() < volumeDeadline);
      assertEquals(ResultType.VECTOR, volume.resultType());
      assertFalse(volume.vector().isEmpty(), "volume of the pushed streams");
      assertTrue(volume.vector().get(0).value() > 0);
      QueryResult volumeRange;
      do {
        volumeRange =
            c.volumeRange(
                "{service_name=\"java-it\"}",
                VolumeOptions.builder()
                    .start(t0.minusSeconds(60))
                    .end(Instant.now().plusSeconds(60))
                    .step(Duration.ofSeconds(60))
                    .build());
        if (!volumeRange.matrix().isEmpty()) {
          break;
        }
        Thread.sleep(250);
      } while (System.nanoTime() < volumeDeadline);
      assertEquals(ResultType.MATRIX, volumeRange.resultType());
      assertFalse(volumeRange.matrix().isEmpty());
      assertNotNull(c.volumeAsync("{service_name=\"java-it\"}", VolumeOptions.defaults()).join());
      assertNotNull(
          c.volumeRangeAsync("{service_name=\"java-it\"}", VolumeOptions.defaults()).join());
      flush(env("GROW_GRAFANA_LOKI_URL"));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      IndexStats stats = c.indexStats("{service_name=\"java-it\"}");
      while (stats.entries() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(500);
        stats = c.indexStats("{service_name=\"java-it\"}");
      }
      assertTrue(stats.streams() > 0 && stats.entries() >= 4, stats.toString());
      assertTrue(
          c.indexStatsAsync("{service_name=\"java-it\"}", TimeRange.last(Duration.ofHours(1)))
                  .join()
                  .chunks()
              > 0);
    }
  }

  @Test
  void pushOtlpAndTraceCorrelation() throws Exception {
    String run = run();
    String traceId = UUID.randomUUID().toString().replace("-", "");
    try (LokiClient c = client()) {
      LogRecord r =
          LogRecord.builder()
              .timestamp(Instant.now())
              .severity(Severity.ERROR)
              .body("otlp " + run)
              .attributes(Map.of("request.id", "req-" + run, "status", 503))
              .resource(Map.of("service.name", "java-otlp", "deployment.environment", "it"))
              .traceId(traceId)
              .spanId("00f067aa0ba902b7")
              .build();
      c.pushOtlp(List.of(r));
      c.pushOtlp(List.of(r.toBuilder().body("otlp gzip " + run).build()), true);
      c.pushOtlpAsync(List.of(r.toBuilder().body("otlp async " + run).build()), false).join();
      LokiLogQueryClient q = c.logQueryClient();
      List<LogRecord> found = new ArrayList<>();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (found.size() < 3 && System.nanoTime() < deadline) {
        found = q.search("{service_name=\"java-otlp\"} | trace_id=\"" + traceId + "\"").records();
        Thread.sleep(250);
      }
      assertEquals(3, found.size());
      LogRecord back = found.get(0);
      assertEquals(traceId, back.traceId().orElseThrow());
      assertEquals("00f067aa0ba902b7", back.spanId().orElseThrow());
      assertEquals(Severity.ERROR, back.severity());
      assertEquals("java-otlp", back.resource().get("service.name"));
      assertEquals("req-" + run, back.attributes().get("request_id"));
    }
  }

  @Test
  void loggingCategory() throws Exception {
    String run = run();
    try (LokiClient c = client()) {
      Map<String, Object> resource = Map.of("service.name", "java-logs", "deployment.environment", "it");
      BatchingLogClient json =
          c.logClient(
              LokiLogClientOptions.builder()
                  .resource(resource)
                  .batchOptions(BatchOptions.builder().flushInterval(Duration.ofMillis(200)).build())
                  .build());
      BatchingLogClient proto =
          c.logClient(
              LokiLogClientOptions.builder()
                  .protocol(LogProtocol.PROTOBUF)
                  .resource(resource)
                  .allowedLabels(List.of("service_name", "level"))
                  .build());
      BatchingLogClient otlp =
          c.logClient(
              LokiLogClientOptions.builder().protocol(LogProtocol.OTLP).resource(resource).build());
      json.send(
          LogInput.builder("json " + run)
              .severity(Severity.WARN)
              .attribute("run", run)
              .attribute("password", "hunter2-" + run)
              .traceId("4bf92f3577b34da6a3ce929d0e0e4736")
              .spanId("00f067aa0ba902b7")
              .build());
      json.logger("it", Map.of("run", run)).child(Map.of("child", "yes")).error("logger " + run, Map.of());
      proto.sendBatch(List.of(LogInput.of("proto " + run), LogInput.of("proto2 " + run)));
      otlp.send(LogInput.of(Severity.INFO, "otlp " + run, Map.of("run", run)));
      json.flush(Duration.ofSeconds(10));
      proto.flush(Duration.ofSeconds(10));
      otlp.flush(Duration.ofSeconds(10));
      assertEquals(2, json.stats().sent());
      assertEquals(2, proto.stats().sent());
      assertEquals(1, otlp.stats().sent());

      LokiLogQueryClient q = c.logQueryClient();
      LogRecord warn =
          q.waitForLog(
              "json " + run, WaitForLogOptions.of(Duration.ofSeconds(30), Duration.ofMillis(250)));
      assertEquals(Severity.WARN, warn.severity());
      assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", warn.traceId().orElseThrow());
      assertEquals("java-logs", warn.resource().get("service.name"));
      assertEquals("[REDACTED]", warn.attributes().get("password"));
      assertEquals(run, warn.attributes().get("run"));
      LogRecord child =
          q.waitForLog(
              run,
              WaitForLogOptions.of(Duration.ofSeconds(30), Duration.ofMillis(250))
                  .withPredicate(r -> r.body().startsWith("logger")));
      assertEquals("yes", child.attributes().get("child"));
      assertEquals(Severity.ERROR, child.severity());

      // search: free text, all five records, paged with cursors (desc and asc)
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      LogSearchResult all = q.search(run);
      while (all.records().size() < 5 && System.nanoTime() < deadline) {
        Thread.sleep(250);
        all = q.search(run);
      }
      assertEquals(5, all.records().size());
      for (SortOrder order : SortOrder.values()) {
        Set<String> seen = new HashSet<>();
        SearchOptions opts =
            SearchOptions.builder()
                .limit(2)
                .order(order)
                .start(Instant.now().minusSeconds(600))
                .end(Instant.now().plusSeconds(5))
                .build();
        int pages = 0;
        while (true) {
          LogSearchResult page = q.searchAsync(run, opts).join();
          page.records().forEach(r -> assertTrue(seen.add(r.body()), "duplicate " + r.body()));
          pages++;
          if (page.nextCursor().isEmpty()) {
            break;
          }
          opts = opts.toBuilder().cursor(page.nextCursor().get()).build();
        }
        assertEquals(5, seen.size(), order + " pages");
        assertTrue(pages >= 3);
      }

      List<String> streams = q.listStreams();
      assertTrue(
          streams.stream().anyMatch(s -> s.contains("service_name=\"java-logs\"")), streams.toString());
      assertFalse(q.listStreamsAsync().join().isEmpty());
      assertTrue(q.listLabels().contains("service_name"));
      assertTrue(q.listLabelsAsync().join().contains("service_name"));

      // the exporter mapping used above, called directly
      List<LokiStream> mapped =
          LokiLogExporter.recordsToStreams(List.of(warn), CardinalityGuard.defaults());
      assertEquals("java-logs", mapped.get(0).labels().get("service_name"));
      assertTrue(Labels.formatLabelSet(mapped.get(0).labels()).startsWith("{"));

      // close flushes queued records
      json.send(LogInput.of("on close " + run));
    }
    try (LokiClient c = client()) {
      assertNotNull(
          c.logQueryClient()
              .waitForLog(
                  "on close " + run,
                  WaitForLogOptions.of(Duration.ofSeconds(30), Duration.ofMillis(250))));
    }
  }

  @Test
  void tail() throws Exception {
    String run = run();
    try (LokiClient c = client()) {
      LinkedBlockingQueue<TailFrame> frames = new LinkedBlockingQueue<>();
      List<Throwable> errors = new CopyOnWriteArrayList<>();
      LokiTail tail =
          c.tail(
              "{service_name=\"java-tail\"} | run=\"" + run + "\"",
              frames::add,
              LiveTailOptions.builder()
                  .start(Instant.now())
                  .limit(10)
                  .delayFor(Duration.ZERO)
                  .onError(errors::add)
                  .build());
      assertTrue(tail.isOpen());
      c.push(
          List.of(
              LokiStream.of(
                  Map.of("service_name", "java-tail"),
                  new LokiEntry(null, "tail marker " + run, Map.of("run", run)))));
      TailFrame f = frames.poll(20, TimeUnit.SECONDS);
      assertNotNull(f, "tail frame");
      StreamEntry e = f.streams().get(0).entries().get(0);
      assertEquals("tail marker " + run, e.line());
      assertEquals(run, e.structuredMetadata().get("run"));
      tail.stop();
      tail.closed().get(10, TimeUnit.SECONDS);
      assertTrue(errors.isEmpty(), errors.toString());

      // async tail and the category tail
      LokiTail t2 = c.tailAsync("{service_name=\"java-tail\"}", frames::add, LiveTailOptions.defaults()).join();
      t2.close();
      LinkedBlockingQueue<LogRecord> records = new LinkedBlockingQueue<>();
      TailHandle h =
          c.logQueryClient()
              .tail(
                  "{service_name=\"java-tail\"} | run=\"" + run + "\"",
                  records::add,
                  TailOptions.defaults().withOnError(errors::add));
      // the handshake is asynchronous: push until the record arrives
      LogRecord got = null;
      for (int i = 0; i < 20 && got == null; i++) {
        c.push(
            List.of(
                LokiStream.of(
                    Map.of("service_name", "java-tail"),
                    new LokiEntry(null, "category tail " + run + " " + i, Map.of("run", run)))));
        got = records.poll(1, TimeUnit.SECONDS);
      }
      assertNotNull(got, "category tail record");
      assertTrue(got.body().startsWith("category tail " + run));
      assertEquals("java-tail", got.resource().get("service.name"));
      h.stop();

      assertThrows(ValidationException.class, () -> c.tail("rate({a=\"b\"}[1m])", f2 -> {}));
      assertThrows(
          ValidationException.class,
          () ->
              c.tail(
                  "{a=\"b\"}",
                  f2 -> {},
                  LiveTailOptions.builder().delayFor(Duration.ofSeconds(6)).build()));
      assertThrows(ValidationException.class, () -> c.tail("{a=\"b\"", f2 -> {})); // server 400
    }
  }

  @Test
  void multiTenant() throws Exception {
    String url = env("GROW_GRAFANA_LOKI_MULTI_TENANT_URL");
    String a = env("GROW_GRAFANA_LOKI_TENANT_A");
    String b = env("GROW_GRAFANA_LOKI_TENANT_B");
    String run = run();
    try (LokiClient root = LokiClient.builder().url(url).build()) {
      assertTrue(root.health().healthy(), "/ready needs no tenant");
      assertThrows(AuthenticationException.class, root::ping);
      assertThrows(
          AuthenticationException.class,
          () -> root.push(List.of(LokiStream.of(Map.of("service_name", "mt"), LokiEntry.of("x")))));
      assertThrows(AuthenticationException.class, () -> root.tail("{service_name=\"mt\"}", f -> {}));
      LokiClient ta = root.withTenant(a);
      LokiClient tb = root.withTenant(b);
      assertEquals(a, ta.tenantId().orElseThrow());
      assertTrue(root.tenantId().isEmpty());
      assertFalse(ta.ping().isNegative());
      ta.push(
          List.of(
              LokiStream.of(
                  Map.of("service_name", "mt"), new LokiEntry(null, "tenant a " + run, Map.of("run", run)))));
      String sel = "{service_name=\"mt\"} | run=\"" + run + "\"";
      assertEquals(1, waitEntries(ta, sel, 1).size());
      assertTrue(tb.queryRange(sel).streams().isEmpty(), "tenant b must not see tenant a");
      assertTrue(ta.labelValues("service_name").contains("mt"));
      LinkedBlockingQueue<TailFrame> frames = new LinkedBlockingQueue<>();
      LokiTail tail = ta.tail(sel, frames::add, LiveTailOptions.builder().start(Instant.now().minusSeconds(60)).build());
      assertNotNull(frames.poll(20, TimeUnit.SECONDS), "tenant tail replays the entry");
      tail.stop();
      assertThrows(ValidationException.class, () -> root.withTenant("a|b"));
      ta.close();
      tb.close();
      assertFalse(root.transport().isClosed(), "children share the transport");
    }
  }

  @Test
  void gatewayAuthAndRetries() throws Exception {
    String gw = env("GROW_GRAFANA_LOKI_AUTH_PROXY_URL");
    String user = env("GROW_GRAFANA_LOKI_AUTH_PROXY_USERNAME");
    String pass = env("GROW_GRAFANA_LOKI_AUTH_PROXY_PASSWORD");
    String bearer = env("GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN");
    String run = run();
    try (LokiClient basic = LokiClient.builder().url(gw).basicAuth(user, pass).build()) {
      assertTrue(basic.health().healthy());
      assertFalse(basic.ping().isNegative());
    }
    try (LokiClient wrong = LokiClient.builder().url(gw).basicAuth(user, "wrong-" + run).build()) {
      AuthenticationException e = assertThrows(AuthenticationException.class, wrong::ping);
      assertFalse(e.getMessage().contains("wrong-" + run), "credentials are redacted");
      assertFalse(wrong.health().healthy());
    }
    try (LokiClient tok = LokiClient.builder().url(gw).bearerToken(bearer).build()) {
      tok.push(
          List.of(
              LokiStream.of(
                  Map.of("service_name", "java-gw"),
                  new LokiEntry(null, "gateway " + run, Map.of("run", run)))));
      assertEquals(1, waitEntries(tok, "{service_name=\"java-gw\"} | run=\"" + run + "\"", 1).size());
      LinkedBlockingQueue<TailFrame> frames = new LinkedBlockingQueue<>();
      LokiTail tail =
          tok.tail(
              "{service_name=\"java-gw\"} | run=\"" + run + "\"",
              frames::add,
              LiveTailOptions.builder().start(Instant.now().minusSeconds(60)).build());
      assertNotNull(frames.poll(20, TimeUnit.SECONDS), "tail through the gateway");
      tail.stop();
    }
    AtomicInteger attempts = new AtomicInteger();
    Hooks hooks =
        new Hooks() {
          @Override
          public void onRequest(Hooks.RequestInfo info) {
            attempts.incrementAndGet();
          }
        };
    for (String fault : List.of("429", "503", "502")) {
      attempts.set(0);
      try (LokiClient faulty =
          LokiClient.builder()
              .url(gw)
              .bearerToken(bearer)
              .header("X-Grow-Fault", fault)
              .hooks(hooks)
              .retry(RetryPolicy.builder().maxRetries(1).initialBackoff(Duration.ofMillis(10)).build())
              .build()) {
        Class<? extends Exception> expected =
            fault.equals("429") ? RateLimitException.class : UnavailableException.class;
        assertThrows(expected, faulty::labels);
        assertEquals(2, attempts.get(), fault + " is retried once");
      }
    }
    // Loki answers a multi-tenant (pipe) id with 500 when multi-tenant queries are disabled; a
    // plain 500 is a retryable UnavailableException (DECISIONS §8-§10: LokiServerException is
    // only for HTTP 260 and undecodable responses)
    try (LokiClient mt =
        LokiClient.builder()
            .url(env("GROW_GRAFANA_LOKI_MULTI_TENANT_URL"))
            .multiTenantQueries(true)
            .retry(RetryPolicy.builder().maxRetries(0).build())
            .build()) {
      LokiClient pipe = mt.withTenant(env("GROW_GRAFANA_LOKI_TENANT_A") + "|" + env("GROW_GRAFANA_LOKI_TENANT_B"));
      assertThrows(
          ValidationException.class,
          () -> pipe.push(List.of(LokiStream.of(Map.of("service_name", "mt"), LokiEntry.of("x")))));
      UnavailableException e =
          assertThrows(UnavailableException.class, () -> pipe.queryRange("{service_name=\"mt\"}"));
      assertTrue(e.retryable());
      assertEquals(500, e.httpStatus().orElse(-1));
    }
  }
}
