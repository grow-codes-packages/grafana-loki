package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.FakeLoki.Reply;
import codes.grow.packages.loki.core.AuthenticationException;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.LogSearchResult;
import codes.grow.packages.loki.logging.SearchOptions;
import codes.grow.packages.loki.logging.Severity;
import codes.grow.packages.loki.logging.SortOrder;
import codes.grow.packages.loki.logging.TailHandle;
import codes.grow.packages.loki.logging.TailOptions;
import codes.grow.packages.loki.logging.WaitForLogOptions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LokiLogQueryClientTest {
  private FakeLoki loki;
  private LokiClient client;
  private LokiLogQueryClient query;

  @BeforeEach
  void start() {
    loki = new FakeLoki();
    client = LokiClient.builder().url(loki.url()).build();
    query = client.logQueryClient();
  }

  @AfterEach
  void stop() {
    client.close();
    loki.close();
  }

  /** A streams response; each stream is {label value of "s", ns...}. */
  static Reply streams(Object[]... streams) {
    StringBuilder sb =
        new StringBuilder(
            "{\"status\":\"success\",\"data\":{\"resultType\":\"streams\",\"result\":[");
    for (int i = 0; i < streams.length; i++) {
      Object[] s = streams[i];
      sb.append(i > 0 ? "," : "")
          .append("{\"stream\":{\"s\":\"")
          .append(s[0])
          .append("\"},\"values\":[");
      for (int k = 1; k < s.length; k++) {
        sb.append(k > 1 ? "," : "")
            .append("[\"")
            .append(s[k])
            .append("\",\"line ")
            .append(s[0])
            .append(s[k])
            .append("\"]");
      }
      sb.append("]}");
    }
    return Reply.json(200, sb.append("]}}").toString());
  }

  private static List<String> lines(LogSearchResult r) {
    return r.records().stream().map(LogRecord::body).toList();
  }

  @Test
  void searchMergesAndPagesThroughEqualTimestamps() {
    loki.enqueue(streams(new Object[] {"b", 30}, new Object[] {"a", 20}))
        .enqueue(streams(new Object[] {"a", 20}, new Object[] {"b", 20}));
    LogSearchResult p1 = query.search("{s=~\".+\"}", SearchOptions.builder().limit(2).build());
    assertThat(lines(p1)).containsExactly("line b30", "line a20");
    assertThat(loki.requests().get(0).param("limit")).isEqualTo("2");
    assertThat(loki.requests().get(0).param("direction")).isEqualTo("backward");
    assertThat(loki.requests().get(1).param("start")).isEqualTo("20");
    assertThat(loki.requests().get(1).param("end")).isEqualTo("21");
    assertThat(p1.nextCursor()).isPresent();
    assertThat(p1.total()).isEmpty();
    assertThat(LokiLogQueryClient.decodeCursor(p1.nextCursor().get())).containsExactly(20, 1);

    loki.enqueue(streams(new Object[] {"a", 20, 10}, new Object[] {"b", 20}))
        .enqueue(streams(new Object[] {"a", 10}));
    LogSearchResult p2 =
        query.search(
            "{s=~\".+\"}", SearchOptions.builder().limit(2).cursor(p1.nextCursor().get()).build());
    assertThat(loki.requests().get(2).param("end")).isEqualTo("21");
    assertThat(loki.requests().get(2).param("limit")).isEqualTo("3");
    assertThat(lines(p2)).containsExactly("line b20", "line a10");
    assertThat(LokiLogQueryClient.decodeCursor(p2.nextCursor().get())).containsExactly(10, 1);

    loki.enqueue(streams(new Object[] {"a", 10}));
    LogSearchResult p3 =
        query.search(
            "{s=~\".+\"}", SearchOptions.builder().limit(2).cursor(p2.nextCursor().get()).build());
    assertThat(p3.records()).isEmpty();
    assertThat(p3.nextCursor()).isEmpty();
  }

  @Test
  void searchAscendingWithCursorAndRemaining() {
    // ascending, cursor at ns 5 with 1 already returned; Loki returns more than the page
    loki.enqueue(streams(new Object[] {"a", 5, 6, 7})).enqueue(streams(new Object[] {"a", 7}));
    String cursor = LokiLogQueryClient.encodeCursor(5, 1);
    LogSearchResult r =
        query.search(
            "free text",
            SearchOptions.builder()
                .order(SortOrder.ASC)
                .limit(1)
                .start(Instant.ofEpochSecond(1))
                .end(Instant.ofEpochSecond(2))
                .cursor(cursor)
                .build());
    FakeLoki.Request req = loki.requests().get(0);
    assertThat(req.param("start")).isEqualTo("5");
    assertThat(req.param("end")).isEqualTo("2000000000");
    assertThat(req.param("direction")).isEqualTo("forward");
    assertThat(req.param("query")).isEqualTo("{service_name=~\".+\"} |= \"free text\"");
    // 3 entries >= fetch 2 → boundary refetch was needed
    assertThat(loki.requests()).hasSize(2);
    assertThat(loki.last().param("start")).isEqualTo("7");
    assertThat(lines(r)).containsExactly("line a6");
    assertThat(LokiLogQueryClient.decodeCursor(r.nextCursor().get())).containsExactly(6, 1);
  }

  @Test
  void searchCursorAtSameTimestampAccumulates() {
    loki.enqueue(streams(new Object[] {"a", 9}, new Object[] {"b", 9}, new Object[] {"c", 9}))
        .enqueue(streams(new Object[] {"a", 9}, new Object[] {"b", 9}, new Object[] {"c", 9}));
    LogSearchResult r =
        query.search(
            "{s=~\".+\"}",
            SearchOptions.builder().limit(1).cursor(LokiLogQueryClient.encodeCursor(9, 1)).build());
    assertThat(lines(r)).containsExactly("line b9");
    assertThat(LokiLogQueryClient.decodeCursor(r.nextCursor().get())).containsExactly(9, 2);
  }

  @Test
  void searchValidationAndErrors() {
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().limit(5001).build()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().cursor("%%%").build()))
        .hasMessageContaining("cursor");
    String bad = Base64.getUrlEncoder().encodeToString("abc".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().cursor(bad).build()))
        .hasMessageContaining("cursor");
    String overflow =
        Base64.getUrlEncoder()
            .encodeToString("9999999999999999999:1".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().cursor(overflow).build()))
        .hasMessageContaining("cursor");
    loki.enqueue(Reply.text(401, "no org id"));
    assertThatThrownBy(() -> query.search("x")).isInstanceOf(AuthenticationException.class);
    loki.enqueue("queryRange.empty");
    assertThat(query.searchAsync("", SearchOptions.defaults()).join().records()).isEmpty();
    assertThat(loki.last().param("query")).isEqualTo(LokiLogQueryClient.ALL_STREAMS_SELECTOR);
    loki.enqueue(
            Reply.json(
                200,
                "{\"data\":{\"resultType\":\"streams\",\"result\":[{\"stream\":{},\"values\":[[\"1\",\"a\"]]}]}}"))
        .enqueue(Reply.text(400, "boom"));
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().limit(1).build()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void toLogQL() {
    assertThat(LokiLogQueryClient.toLogQL(" {a=\"b\"} |= \"x\" ")).isEqualTo("{a=\"b\"} |= \"x\"");
    assertThat(LokiLogQueryClient.toLogQL("  ")).isEqualTo(LokiLogQueryClient.ALL_STREAMS_SELECTOR);
    assertThat(LokiLogQueryClient.toLogQL("say \"hi\""))
        .isEqualTo("{service_name=~\".+\"} |= \"say \\\"hi\\\"\"");
  }

  @Test
  void recordMapping() {
    Map<String, String> labels =
        Map.of("service_name", "api", "deployment_environment", "prod", "env", "x");
    StreamEntry e =
        new StreamEntry(
            1_000_000_005L,
            Nanos.toInstant(1_000_000_005L),
            "body",
            Map.of(
                "severity_number", "17",
                "severity_text", "ERR",
                "trace_id", "4BF92F3577B34DA6A3CE929D0E0E4736",
                "span_id", "00F067AA0BA902B7",
                "trace_flags", "1",
                "observed_timestamp", "2000000000",
                "request_id", "r1",
                "flags", "9"),
            Map.of("status", "500"));
    LogRecord r = LokiLogQueryClient.toLogRecord(labels, e);
    assertThat(r.timestamp()).isEqualTo(Instant.ofEpochSecond(1, 5));
    assertThat(r.severity()).isEqualTo(Severity.ERROR);
    assertThat(r.severityText()).isEqualTo("ERR");
    assertThat(r.body()).isEqualTo("body");
    assertThat(r.traceId()).contains("4bf92f3577b34da6a3ce929d0e0e4736");
    assertThat(r.spanId()).contains("00f067aa0ba902b7");
    assertThat(r.traceFlags()).hasValue(1);
    assertThat(r.observedTimestamp()).contains(Instant.ofEpochSecond(2));
    assertThat(r.attributes())
        .containsEntry("request_id", "r1")
        .containsEntry("status", "500")
        .containsEntry("flags", "9")
        .doesNotContainKeys("severity_number", "trace_id");
    assertThat(r.resource())
        .containsEntry("service.name", "api")
        .containsEntry("deployment.environment", "prod")
        .containsEntry("env", "x");

    // OTLP-style metadata: "flags" is consumed as trace flags
    LogRecord otlp =
        LokiLogQueryClient.toLogRecord(
            Map.of(),
            entry(
                Map.of(
                    "trace_id",
                    "4bf92f3577b34da6a3ce929d0e0e4736",
                    "flags",
                    "1",
                    "detected_level",
                    "warn")));
    assertThat(otlp.traceFlags()).hasValue(1);
    assertThat(otlp.attributes()).doesNotContainKey("flags");
    assertThat(otlp.severity()).isEqualTo(Severity.WARN);
    assertThat(otlp.spanId()).isEmpty();

    // invalid values are ignored
    LogRecord junk =
        LokiLogQueryClient.toLogRecord(
            Map.of("level", "debug"),
            entry(
                Map.of(
                    "severity_number", "x",
                    "trace_id", "nothex",
                    "observed_timestamp", "soon")));
    assertThat(junk.severity()).isEqualTo(Severity.DEBUG);
    assertThat(junk.traceId()).isEmpty();
    assertThat(junk.observedTimestamp()).isEmpty();
    LogRecord badSpan =
        LokiLogQueryClient.toLogRecord(
            Map.of(),
            entry(
                Map.of(
                    "trace_id", "4bf92f3577b34da6a3ce929d0e0e4736",
                    "span_id", "zz",
                    "trace_flags", "999",
                    "severity_number", "12345678901",
                    "observed_timestamp", "123456789012345678901")));
    assertThat(badSpan.spanId()).isEmpty();
    assertThat(badSpan.traceFlags()).isEmpty();
    assertThat(badSpan.severity()).isEqualTo(Severity.INFO);
    assertThat(badSpan.observedTimestamp()).isEmpty();
    LogRecord bigFlags =
        LokiLogQueryClient.toLogRecord(
            Map.of(),
            entry(Map.of("trace_id", "4bf92f3577b34da6a3ce929d0e0e4736", "trace_flags", "300")));
    assertThat(bigFlags.traceFlags()).isEmpty();
    LogRecord flagsText =
        LokiLogQueryClient.toLogRecord(
            Map.of(),
            entry(Map.of("trace_id", "4bf92f3577b34da6a3ce929d0e0e4736", "trace_flags", "x")));
    assertThat(flagsText.traceFlags()).isEmpty();
    assertThat(LokiLogQueryClient.toLogRecord(Map.of(), entry(Map.of())).severity())
        .isEqualTo(Severity.INFO);
  }

  private static StreamEntry entry(Map<String, String> md) {
    return new StreamEntry(1, Nanos.toInstant(1), "l", md, Map.of());
  }

  @Test
  void listStreamsLabelsAndWait() {
    loki.enqueue("series.ok");
    List<String> streams = query.listStreams();
    assertThat(streams).isNotEmpty().isSorted();
    assertThat(streams.get(0)).startsWith("{");
    assertThat(loki.last().param("match[]")).isEqualTo(LokiLogQueryClient.ALL_STREAMS_SELECTOR);
    loki.enqueue("labels.ok");
    assertThat(query.listLabels()).contains("service_name");
    loki.enqueue("queryRange.empty").enqueue(streams(new Object[] {"a", 1}));
    LogRecord found =
        query.waitForLog(
            "{s=\"a\"}", WaitForLogOptions.of(Duration.ofSeconds(5), Duration.ofMillis(10)));
    assertThat(found.body()).isEqualTo("line a1");
  }

  @Test
  void tailDeliversRecordsAndStops() throws Exception {
    LinkedBlockingQueue<String> closeSeen = new LinkedBlockingQueue<>();
    String frame = FakeLoki.load("tail.ok").get("response").get("frame").toString();
    try (FakeWs ws =
        FakeWs.accept(
            conn -> {
              conn.text(frame);
              closeSeen.add(conn.awaitClose() ? "close" : "eof");
            })) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      LinkedBlockingQueue<LogRecord> got = new LinkedBlockingQueue<>();
      List<Throwable> errors = new CopyOnWriteArrayList<>();
      TailHandle h =
          c.logQueryClient()
              .tail("tail marker", got::add, TailOptions.defaults().withOnError(errors::add));
      LogRecord r = got.poll(5, TimeUnit.SECONDS);
      assertThat(r).isNotNull();
      assertThat(r.body()).contains("tail marker");
      assertThat(ws.requestLine()).contains("start=").contains("query=");
      h.stop();
      assertThat(closeSeen.poll(5, TimeUnit.SECONDS)).isEqualTo("close");
      assertThat(errors).isEmpty();
      h.stop();
      c.close();
    }
  }

  @Test
  void tailReportsHandshakeErrorsAndStopsEarly() throws Exception {
    try (FakeWs ws = FakeWs.reject(401, "no org id")) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      LinkedBlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();
      TailHandle h =
          c.logQueryClient()
              .tail(
                  "{a=\"b\"}",
                  r -> {},
                  new TailOptions(Optional.of(Instant.ofEpochSecond(5)), null, errors::add));
      assertThat(errors.poll(5, TimeUnit.SECONDS)).isInstanceOf(AuthenticationException.class);
      h.stop();
      c.close();
    }
    // stop before the connection is established: the socket is closed as soon as it opens
    LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<>();
    try (FakeWs ws = FakeWs.accept(conn -> seen.add(conn.awaitClose() ? "close" : "eof"))) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      List<Throwable> errors = new CopyOnWriteArrayList<>();
      TailHandle h =
          c.logQueryClient()
              .tail("{a=\"b\"}", r -> {}, TailOptions.defaults().withOnError(errors::add));
      h.stop();
      assertThat(seen.poll(5, TimeUnit.SECONDS)).isEqualTo("close");
      assertThat(errors).isEmpty();
      c.close();
    }
  }
}
