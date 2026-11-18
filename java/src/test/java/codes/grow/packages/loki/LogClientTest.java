package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;

import codes.grow.packages.loki.FakeLoki.Reply;
import codes.grow.packages.loki.core.Json;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.BatchOptions;
import codes.grow.packages.loki.logging.BatchingLogClient;
import codes.grow.packages.loki.logging.ExportOptions;
import codes.grow.packages.loki.logging.ExportResult;
import codes.grow.packages.loki.logging.LogInput;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.Severity;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogClientTest {
  private FakeLoki loki;
  private LokiClient client;

  @BeforeEach
  void start() {
    loki = new FakeLoki().fallback(Reply.empty(204));
    client = LokiClient.builder().url(loki.url()).build();
  }

  @AfterEach
  void stop() {
    client.close();
    loki.close();
  }

  @Test
  void recordsToStreamsAppliesGuard() {
    LogRecord a =
        LogRecord.builder()
            .timestamp(Instant.ofEpochSecond(1))
            .observedTimestamp(Instant.ofEpochSecond(2))
            .severity(Severity.WARN)
            .body("a")
            .resource(Map.of("service.name", "api", "host.name", "h1"))
            .attributes(Map.of("run", "r1", "password", "x", "n", 2, "empty", ""))
            .traceId("4BF92F3577B34DA6A3CE929D0E0E4736")
            .spanId("00F067AA0BA902B7")
            .traceFlags(1)
            .build();
    LogRecord b =
        LogRecord.builder()
            .timestamp(Instant.ofEpochSecond(3))
            .body("b")
            .resource(Map.of("service.name", "api", "host.name", "h1"))
            .attributes(Map.of("level", "custom"))
            .build();
    List<LokiStream> streams =
        LokiLogExporter.recordsToStreams(List.of(a, b, a), CardinalityGuard.defaults());
    assertThat(streams).hasSize(2);
    LokiStream s = streams.get(0);
    assertThat(s.labels())
        .containsOnly(
            Map.entry("service_name", "api"),
            Map.entry("host_name", "h1"),
            Map.entry("level", "warn"));
    assertThat(s.entries()).hasSize(2);
    Map<String, String> md = s.entries().get(0).structuredMetadata();
    assertThat(md)
        .containsEntry("run", "r1")
        .containsEntry("n", "2")
        .containsEntry("severity_text", "WARN")
        .containsEntry("severity_number", "13")
        .containsEntry("trace_id", "4bf92f3577b34da6a3ce929d0e0e4736")
        .containsEntry("span_id", "00f067aa0ba902b7")
        .containsEntry("trace_flags", "1")
        .containsEntry("observed_timestamp", "2000000000")
        .doesNotContainKey("empty");
    assertThat(streams.get(1).labels()).containsEntry("level", "custom");
    List<LokiStream> noLevel =
        LokiLogExporter.recordsToStreams(List.of(b), new CardinalityGuard(List.of("service_name")));
    assertThat(noLevel.get(0).labels()).containsOnlyKeys("service_name");
  }

  @Test
  void logClientJsonRoundTrip() {
    BatchingLogClient lc =
        client.logClient(
            LokiLogClientOptions.builder()
                .resource(Map.of("service.name", "api"))
                .batchOptions(BatchOptions.builder().gzip(false).build())
                .build());
    lc.send(LogInput.of(Severity.ERROR, "boom", Map.of("run", "r1", "password", "hunter2")));
    lc.logger("svc").info("hello", Map.of("k", "v"));
    lc.flush(Duration.ofSeconds(5));
    assertThat(lc.stats().sent()).isEqualTo(2);
    FakeLoki.Request r = loki.last();
    JsonNode body = Json.tree(r.body());
    String text = body.toString();
    assertThat(text).contains("boom").contains("hello").doesNotContain("hunter2");
    assertThat(body.get("streams").get(0).get("stream").get("service_name").asText())
        .isEqualTo("api");
  }

  @Test
  void logClientProtobufOtlpAndClose() throws Exception {
    BatchingLogClient pb =
        client.logClient(
            LokiLogClientOptions.builder()
                .protocol(LogProtocol.PROTOBUF)
                .allowedLabels(List.of("service_name"))
                .redaction(null)
                .build());
    pb.send(LogInput.of("pb line"));
    pb.flush(Duration.ofSeconds(5));
    assertThat(loki.last().header("Content-Type")).isEqualTo("application/x-protobuf");
    BatchingLogClient otlp =
        client.logClient(LokiLogClientOptions.builder().protocol(LogProtocol.OTLP).build());
    otlp.send(LogInput.of("otlp line"));
    otlp.flush(Duration.ofSeconds(5));
    assertThat(loki.last().path()).isEqualTo("/otlp/v1/logs");
    BatchingLogClient def = client.logClient();
    def.send(LogInput.of("closed flushes"));
    client.close(); // flushes every log client
    assertThat(loki.last().header("Content-Encoding")).isEqualTo("gzip");
    assertThat(
            new String(
                LokiClientTest.gunzip(loki.last().body()), java.nio.charset.StandardCharsets.UTF_8))
        .contains("closed flushes");
  }

  @Test
  void exportResults() throws Exception {
    LokiLogExporter exporter =
        new LokiLogExporter(client, PushFormat.JSON, CardinalityGuard.defaults());
    ExportOptions opts = new ExportOptions(false, Duration.ofSeconds(5));
    assertThat(exporter.export(List.of(), opts).get().accepted()).isZero();
    LogRecord r = LogRecord.builder().timestamp(Instant.ofEpochSecond(1)).body("x").build();
    assertThat(exporter.export(List.of(r), opts).get()).isEqualTo(ExportResult.success(1));
    loki.enqueue(Reply.text(400, "entry too old"));
    ExportResult rejected = exporter.export(List.of(r), opts).get();
    assertThat(rejected.rejected()).isEqualTo(1);
    assertThat(rejected.error())
        .hasValueSatisfying(e -> assertThat(e).isInstanceOf(ValidationException.class));
    LogRecord bad =
        LogRecord.builder()
            .timestamp(Instant.ofEpochSecond(1))
            .observedTimestamp(Instant.ofEpochSecond(-1))
            .body("x")
            .build();
    assertThat(exporter.export(List.of(bad), opts).get().rejected()).isEqualTo(1);
    assertThat(exporter.shutdown().get()).isNull();
    LokiOtlpExporter otlp = new LokiOtlpExporter(client);
    assertThat(otlp.export(List.of(), opts).get().accepted()).isZero();
    assertThat(
            otlp.export(List.of(r), new ExportOptions(true, Duration.ofSeconds(5)))
                .get()
                .accepted())
        .isEqualTo(1);
    assertThat(otlp.shutdown().get()).isNull();
  }
}
