package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.core.AuthenticationException;
import codes.grow.packages.loki.core.CancelledException;
import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.core.TlsOptions;
import codes.grow.packages.loki.core.UnavailableException;
import codes.grow.packages.loki.core.ValidationException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TailTest {
  static final String FRAME =
      FakeLoki.load("tail.categorized-labels").get("response").get("frame").toString();

  @Test
  void framesHeadersAndCallbacks() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    try (FakeWs ws =
        FakeWs.accept(
            conn -> {
              conn.ping();
              conn.binary(new byte[] {1});
              conn.fragmented(FRAME);
              conn.text(
                  "{\"streams\":[],\"dropped_entries\":[{\"labels\":{\"a\":\"b\"},\"timestamp\":\"5\"}]}");
              conn.text("not json");
              conn.text(FRAME); // the frame callback throws: ignored
              release.await(5, TimeUnit.SECONDS);
              conn.close(1011, "internal boom");
              conn.awaitClose();
            })) {
      LokiClient c =
          LokiClient.builder()
              .url(ws.url())
              .tenantId("team-a")
              .bearerToken("tok-123456")
              .header("X-Extra", "1")
              .build();
      LinkedBlockingQueue<TailFrame> frames = new LinkedBlockingQueue<>();
      LinkedBlockingQueue<LokiException> errors = new LinkedBlockingQueue<>();
      LinkedBlockingQueue<List<DroppedEntry>> dropped = new LinkedBlockingQueue<>();
      LokiTail tail =
          c.tail(
              "{run=\"x\"}",
              f -> {
                frames.add(f);
                if (frames.size() > 1) {
                  throw new IllegalStateException("callback failure");
                }
              },
              LiveTailOptions.builder()
                  .start(Instant.ofEpochSecond(1, 5))
                  .limit(10)
                  .delayFor(Duration.ofMillis(1600))
                  .onDropped(
                      d -> {
                        dropped.add(d);
                        throw new IllegalStateException("ignored");
                      })
                  .onError(errors::add)
                  .build());
      Map<String, String> h = ws.handshake();
      assertThat(h).containsEntry("x-scope-orgid", "team-a");
      assertThat(h).containsEntry("authorization", "Bearer tok-123456");
      assertThat(h).containsEntry("x-loki-response-encoding-flags", "categorize-labels");
      assertThat(h).containsEntry("x-extra", "1");
      assertThat(h.get("user-agent")).startsWith("grow-codes-grafana-loki/");
      assertThat(ws.requestLine())
          .startsWith("GET /loki/api/v1/tail?query=")
          .contains("start=1000000005")
          .contains("limit=10")
          .contains("delay_for=2");
      TailFrame f = frames.poll(5, TimeUnit.SECONDS);
      assertThat(f).isNotNull();
      assertThat(f.streams().get(0).entries().get(0).structuredMetadata())
          .containsKey("request_id");
      assertThat(dropped.poll(5, TimeUnit.SECONDS)).hasSize(1);
      LokiException malformed = errors.poll(5, TimeUnit.SECONDS);
      assertThat(malformed)
          .isInstanceOf(LokiServerException.class)
          .hasMessageContaining("malformed");
      assertThat(frames.poll(5, TimeUnit.SECONDS)).isNotNull();
      assertThat(tail.droppedCount()).isEqualTo(1);
      assertThat(tail.isOpen()).isTrue();
      release.countDown();
      LokiException closed = errors.poll(5, TimeUnit.SECONDS);
      assertThat(closed)
          .isInstanceOf(UnavailableException.class)
          .hasMessageContaining("code 1011")
          .hasMessageContaining("internal boom");
      tail.closed().get(5, TimeUnit.SECONDS);
      assertThat(tail.isOpen()).isFalse();
      tail.close();
      c.close();
    }
  }

  @Test
  void normalServerCloseAndClientClose() throws Exception {
    try (FakeWs ws = FakeWs.accept(conn -> conn.close(1000, ""))) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      LinkedBlockingQueue<LokiException> errors = new LinkedBlockingQueue<>();
      LokiTail tail =
          c.tail("{a=\"b\"}", f -> {}, LiveTailOptions.builder().onError(errors::add).build());
      tail.closed().get(5, TimeUnit.SECONDS);
      assertThat(errors).isEmpty();
      c.close();
    }
    LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<>();
    try (FakeWs ws = FakeWs.accept(conn -> seen.add(conn.awaitClose() ? "close" : "eof"))) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      LokiTail tail =
          c.tailAsync("{a=\"b\"}", f -> {}, LiveTailOptions.defaults()).get(5, TimeUnit.SECONDS);
      c.close(); // stops open tails
      assertThat(seen.poll(5, TimeUnit.SECONDS)).isEqualTo("close");
      tail.closed().get(5, TimeUnit.SECONDS);
      assertThatThrownBy(() -> c.tail("{a=\"b\"}", f -> {})).isInstanceOf(CancelledException.class);
    }
    // the server drops the TCP connection without a close frame
    try (FakeWs ws = FakeWs.accept(conn -> {})) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      LinkedBlockingQueue<LokiException> errors = new LinkedBlockingQueue<>();
      LokiTail tail =
          c.tail("{a=\"b\"}", f -> {}, LiveTailOptions.builder().onError(errors::add).build());
      tail.closed().get(5, TimeUnit.SECONDS);
      assertThat(errors.poll(5, TimeUnit.SECONDS)).isInstanceOf(UnavailableException.class);
      c.close();
    }
  }

  @Test
  void handshakeErrors() throws Exception {
    try (FakeWs ws = FakeWs.reject(401, "no org id")) {
      LokiClient c = LokiClient.builder().url(ws.url()).basicAuth("u", "pw-123456").build();
      assertThatThrownBy(() -> c.tail("{a=\"b\"}", f -> {}))
          .isInstanceOf(AuthenticationException.class)
          .hasMessageContaining("HTTP 401");
      c.close();
    }
    try (FakeWs ws = FakeWs.reject(400, "delay_for can't be greater than 5")) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      assertThatThrownBy(() -> c.tail("{a=\"b\"}", f -> {}))
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("HTTP 400");
      c.close();
    }
    try (FakeWs ws = FakeWs.reject(500, "multiple org IDs present")) {
      LokiClient c = LokiClient.builder().url(ws.url()).build();
      assertThatThrownBy(() -> c.tail("{a=\"b\"}", f -> {}))
          .isInstanceOfSatisfying(
              UnavailableException.class, e -> assertThat(e.retryable()).isTrue());
      c.close();
    }
    int port;
    try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
      port = s.getLocalPort();
    }
    for (LokiClient c :
        List.of(
            LokiClient.builder().url("http://127.0.0.1:" + port).build(),
            LokiClient.builder()
                .url("https://127.0.0.1:" + port)
                .tls(TlsOptions.builder().insecureSkipVerify(true).build())
                .build(),
            LokiClient.builder()
                .url("http://127.0.0.1:" + port)
                .proxy("http://127.0.0.1:" + port)
                .build(),
            LokiClient.builder()
                .url("http://127.0.0.1:" + port)
                .proxy("https://127.0.0.1")
                .build())) {
      assertThatThrownBy(() -> c.tail("{a=\"b\"}", f -> {})).isInstanceOf(LokiException.class);
      c.close();
    }
  }

  @Test
  void validation() {
    LokiClient c = LokiClient.builder().build();
    assertThatThrownBy(() -> c.tail("", f -> {})).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> c.tail("rate({a=\"b\"}[1m])", f -> {}))
        .hasMessageContaining("only log queries");
    assertThatThrownBy(
            () ->
                c.tail(
                    "{a=\"b\"}",
                    f -> {},
                    LiveTailOptions.builder().delayFor(Duration.ofSeconds(6)).build()))
        .hasMessageContaining("delayFor");
    assertThatThrownBy(
            () ->
                c.tail(
                    "{a=\"b\"}",
                    f -> {},
                    LiveTailOptions.builder().delayFor(Duration.ofSeconds(-1)).build()))
        .hasMessageContaining("delayFor");
    assertThatThrownBy(
            () -> c.tail("{a=\"b\"}", f -> {}, LiveTailOptions.builder().limit(0).build()))
        .hasMessageContaining("limit");
    c.close();
  }
}
