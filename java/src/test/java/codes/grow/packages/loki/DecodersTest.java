package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.core.Json;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DecodersTest {
  static JsonNode j(String s) {
    return Json.tree(s.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void entriesAndStreams() {
    StreamEntry e =
        Decoders.entry(
            j(
                "[\"1795035084588079388\",\"line\",{\"structuredMetadata\":{\"a\":\"1\",\"n\":2,\"z\":null},\"parsed\":{\"p\":\"v\"}}]"));
    assertThat(e.timestampNanos()).isEqualTo(1795035084588079388L);
    assertThat(e.timestamp()).isEqualTo(Instant.ofEpochSecond(1795035084L, 588079388L));
    assertThat(e.structuredMetadata())
        .containsEntry("a", "1")
        .containsEntry("n", "2")
        .doesNotContainKey("z");
    assertThat(e.parsed()).containsEntry("p", "v");
    assertThat(Decoders.entry(j("[\"1\",\"l\"]")).structuredMetadata()).isEmpty();
    assertThatThrownBy(() -> Decoders.entry(j("[1,\"l\"]")))
        .isInstanceOf(LokiServerException.class);
    assertThatThrownBy(() -> Decoders.entry(j("[\"x\",\"l\"]"))).hasMessageContaining("timestamp");
    assertThatThrownBy(() -> Decoders.entry(j("[\"\",\"l\"]"))).hasMessageContaining("timestamp");
    assertThatThrownBy(() -> Decoders.entry(j("[\"99999999999999999999\",\"l\"]")))
        .hasMessageContaining("timestamp");
    assertThatThrownBy(() -> Decoders.entry(j("[\"1\"]"))).hasMessageContaining("value");
    assertThatThrownBy(() -> Decoders.entry(j("[\"1\",2]"))).hasMessageContaining("value");
    assertThatThrownBy(() -> Decoders.entry(j("{}"))).hasMessageContaining("value");
    assertThatThrownBy(() -> Decoders.entry(null)).hasMessageContaining("value");
    assertThat(Decoders.streams(null)).isEmpty();
    assertThat(Decoders.streams(j("null"))).isEmpty();
    assertThatThrownBy(() -> Decoders.streams(j("{}"))).hasMessageContaining("not a list");
    assertThatThrownBy(() -> Decoders.streams(j("[1]"))).hasMessageContaining("malformed stream");
    assertThat(Decoders.streams(j("[{\"stream\":{\"a\":\"b\"}}]")).get(0).entries()).isEmpty();
    assertThat(Decoders.stringMap(j("[]"))).isEmpty();
  }

  @Test
  void queryResults() {
    assertThatThrownBy(() -> Decoders.queryResult(j("{}"))).hasMessageContaining("missing data");
    assertThatThrownBy(() -> Decoders.queryResult(null)).hasMessageContaining("missing data");
    assertThatThrownBy(() -> Decoders.queryResult(j("{\"data\":[]}")))
        .hasMessageContaining("missing data");
    assertThatThrownBy(() -> Decoders.queryResult(j("{\"data\":{\"resultType\":\"x\"}}")))
        .hasMessageContaining("unknown resultType");
    QueryResult v =
        Decoders.queryResult(
            j(
                "{\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{},\"value\":[1.5,\"+Inf\"]},{\"value\":[1,\"-Inf\"]},{\"value\":[1,\"Inf\"]},{\"value\":[1,\"x\"]},{\"value\":[1,3]}]}}"));
    assertThat(v.vector())
        .extracting(VectorSample::value)
        .containsExactly(
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NaN,
            3.0);
    assertThat(v.vector().get(0).timestamp()).isEqualTo(Instant.ofEpochMilli(1500));
    assertThat(v.stats()).isEmpty();
    assertThat(Decoders.queryResult(j("{\"data\":{\"resultType\":\"vector\"}}")).vector())
        .isEmpty();
    assertThatThrownBy(
            () -> Decoders.queryResult(j("{\"data\":{\"resultType\":\"vector\",\"result\":[1]}}")))
        .hasMessageContaining("series");
    assertThatThrownBy(
            () ->
                Decoders.queryResult(
                    j(
                        "{\"data\":{\"resultType\":\"vector\",\"result\":[{\"value\":[\"1\",\"2\"]}]}}")))
        .hasMessageContaining("sample");
    assertThatThrownBy(
            () ->
                Decoders.queryResult(
                    j("{\"data\":{\"resultType\":\"vector\",\"result\":[{\"value\":[1]}]}}")))
        .hasMessageContaining("sample");
    assertThatThrownBy(
            () ->
                Decoders.queryResult(
                    j("{\"data\":{\"resultType\":\"vector\",\"result\":[{\"value\":{}}]}}")))
        .hasMessageContaining("sample");
    assertThatThrownBy(
            () -> Decoders.queryResult(j("{\"data\":{\"resultType\":\"vector\",\"result\":[{}]}}")))
        .hasMessageContaining("sample");
    QueryResult m =
        Decoders.queryResult(
            j("{\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"a\":\"b\"}}]}}"));
    assertThat(m.matrix().get(0).samples()).isEmpty();
  }

  @Test
  void lists() {
    assertThat(Decoders.stringList(j("{\"data\":null}"))).isEmpty();
    assertThat(Decoders.stringList(j("{\"data\":[\"a\",1]}"))).containsExactly("a");
    assertThat(Decoders.series(j("{\"data\":null}"))).isEmpty();
    assertThat(Decoders.series(j("{}"))).isEmpty();
    assertThat(Decoders.indexStats(j("{}")).bytes()).isZero();
    assertThat(Decoders.buildInfo(j("{}")).version()).isEmpty();
  }

  @Test
  void tailFrames() {
    TailFrame f =
        Decoders.tailFrame(FakeLoki.load("tail.categorized-labels").get("response").get("frame"));
    assertThat(f.streams()).hasSize(1);
    assertThat(f.streams().get(0).entries().get(0).structuredMetadata()).containsKey("request_id");
    TailFrame d =
        Decoders.tailFrame(
            j("{\"dropped_entries\":[{\"labels\":{\"a\":\"b\"},\"timestamp\":\"12\"},1]}"));
    assertThat(d.streams()).isEmpty();
    assertThat(d.droppedEntries())
        .containsExactly(new DroppedEntry(java.util.Map.of("a", "b"), 12));
    assertThatThrownBy(() -> Decoders.tailFrame(j("[]"))).hasMessageContaining("tail frame");
    assertThatThrownBy(() -> Decoders.tailFrame(null)).hasMessageContaining("tail frame");
  }

  @Test
  void nanos() {
    assertThat(Nanos.of(Instant.ofEpochSecond(1, 2), "t")).isEqualTo(1_000_000_002L);
    assertThatThrownBy(() -> Nanos.of(Instant.ofEpochSecond(-1), "t")).hasMessageContaining("t:");
    assertThatThrownBy(() -> Nanos.of(Instant.ofEpochSecond(Long.MAX_VALUE / 1_000_000_000L), "t"))
        .hasMessageContaining("range");
    assertThat(Nanos.toInstant(-1)).isEqualTo(Instant.ofEpochSecond(-1, 999_999_999));
    assertThat(Nanos.fromSeconds(-0.5)).isEqualTo(Instant.ofEpochMilli(-500));
    assertThat(Nanos.duration(Duration.ofSeconds(2), "s")).isEqualTo("2000ms");
    assertThat(Nanos.duration(Duration.ofNanos(1), "s")).isEqualTo("1ms");
    assertThatThrownBy(() -> Nanos.duration(Duration.ofSeconds(-1), "s"))
        .hasMessageContaining("positive");
  }
}
