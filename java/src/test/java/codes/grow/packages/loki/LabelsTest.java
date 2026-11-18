package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.core.ValidationException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LabelsTest {
  @Test
  void names() {
    assertThat(Labels.isValidLabelName("service_name")).isTrue();
    assertThat(Labels.isValidLabelName("_x1")).isTrue();
    assertThat(Labels.isValidLabelName("1x")).isFalse();
    assertThat(Labels.isValidLabelName("a-b")).isFalse();
    assertThat(Labels.isValidLabelName("__name__")).isFalse();
    assertThat(Labels.isValidLabelName("")).isFalse();
    assertThat(Labels.isValidLabelName(null)).isFalse();
    assertThat(Labels.isValidLabelName("Zz9")).isTrue();
    assertThat(Labels.isValidLabelName("a.b")).isFalse();
    assertThatThrownBy(() -> Labels.validateLabelName("a b"))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> Labels.validateLabel("app", null))
        .isInstanceOf(ValidationException.class);
    Labels.validateLabel("app", "v");
  }

  @Test
  void normalize() {
    assertThat(Labels.normalizeLabelName("service.name")).isEqualTo("service_name");
    assertThat(Labels.normalizeLabelName("k8s-pod/name")).isEqualTo("k8s_pod_name");
    assertThat(Labels.normalizeLabelName("1abc")).isEqualTo("_1abc");
    assertThat(Labels.normalizeLabelName("")).isEmpty();
    assertThat(Labels.normalizeLabelName(null)).isEmpty();
    assertThat(Labels.normalizeLabelName("AZaz09_")).isEqualTo("AZaz09_");
  }

  @Test
  void labelSetAndAttrStrings() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("z", "q\"uote\\");
    m.put("a", "b");
    assertThat(Labels.formatLabelSet(m)).isEqualTo("{a=\"b\", z=\"q\\\"uote\\\\\"}");
    assertThat(Labels.formatLabelSet(Map.of())).isEqualTo("{}");
    assertThat(Labels.attrToString("s")).isEqualTo("s");
    assertThat(Labels.attrToString(null)).isEmpty();
    assertThat(Labels.attrToString(5)).isEqualTo("5");
    assertThat(Labels.attrToString(true)).isEqualTo("true");
    assertThat(Labels.attrToString(1.0)).isEqualTo("1");
    assertThat(Labels.attrToString(0.25)).isEqualTo("0.25");
    assertThat(Labels.attrToString(Double.NaN)).isEqualTo("NaN");
    assertThat(Labels.attrToString(Double.POSITIVE_INFINITY)).isEqualTo("Infinity");
    assertThat(Labels.attrToString(List.of("a", 1))).isEqualTo("[\"a\",1]");
    assertThat(Labels.attrToString(Map.of("k", "v"))).isEqualTo("{\"k\":\"v\"}");
  }

  @Test
  void guard() {
    CardinalityGuard g = CardinalityGuard.defaults();
    assertThat(g.allowed()).contains("service_name", "level");
    assertThat(g.isAllowed("run")).isFalse();
    assertThat(g.toString()).startsWith("CardinalityGuard[");
    LokiStream s =
        LokiStream.of(
            Map.of("service_name", "a", "run", "r1"),
            LokiEntry.of(null, "x", Map.of("run", "own")),
            LokiEntry.of("y"));
    LokiStream out = g.apply(s);
    assertThat(out.labels()).containsOnlyKeys("service_name");
    assertThat(out.entries().get(0).structuredMetadata()).containsEntry("run", "own");
    assertThat(out.entries().get(1).structuredMetadata()).containsEntry("run", "r1");
    LokiStream kept = LokiStream.of(Map.of("service_name", "a"), LokiEntry.of("x"));
    assertThat(g.apply(kept)).isSameAs(kept);
    Map<String, String> labels = new LinkedHashMap<>();
    Map<String, String> md = new LinkedHashMap<>();
    Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("service.name", "api");
    attrs.put("level", "");
    attrs.put("", "dropped");
    attrs.put("http.status", 500);
    g.split(attrs, labels, md);
    assertThat(labels).containsExactly(Map.entry("service_name", "api"));
    assertThat(md).containsEntry("level", "").containsEntry("http_status", "500").hasSize(2);
    CardinalityGuard reserved = new CardinalityGuard(List.of("__x"));
    Map<String, String> l2 = new LinkedHashMap<>();
    Map<String, String> m2 = new LinkedHashMap<>();
    reserved.split(Map.of("__x", "v"), l2, m2);
    assertThat(l2).isEmpty();
    assertThat(m2).containsKey("__x");
  }

  @Test
  void records() {
    assertThatThrownBy(() -> new LokiEntry(null, null, null))
        .isInstanceOf(NullPointerException.class);
    assertThat(LokiEntry.of("x").structuredMetadata()).isEmpty();
    assertThat(new LokiStream(null, null).labels()).isEmpty();
    assertThat(TimeRange.defaults().start()).isEmpty();
    assertThat(new TimeRange(null, null).end()).isEmpty();
    assertThat(TimeRange.last(java.time.Duration.ofMinutes(5)).start()).isPresent();
    assertThat(new LabelsOptions(null, null, null).query()).isEmpty();
    assertThat(Direction.FORWARD.wireValue()).isEqualTo("forward");
    assertThat(VolumeOptions.AggregateBy.SERIES.wireValue()).isEqualTo("series");
    assertThat(QueryOptions.defaults().time()).isEmpty();
    assertThat(QueryOptions.defaults().limit()).isEmpty();
    assertThat(QueryOptions.defaults().direction()).isEmpty();
    QueryRangeOptions q = QueryRangeOptions.defaults();
    assertThat(q.start()).isEmpty();
    assertThat(q.end()).isEmpty();
    assertThat(q.step()).isEmpty();
    assertThat(q.interval()).isEmpty();
    assertThat(q.limit()).isEmpty();
    assertThat(q.direction()).isEmpty();
    VolumeOptions v = VolumeOptions.defaults();
    assertThat(v.start()).isEmpty();
    assertThat(v.end()).isEmpty();
    assertThat(v.limit()).isEmpty();
    assertThat(v.targetLabels()).isEmpty();
    assertThat(v.aggregateBy()).isEmpty();
    assertThat(v.step()).isEmpty();
    LiveTailOptions t = LiveTailOptions.builder().onDropped(null).onError(null).build();
    assertThat(t.start()).isEmpty();
    assertThat(t.limit()).isEmpty();
    assertThat(t.delayFor()).isEmpty();
    t.onDropped().accept(List.of());
    t.onError().accept(null);
    assertThat(
            new QueryResult(ResultType.SCALAR, List.of(), List.of(), List.of(), null, null).stats())
        .isEmpty();
    LokiLogClientOptions o = LokiLogClientOptions.defaults();
    assertThat(o.protocol()).isEqualTo(LogProtocol.JSON);
    assertThat(o.batchOptions()).isNotNull();
    assertThat(o.resource()).isEmpty();
    assertThat(o.redaction()).isNotNull();
    assertThat(o.allowedLabels()).isEmpty();
  }
}
