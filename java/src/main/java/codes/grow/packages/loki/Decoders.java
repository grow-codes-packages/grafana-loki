package codes.grow.packages.loki;

import codes.grow.packages.loki.core.Json;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Decoders of the Loki query API responses (lenient: unknown fields are ignored). */
final class Decoders {
  private Decoders() {}

  static LokiServerException bad(String what) {
    return new LokiServerException("unexpected response from Loki: " + what, null, null, null);
  }

  /** A JSON object of values as a string map (non-strings are JSON-encoded, nulls dropped). */
  static Map<String, String> stringMap(JsonNode node) {
    Map<String, String> out = new LinkedHashMap<>();
    if (node == null || !node.isObject()) {
      return out;
    }
    for (Map.Entry<String, JsonNode> e : node.properties()) {
      JsonNode v = e.getValue();
      if (v.isNull()) {
        continue;
      }
      out.put(e.getKey(), v.isTextual() ? v.asText() : v.toString());
    }
    return out;
  }

  static long parseNanos(JsonNode node) {
    String text = node == null ? "" : node.asText("");
    if (text.isEmpty() || !text.chars().allMatch(c -> c >= '0' && c <= '9')) {
      throw bad("malformed timestamp");
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      throw bad("malformed timestamp");
    }
  }

  static double parseValue(JsonNode v) {
    if (v.isNumber()) {
      return v.asDouble();
    }
    switch (v.asText("")) {
      case "+Inf":
      case "Inf":
        return Double.POSITIVE_INFINITY;
      case "-Inf":
        return Double.NEGATIVE_INFINITY;
      default:
        try {
          return Double.parseDouble(v.asText(""));
        } catch (NumberFormatException e) {
          return Double.NaN;
        }
    }
  }

  static Sample sample(JsonNode v) {
    if (v == null || !v.isArray() || v.size() < 2 || !v.get(0).isNumber()) {
      throw bad("malformed sample");
    }
    return new Sample(Nanos.fromSeconds(v.get(0).asDouble()), parseValue(v.get(1)));
  }

  /** Decodes one {@code [ns, line, {structuredMetadata, parsed}?]} value. */
  static StreamEntry entry(JsonNode v) {
    if (v == null
        || !v.isArray()
        || v.size() < 2
        || !v.get(0).isTextual()
        || !v.get(1).isTextual()) {
      throw bad("malformed stream value");
    }
    long ns = parseNanos(v.get(0));
    JsonNode extra = v.get(2);
    return new StreamEntry(
        ns,
        Nanos.toInstant(ns),
        v.get(1).asText(),
        stringMap(extra == null ? null : extra.get("structuredMetadata")),
        stringMap(extra == null ? null : extra.get("parsed")));
  }

  static List<StreamResult> streams(JsonNode v) {
    List<StreamResult> out = new ArrayList<>();
    if (v == null || v.isNull()) {
      return out;
    }
    if (!v.isArray()) {
      throw bad("streams is not a list");
    }
    for (JsonNode s : v) {
      if (!s.isObject()) {
        throw bad("malformed stream");
      }
      List<StreamEntry> entries = new ArrayList<>();
      for (JsonNode value : s.path("values")) {
        entries.add(entry(value));
      }
      out.add(new StreamResult(stringMap(s.get("stream")), entries));
    }
    return out;
  }

  /** Decodes the {@code {status, data: {resultType, result, stats}}} envelope. */
  static QueryResult queryResult(JsonNode body) {
    JsonNode data = body == null ? null : body.get("data");
    if (data == null || !data.isObject()) {
      throw bad("missing data");
    }
    JsonNode result = data.get("result");
    JsonNode statsNode = data.get("stats");
    Map<String, Object> stats =
        statsNode != null && statsNode.isObject()
            ? Json.mapper()
                .convertValue(
                    statsNode,
                    Json.mapper()
                        .getTypeFactory()
                        .constructMapType(LinkedHashMap.class, String.class, Object.class))
            : Map.of();
    String type = data.path("resultType").asText("");
    List<StreamResult> streams = List.of();
    List<VectorSample> vector = new ArrayList<>();
    List<MatrixSeries> matrix = new ArrayList<>();
    Optional<Sample> scalar = Optional.empty();
    ResultType rt;
    switch (type) {
      case "streams":
        rt = ResultType.STREAMS;
        streams = streams(result);
        break;
      case "vector":
        rt = ResultType.VECTOR;
        for (JsonNode r : list(result)) {
          Sample s = sample(r.get("value"));
          vector.add(new VectorSample(stringMap(r.get("metric")), s.timestamp(), s.value()));
        }
        break;
      case "matrix":
        rt = ResultType.MATRIX;
        for (JsonNode r : list(result)) {
          List<Sample> samples = new ArrayList<>();
          for (JsonNode p : r.path("values")) {
            samples.add(sample(p));
          }
          matrix.add(new MatrixSeries(stringMap(r.get("metric")), samples));
        }
        break;
      case "scalar":
        rt = ResultType.SCALAR;
        scalar = Optional.of(sample(result));
        break;
      default:
        throw bad("unknown resultType " + type);
    }
    return new QueryResult(rt, streams, vector, matrix, scalar, stats);
  }

  private static List<JsonNode> list(JsonNode v) {
    List<JsonNode> out = new ArrayList<>();
    if (v != null && v.isArray()) {
      for (JsonNode r : v) {
        if (!r.isObject()) {
          throw bad("malformed series");
        }
        out.add(r);
      }
    }
    return out;
  }

  /** Decodes {@code {status, data: [...]}}; a missing {@code data} (empty result) is empty. */
  static List<String> stringList(JsonNode body) {
    JsonNode data = body.get("data");
    List<String> out = new ArrayList<>();
    if (data == null || data.isNull()) {
      return out;
    }
    if (!data.isArray()) {
      throw bad("data is not a list");
    }
    for (JsonNode n : data) {
      if (n.isTextual()) {
        out.add(n.asText());
      }
    }
    return out;
  }

  static List<Map<String, String>> series(JsonNode body) {
    JsonNode data = body.get("data");
    List<Map<String, String>> out = new ArrayList<>();
    if (data == null || data.isNull()) {
      return out;
    }
    if (!data.isArray()) {
      throw bad("data is not a list");
    }
    for (JsonNode n : data) {
      out.add(Map.copyOf(stringMap(n)));
    }
    return out;
  }

  static IndexStats indexStats(JsonNode body) {
    return new IndexStats(
        body.path("streams").asLong(0),
        body.path("chunks").asLong(0),
        body.path("bytes").asLong(0),
        body.path("entries").asLong(0));
  }

  static BuildInfo buildInfo(JsonNode body) {
    return new BuildInfo(
        body.path("version").asText(""),
        body.path("revision").asText(""),
        body.path("branch").asText(""),
        body.path("buildUser").asText(""),
        body.path("buildDate").asText(""),
        body.path("goVersion").asText(""));
  }

  static TailFrame tailFrame(JsonNode body) {
    if (body == null || !body.isObject()) {
      throw bad("malformed tail frame");
    }
    List<DroppedEntry> dropped = new ArrayList<>();
    for (JsonNode d : body.path("dropped_entries")) {
      if (d.isObject()) {
        dropped.add(new DroppedEntry(stringMap(d.get("labels")), d.path("timestamp").asLong(0)));
      }
    }
    return new TailFrame(streams(body.get("streams")), dropped);
  }
}
