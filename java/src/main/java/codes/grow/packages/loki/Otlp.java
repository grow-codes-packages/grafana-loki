package codes.grow.packages.loki;

import codes.grow.packages.loki.core.SdkInfo;
import codes.grow.packages.loki.logging.AttrValues;
import codes.grow.packages.loki.logging.LogRecord;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** OTLP/JSON encoding of log records ({@code ExportLogsServiceRequest}). */
final class Otlp {
  /** Instrumentation scope name used in OTLP payloads. */
  static final String SCOPE_NAME = "grow-codes-grafana-loki";

  private Otlp() {}

  /** One {@code resourceLogs} entry per distinct resource, in first-seen order. */
  static Map<String, Object> request(List<LogRecord> records) {
    Map<String, List<Object>> logsByResource = new LinkedHashMap<>();
    Map<String, Map<String, Object>> resources = new LinkedHashMap<>();
    for (LogRecord r : records) {
      String key = new TreeMap<>(r.resource()).toString();
      resources.putIfAbsent(key, r.resource());
      Map<String, Object> lr = new LinkedHashMap<>();
      lr.put("timeUnixNano", Long.toString(Nanos.of(r.timestamp(), "timestamp")));
      r.observedTimestamp()
          .ifPresent(
              t -> lr.put("observedTimeUnixNano", Long.toString(Nanos.of(t, "observedTimestamp"))));
      lr.put("severityNumber", r.severity().number());
      lr.put("severityText", r.severityText());
      lr.put("body", Map.of("stringValue", r.body()));
      lr.put("attributes", attributes(r.attributes()));
      r.traceId().ifPresent(t -> lr.put("traceId", t.toLowerCase(Locale.ROOT)));
      r.spanId().ifPresent(s -> lr.put("spanId", s.toLowerCase(Locale.ROOT)));
      r.traceFlags().ifPresent(f -> lr.put("flags", f));
      logsByResource.computeIfAbsent(key, k -> new ArrayList<>()).add(lr);
    }
    List<Object> rls = new ArrayList<>();
    for (Map.Entry<String, List<Object>> e : logsByResource.entrySet()) {
      Map<String, Object> scope = new LinkedHashMap<>();
      scope.put("scope", Map.of("name", SCOPE_NAME, "version", SdkInfo.VERSION));
      scope.put("logRecords", e.getValue());
      Map<String, Object> rl = new LinkedHashMap<>();
      rl.put("resource", Map.of("attributes", attributes(resources.get(e.getKey()))));
      rl.put("scopeLogs", List.of(scope));
      rls.add(rl);
    }
    return Map.of("resourceLogs", rls);
  }

  static List<Object> attributes(Map<String, ?> attrs) {
    List<Object> out = new ArrayList<>();
    for (Map.Entry<String, ?> e : new TreeMap<String, Object>(attrs).entrySet()) {
      Map<String, Object> kv = new LinkedHashMap<>();
      kv.put("key", e.getKey());
      kv.put("value", anyValue(AttrValues.normalize(e.getValue())));
      out.add(kv);
    }
    return out;
  }

  static Map<String, Object> anyValue(Object v) {
    if (v instanceof Boolean) {
      return Map.of("boolValue", v);
    }
    if (v instanceof Long) {
      return Map.of("intValue", v.toString());
    }
    if (v instanceof Double) {
      return Map.of("doubleValue", v);
    }
    if (v instanceof List) {
      List<Object> values = new ArrayList<>();
      for (Object item : (List<?>) v) {
        values.add(anyValue(item));
      }
      return Map.of("arrayValue", Map.of("values", values));
    }
    if (v instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, ?> m = (Map<String, ?>) v;
      return Map.of("kvlistValue", Map.of("values", attributes(m)));
    }
    return Map.of("stringValue", String.valueOf(v));
  }
}
