package codes.grow.packages.loki;

import codes.grow.packages.loki.core.Json;
import codes.grow.packages.loki.core.ValidationException;
import codes.grow.packages.loki.logging.AttrValues;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Label helpers: validation, normalization and Prometheus label-set rendering.
 *
 * <p>Loki label names follow the Prometheus rules ({@code [a-zA-Z_][a-zA-Z0-9_]*}); names starting
 * with {@code __} are reserved.
 */
public final class Labels {
  /**
   * Keys the cardinality guard lets become index labels by default (coordinator decision 3). Every
   * other attribute or resource key goes to structured metadata.
   */
  public static final List<String> DEFAULT_ALLOWED_LABELS =
      List.of(
          "service_name",
          "service_namespace",
          "deployment_environment",
          "host_name",
          "level",
          "job",
          "instance",
          "namespace",
          "pod",
          "container");

  private Labels() {}

  /**
   * Returns whether {@code name} matches {@code [a-zA-Z_][a-zA-Z0-9_]*} and does not start with
   * {@code __}.
   *
   * @param name label name
   * @return true when Loki accepts it as an index label name
   */
  public static boolean isValidLabelName(String name) {
    return matchesNamePattern(name) && !name.startsWith("__");
  }

  /**
   * Returns whether {@code name} matches {@code [a-zA-Z_][a-zA-Z0-9_]*}.
   *
   * @param name name
   * @return true when it matches
   */
  static boolean matchesNamePattern(String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      boolean letter = c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
      boolean digit = c >= '0' && c <= '9';
      if (!(letter || (digit && i > 0))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks one index label: valid, non-reserved name and a non-empty value.
   *
   * @param name label name
   * @param value label value
   * @throws ValidationException when the label is invalid
   */
  public static void validateLabel(String name, String value) {
    validateLabelName(name);
    if (value == null || value.isEmpty()) {
      throw new ValidationException("label " + name + ": value must be a non-empty string");
    }
  }

  /**
   * Checks a label name.
   *
   * @param name label name
   * @throws ValidationException unless the name is valid and not reserved
   */
  public static void validateLabelName(String name) {
    if (!matchesNamePattern(name)) {
      throw new ValidationException(
          "invalid label name " + Json.stringify(name) + ": must match [a-zA-Z_][a-zA-Z0-9_]*");
    }
    if (name.startsWith("__")) {
      throw new ValidationException(
          "invalid label name " + Json.stringify(name) + ": names starting with __ are reserved");
    }
  }

  /**
   * Normalizes an attribute key the way Loki does for OTLP attributes: every character outside
   * {@code [a-zA-Z0-9_]} becomes {@code _} ({@code service.name} → {@code service_name}) and a
   * leading digit gets a {@code _} prefix. An empty key stays empty.
   *
   * @param key attribute key
   * @return the label / structured metadata name
   */
  public static String normalizeLabelName(String key) {
    if (key == null || key.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder(key.length() + 1);
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      boolean ok =
          c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
      sb.append(ok ? c : '_');
    }
    if (sb.charAt(0) >= '0' && sb.charAt(0) <= '9') {
      sb.insert(0, '_');
    }
    return sb.toString();
  }

  /**
   * Renders labels in Prometheus label-set syntax with sorted names and Go-quoted values: {@code
   * {a="b", c="d"}} (the form protobuf push and {@code listStreams()} use).
   *
   * @param labels labels
   * @return the label-set string
   */
  public static String formatLabelSet(Map<String, String> labels) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, String> e : new TreeMap<>(labels).entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append('=').append(LogQL.quote(e.getValue()));
    }
    return sb.append('}').toString();
  }

  /**
   * Renders an attribute value as a structured metadata string: strings as is, numbers and booleans
   * as their text, lists and maps as JSON.
   *
   * @param value attribute value
   * @return the string form ({@code ""} for null)
   */
  public static String attrToString(Object value) {
    Object v = AttrValues.normalize(value);
    if (v instanceof String) {
      return (String) v;
    }
    if (v instanceof Double) {
      double d = (Double) v;
      if (Double.isNaN(d) || Double.isInfinite(d)) {
        return Double.toString(d);
      }
      return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
    }
    if (v instanceof Number || v instanceof Boolean) {
      return v.toString();
    }
    return Json.stringify(v);
  }
}
