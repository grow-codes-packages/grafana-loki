package codes.grow.packages.loki;

import codes.grow.packages.loki.core.ValidationException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Immutable LogQL query builder with safe escaping. Every method returns a new builder.
 *
 * <pre>{@code
 * LogQL.selector(Map.of("app", "x")).lineContains("err").json().where("status", ">=", 500).build()
 * // {app="x"} |= "err" | json | status >= 500
 * }</pre>
 *
 * <p>String literals are double-quoted with Go/JSON escaping (verified against Loki 3.7).
 */
public final class LogQL {
  private static final Set<String> MATCH_OPS = Set.of("=", "!=", "=~", "!~");
  private static final Set<String> NUMERIC_OPS = Set.of("==", "!=", ">", ">=", "<", "<=");

  private record Matcher(String name, String op, String value) {}

  private final List<Matcher> matchers;
  private final List<String> stages;

  private LogQL(List<Matcher> matchers, List<String> stages) {
    this.matchers = List.copyOf(matchers);
    this.stages = List.copyOf(stages);
  }

  /**
   * Starts a query from equality matchers ({@code {app="x", env="prod"}}, names sorted).
   *
   * @param labels label name → value
   * @return the builder
   */
  public static LogQL selector(Map<String, String> labels) {
    LogQL q = new LogQL(List.of(), List.of());
    for (Map.Entry<String, String> e : new TreeMap<>(labels).entrySet()) {
      q = q.label(e.getKey(), "=", e.getValue());
    }
    return q;
  }

  /**
   * Quotes a LogQL string literal with Go/JSON escaping ({@code "}, {@code \}, control characters);
   * other Unicode stays as is.
   *
   * @param value text
   * @return the quoted literal
   */
  public static String quote(String value) {
    StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20 || c == 0x7f) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.append('"').toString();
  }

  private static void checkName(String name, String what) {
    if (!Labels.matchesNamePattern(name)) {
      throw new ValidationException(
          "logql: invalid "
              + what
              + " "
              + quote(String.valueOf(name))
              + ": must match [a-zA-Z_][a-zA-Z0-9_]*");
    }
  }

  /**
   * Adds a stream-selector matcher.
   *
   * @param name label name
   * @param op {@code =}, {@code !=}, {@code =~} or {@code !~}
   * @param value value or RE2 regex
   * @return a new builder
   */
  public LogQL label(String name, String op, String value) {
    checkName(name, "label name");
    if (!MATCH_OPS.contains(op)) {
      throw new ValidationException("logql: invalid matcher operator " + op);
    }
    List<Matcher> m = new ArrayList<>(matchers);
    m.add(new Matcher(name, op, value));
    return new LogQL(m, stages);
  }

  private LogQL stage(String text) {
    List<String> s = new ArrayList<>(stages);
    s.add(text);
    return new LogQL(matchers, s);
  }

  /**
   * Line filter {@code |= "text"} (case-sensitive substring).
   *
   * @param text text
   * @return a new builder
   */
  public LogQL lineContains(String text) {
    return stage("|= " + quote(text));
  }

  /**
   * Line filter {@code != "text"}.
   *
   * @param text text
   * @return a new builder
   */
  public LogQL lineNotContains(String text) {
    return stage("!= " + quote(text));
  }

  /**
   * Line filter {@code |~ "regex"} (RE2; prefix {@code (?i)} for case-insensitive).
   *
   * @param regex regex
   * @return a new builder
   */
  public LogQL lineMatches(String regex) {
    return stage("|~ " + quote(regex));
  }

  /**
   * Line filter {@code !~ "regex"}.
   *
   * @param regex regex
   * @return a new builder
   */
  public LogQL lineNotMatches(String regex) {
    return stage("!~ " + quote(regex));
  }

  /**
   * Parser stage {@code | json}.
   *
   * @return a new builder
   */
  public LogQL json() {
    return stage("| json");
  }

  /**
   * Parser stage {@code | logfmt}.
   *
   * @return a new builder
   */
  public LogQL logfmt() {
    return stage("| logfmt");
  }

  /**
   * Parser stage {@code | unpack}.
   *
   * @return a new builder
   */
  public LogQL unpack() {
    return stage("| unpack");
  }

  /**
   * Parser stage {@code | regexp "<re>"} with named capture groups.
   *
   * @param pattern RE2 pattern
   * @return a new builder
   */
  public LogQL regexp(String pattern) {
    return stage("| regexp " + quote(pattern));
  }

  /**
   * Parser stage {@code | pattern "<pattern>"}.
   *
   * @param pattern pattern expression
   * @return a new builder
   */
  public LogQL pattern(String pattern) {
    return stage("| pattern " + quote(pattern));
  }

  /**
   * Formatting stage {@code | line_format "<template>"}.
   *
   * @param template Go template
   * @return a new builder
   */
  public LogQL lineFormat(String template) {
    return stage("| line_format " + quote(template));
  }

  /**
   * Label filter on an extracted label or structured-metadata key, comparing strings with {@code
   * =}, {@code !=}, {@code =~}, {@code !~}.
   *
   * @param field label name
   * @param op operator
   * @param value value
   * @return a new builder
   */
  public LogQL where(String field, String op, String value) {
    checkName(field, "field name");
    if (!MATCH_OPS.contains(op)) {
      throw new ValidationException("logql: operator " + op + " cannot compare strings");
    }
    return stage("| " + field + " " + op + " " + quote(value));
  }

  /**
   * Numeric label filter with {@code ==}, {@code !=}, {@code >}, {@code >=}, {@code <}, {@code <=}.
   *
   * @param field label name
   * @param op operator
   * @param value finite number
   * @return a new builder
   */
  public LogQL where(String field, String op, double value) {
    checkName(field, "field name");
    if (!NUMERIC_OPS.contains(op)) {
      throw new ValidationException("logql: operator " + op + " cannot compare numbers");
    }
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new ValidationException("logql: numbers must be finite");
    }
    String number = BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    return stage("| " + field + " " + op + " " + number);
  }

  /**
   * Appends a raw pipeline stage ({@code | <stage>}), e.g. {@code drop level}.
   *
   * @param stage stage text without the leading pipe
   * @return a new builder
   */
  public LogQL pipe(String stage) {
    return stage("| " + stage.trim());
  }

  private static boolean matchesEmpty(Matcher m) {
    switch (m.op()) {
      case "=":
        return m.value().isEmpty();
      case "!=":
        return !m.value().isEmpty();
      default:
        boolean match;
        try {
          match = Pattern.compile("(?:" + m.value() + ")").matcher("").matches();
        } catch (PatternSyntaxException e) {
          return false; // the server reports RE2 syntax problems
        }
        return m.op().equals("=~") == match;
    }
  }

  /**
   * Renders the query.
   *
   * @return LogQL text
   * @throws ValidationException when the selector is empty or every matcher also matches the empty
   *     value (Loki rejects such selectors)
   */
  public String build() {
    if (matchers.isEmpty()) {
      throw new ValidationException("logql: a stream selector needs at least one matcher");
    }
    if (matchers.stream().allMatch(LogQL::matchesEmpty)) {
      throw new ValidationException(
          "logql: the selector needs at least one matcher that does not match the empty value");
    }
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < matchers.size(); i++) {
      Matcher m = matchers.get(i);
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(m.name()).append(m.op()).append(quote(m.value()));
    }
    sb.append('}');
    for (String s : stages) {
      sb.append(' ').append(s);
    }
    return sb.toString();
  }

  /**
   * Same as {@link #build()}.
   *
   * @return LogQL text
   */
  @Override
  public String toString() {
    return build();
  }
}
