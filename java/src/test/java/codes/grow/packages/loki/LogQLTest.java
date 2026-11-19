package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.core.ValidationException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogQLTest {
  @Test
  void ticketExample() {
    assertThat(
            LogQL.selector(Map.of("app", "x"))
                .lineContains("err")
                .json()
                .where("status", ">=", 500)
                .build())
        .isEqualTo("{app=\"x\"} |= \"err\" | json | status >= 500");
  }

  @Test
  void allStages() {
    String q =
        LogQL.selector(Map.of("b", "2", "a", "1"))
            .label("env", "!=", "dev")
            .label("pod", "=~", "api-.*")
            .label("ns", "!~", "kube-.*")
            .lineContains("a")
            .lineNotContains("b")
            .lineMatches("(?i)c")
            .lineNotMatches("d")
            .json()
            .logfmt()
            .unpack()
            .regexp("(?P<x>\\w+)")
            .pattern("<ip> - <_>")
            .lineFormat("{{.msg}}")
            .where("level", "=", "error")
            .where("lat", "<", 0.25)
            .where("n", "==", 3)
            .pipe(" drop level ")
            .toString();
    assertThat(q)
        .isEqualTo(
            "{a=\"1\", b=\"2\", env!=\"dev\", pod=~\"api-.*\", ns!~\"kube-.*\"} |= \"a\" != \"b\""
                + " |~ \"(?i)c\" !~ \"d\" | json | logfmt | unpack | regexp \"(?P<x>\\\\w+)\""
                + " | pattern \"<ip> - <_>\" | line_format \"{{.msg}}\" | level = \"error\""
                + " | lat < 0.25 | n == 3 | drop level");
  }

  @Test
  void escaping() {
    assertThat(LogQL.quote("q\"b\\n\nt\tr\rü😀\u0001\u007f"))
        .isEqualTo("\"q\\\"b\\\\n\\nt\\tr\\rü😀\\u0001\\u007f\"");
  }

  @Test
  void validation() {
    assertThatThrownBy(() -> LogQL.selector(Map.of()).build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("at least one matcher");
    assertThatThrownBy(() -> LogQL.selector(Map.of("a", "")).build())
        .hasMessageContaining("empty value");
    assertThatThrownBy(() -> LogQL.selector(Map.of()).label("a", "=~", ".*").build())
        .hasMessageContaining("empty value");
    assertThatThrownBy(() -> LogQL.selector(Map.of()).label("a", "!=", "x").build())
        .hasMessageContaining("empty value");
    assertThatThrownBy(() -> LogQL.selector(Map.of()).label("a", "!~", "x").build())
        .hasMessageContaining("empty value");
    assertThat(LogQL.selector(Map.of()).label("a", "=~", ".+").build()).isEqualTo("{a=~\".+\"}");
    assertThat(LogQL.selector(Map.of()).label("a", "!~", ".*").build()).isEqualTo("{a!~\".*\"}");
    assertThat(LogQL.selector(Map.of()).label("a", "=~", "(").build()).isEqualTo("{a=~\"(\"}");
    assertThat(LogQL.selector(Map.of()).label("a", "!=", "").build()).isEqualTo("{a!=\"\"}");
    assertThatThrownBy(() -> LogQL.selector(Map.of("bad-name", "x")))
        .hasMessageContaining("label name");
    assertThatThrownBy(() -> LogQL.selector(Map.of()).label("a", "==", "x"))
        .hasMessageContaining("operator");
    assertThatThrownBy(() -> LogQL.selector(Map.of("a", "b")).where("x", ">", "s"))
        .hasMessageContaining("strings");
    assertThatThrownBy(() -> LogQL.selector(Map.of("a", "b")).where("x", "=~", 1))
        .hasMessageContaining("numbers");
    assertThatThrownBy(() -> LogQL.selector(Map.of("a", "b")).where("x", ">", Double.NaN))
        .hasMessageContaining("finite");
    assertThatThrownBy(
            () -> LogQL.selector(Map.of("a", "b")).where("x", ">", Double.NEGATIVE_INFINITY))
        .hasMessageContaining("finite");
    assertThatThrownBy(() -> LogQL.selector(Map.of("a", "b")).where("1x", "=", "s"))
        .hasMessageContaining("field name");
  }
}
