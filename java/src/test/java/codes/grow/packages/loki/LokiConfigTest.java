package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.loki.core.ConfigException;
import codes.grow.packages.loki.core.EnvReader;
import codes.grow.packages.loki.core.Hooks;
import codes.grow.packages.loki.core.Logger;
import codes.grow.packages.loki.core.RetryPolicy;
import codes.grow.packages.loki.core.Secret;
import codes.grow.packages.loki.core.TlsOptions;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LokiConfigTest {
  private static final String P = EnvReader.DEFAULT_PREFIX;

  @Test
  void defaults() {
    LokiConfig c = LokiConfig.builder().build();
    assertThat(c.url()).isEqualTo("http://127.0.0.1:3100");
    assertThat(c.tenantId()).isEmpty();
    assertThat(c.username()).isEmpty();
    assertThat(c.password()).isEmpty();
    assertThat(c.bearerToken()).isEmpty();
    assertThat(c.multiTenantQueries()).isFalse();
    assertThat(c.allowedLabels()).isEqualTo(Labels.DEFAULT_ALLOWED_LABELS);
    assertThat(c.disableLabelGuard()).isFalse();
    assertThat(c.connectTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(c.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
    assertThat(c.totalTimeout()).isEqualTo(Duration.ofSeconds(120));
    assertThat(c.retry().maxRetries()).isEqualTo(3);
    assertThat(c.tls().isCustom()).isFalse();
    assertThat(c.proxy()).isEmpty();
    assertThat(c.headers()).isEmpty();
    assertThat(c.userAgentSuffix()).isEmpty();
    assertThat(c.hooks()).isSameAs(Hooks.NONE);
    assertThat(c.logger()).isSameAs(Logger.NOOP);
    assertThat(c.authorizationHeader()).isEmpty();
    assertThat(c.knownSecrets()).isEmpty();
  }

  @Test
  void envPrecedence() {
    Map<String, String> env =
        Map.of(
            P + "URL",
            "http://grow:1",
            "LOKI_URL",
            "http://vendor:2",
            "LOKI_TENANT_ID",
            "team-a",
            "LOKI_BASIC_AUTH_USER",
            "user",
            "LOKI_BASIC_AUTH_PASSWORD",
            "pw-123456",
            P + "CONNECT_TIMEOUT",
            "2s",
            P + "REQUEST_TIMEOUT",
            "1500",
            P + "TOTAL_TIMEOUT",
            "1m",
            P + "MAX_RETRIES",
            "5",
            P + "PROXY",
            "http://proxy:3128");
    LokiConfig c = LokiConfig.fromEnv(env);
    assertThat(c.url()).isEqualTo("http://grow:1");
    assertThat(c.tenantId()).contains("team-a");
    assertThat(c.username()).contains("user");
    assertThat(c.password().orElseThrow().reveal()).isEqualTo("pw-123456");
    assertThat(c.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
    assertThat(c.requestTimeout()).isEqualTo(Duration.ofMillis(1500));
    assertThat(c.totalTimeout()).isEqualTo(Duration.ofMinutes(1));
    assertThat(c.retry().maxRetries()).isEqualTo(5);
    assertThat(c.proxy()).contains("http://proxy:3128");
    String basic = Base64.getEncoder().encodeToString("user:pw-123456".getBytes());
    assertThat(c.authorizationHeader()).contains("Basic " + basic);
    assertThat(c.knownSecrets()).contains("pw-123456", basic);
    assertThat(c.toString()).doesNotContain("pw-123456").contains("[REDACTED]");

    LokiConfig vendor =
        LokiConfig.fromEnv(
            Map.of(
                "LOKI_URL",
                "http://vendor:2",
                "LOKI_USERNAME",
                "u1",
                "LOKI_BASIC_AUTH_USER",
                "u2",
                "LOKI_PASSWORD",
                "p1",
                P + "TENANT_ID",
                "grow-t",
                "LOKI_TENANT_ID",
                "vendor-t"));
    assertThat(vendor.url()).isEqualTo("http://vendor:2");
    assertThat(vendor.username()).contains("u1");
    assertThat(vendor.password().orElseThrow().reveal()).isEqualTo("p1");
    assertThat(vendor.tenantId()).contains("grow-t");

    LokiConfig bearer =
        LokiConfig.fromEnv(
            Map.of("LOKI_BEARER_TOKEN", "tok-abcdef", P + "INSECURE_SKIP_VERIFY", "1"));
    assertThat(bearer.authorizationHeader()).contains("Bearer tok-abcdef");
    assertThat(bearer.knownSecrets()).containsExactly("tok-abcdef");
    assertThat(bearer.tls().insecureSkipVerify()).isTrue();
    assertThat(bearer.toString()).doesNotContain("tok-abcdef");

    // explicit values win over the environment
    LokiConfig explicit =
        LokiConfig.builder()
            .url("http://explicit:3")
            .tenantId("x")
            .username("eu")
            .password("ep")
            .connectTimeout(Duration.ofSeconds(1))
            .requestTimeout(Duration.ofSeconds(1))
            .totalTimeout(Duration.ofSeconds(1))
            .retry(RetryPolicy.builder().maxRetries(0).build())
            .tls(TlsOptions.defaults())
            .proxy("http://p:1")
            .fromEnv(EnvReader.of(env))
            .build();
    assertThat(explicit.url()).isEqualTo("http://explicit:3");
    assertThat(explicit.tenantId()).contains("x");
    assertThat(explicit.username()).contains("eu");
    assertThat(explicit.retry().maxRetries()).isZero();
    assertThat(explicit.proxy()).contains("http://p:1");
    // a pre-set bearer token is not overwritten either
    assertThat(
            LokiConfig.builder()
                .bearerToken(Secret.of("t-1234"))
                .fromEnv(EnvReader.of(Map.of("LOKI_BEARER_TOKEN", "other")))
                .build()
                .bearerToken()
                .orElseThrow()
                .reveal())
        .isEqualTo("t-1234");
  }

  @Test
  void envErrorsNameTheVariable() {
    assertThatThrownBy(() -> LokiConfig.fromEnv(Map.of(P + "MAX_RETRIES", "-1")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(P + "MAX_RETRIES");
    assertThatThrownBy(() -> LokiConfig.fromEnv(Map.of(P + "CONNECT_TIMEOUT", "soon")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(P + "CONNECT_TIMEOUT")
        .hasMessageNotContaining("soon");
  }

  @Test
  void validation() {
    assertThatThrownBy(() -> LokiConfig.builder().url("ftp://x").build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().url("http://bad host").build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().url("/relative").build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().connectTimeout(Duration.ZERO).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().requestTimeout(Duration.ofSeconds(-1)).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().totalTimeout(Duration.ZERO).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().proxy("relative").build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().proxy("http://bad host").build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> LokiConfig.builder().username("u").build())
        .hasMessageContaining("together");
    assertThatThrownBy(() -> LokiConfig.builder().password("p").build())
        .hasMessageContaining("together");
    assertThatThrownBy(
            () -> LokiConfig.builder().username("u").password("p").bearerToken("t").build())
        .hasMessageContaining("mutually exclusive");
    assertThatThrownBy(() -> LokiConfig.builder().bearerToken(" ").build())
        .hasMessageContaining("blank");
    assertThatThrownBy(() -> LokiConfig.builder().tenantId("bad/tenant").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("tenant id");
    assertThatThrownBy(() -> LokiConfig.builder().allowedLabels(List.of("bad-name")).build())
        .hasMessageContaining("allowedLabels");
    assertThat(
            LokiConfig.builder()
                .tenantId("a|b")
                .multiTenantQueries(true)
                .allowedLabels(List.of("app"))
                .disableLabelGuard(true)
                .build()
                .allowedLabels())
        .containsExactly("app");
    // empty strings mean unset
    LokiConfig empty = LokiConfig.builder().tenantId("").username("").build();
    assertThat(empty.tenantId()).isEmpty();
    assertThat(empty.username()).isEmpty();
    assertThat(
            LokiConfig.builder()
                .bearerToken((String) null)
                .password((String) null)
                .build()
                .bearerToken())
        .isEmpty();
  }

  @Test
  void tenantRules() {
    assertThat(LokiConfig.tenantIdProblem("team-a_1.*'()!", false)).isEmpty();
    assertThat(LokiConfig.tenantIdProblem("", false)).isPresent();
    assertThat(LokiConfig.tenantIdProblem(null, false)).isPresent();
    assertThat(LokiConfig.tenantIdProblem(".", false))
        .hasValueSatisfying(p -> assertThat(p).contains("'.'"));
    assertThat(LokiConfig.tenantIdProblem("..", false)).isPresent();
    assertThat(LokiConfig.tenantIdProblem("x".repeat(151), false))
        .hasValueSatisfying(p -> assertThat(p).contains("too long"));
    assertThat(LokiConfig.tenantIdProblem("x".repeat(150), false)).isEmpty();
    assertThat(LokiConfig.tenantIdProblem("a b", false)).isPresent();
    assertThat(LokiConfig.tenantIdProblem("ü", false)).isPresent();
    assertThat(LokiConfig.tenantIdProblem("a|b", false))
        .hasValueSatisfying(p -> assertThat(p).contains("|"));
    assertThat(LokiConfig.tenantIdProblem("a|b", true)).isEmpty();
    assertThat(LokiConfig.tenantIdProblem("a|", true)).isPresent();
    assertThat(LokiConfig.tenantIdProblem("a|b/c", true)).isPresent();
  }

  @Test
  void toBuilderCopiesEverything() {
    Hooks hooks = new Hooks() {};
    LokiConfig c =
        LokiConfig.builder()
            .url("https://loki.example:443/base")
            .tenantId("t1")
            .bearerToken("tok-123456")
            .multiTenantQueries(true)
            .allowedLabels(List.of("app"))
            .disableLabelGuard(true)
            .header("X-Extra", "1")
            .userAgentSuffix("suite")
            .hooks(hooks)
            .logger(Logger.NOOP)
            .build();
    LokiConfig copy = c.toBuilder().build();
    assertThat(copy.toString()).isEqualTo(c.toString());
    assertThat(copy.headers()).containsEntry("X-Extra", "1");
    assertThat(copy.hooks()).isSameAs(hooks);
    assertThat(copy.toTransportConfig().userAgent())
        .contains("grow-codes-grafana-loki/")
        .endsWith("suite");
    assertThat(copy.toTransportConfig().knownSecrets()).containsExactly("tok-123456");
    Secret pw = Secret.of("pw-999999");
    LokiConfig basic = LokiConfig.builder().username("u").password(pw).build();
    pw.clear();
    assertThat(basic.knownSecrets()).isEmpty();
    Secret tok = Secret.of("tok-999999");
    LokiConfig bearer = LokiConfig.builder().bearerToken(tok).build();
    tok.clear();
    assertThat(bearer.knownSecrets()).isEmpty();
    assertThat(LokiConfig.builder().proxy("http://u:pw@p:1").build().toString())
        .doesNotContain("pw@");
  }
}
