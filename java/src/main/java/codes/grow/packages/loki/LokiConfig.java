package codes.grow.packages.loki;

import codes.grow.packages.loki.core.ConfigException;
import codes.grow.packages.loki.core.EnvReader;
import codes.grow.packages.loki.core.Hooks;
import codes.grow.packages.loki.core.Logger;
import codes.grow.packages.loki.core.Redaction;
import codes.grow.packages.loki.core.RetryPolicy;
import codes.grow.packages.loki.core.Secret;
import codes.grow.packages.loki.core.TlsOptions;
import codes.grow.packages.loki.core.TransportConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable configuration of a {@link LokiClient} (SPEC section 2.2).
 *
 * <p>Build one with {@link #builder()} or read it from the environment with {@link #fromEnv()}.
 * Precedence: explicit builder value &gt; {@code GROW_GRAFANA_LOKI_<NAME>} &gt; vendor variables
 * &gt; default. {@link #toString()} never prints credentials.
 *
 * <p>Loki itself has no authentication: {@link Builder#tenantId(String)} is sent as {@code
 * X-Scope-OrgID} (required by multi-tenant Loki), Basic ({@code username}/{@code password}) and
 * Bearer credentials are for gateways and hosted Loki (Grafana Cloud: user = instance id, password
 * = access token).
 *
 * <table>
 *   <caption>Environment variables</caption>
 *   <tr><th>Field</th><th>Variables (in order)</th></tr>
 *   <tr><td>url</td><td>{@code GROW_GRAFANA_LOKI_URL}, {@code LOKI_URL}</td></tr>
 *   <tr><td>tenantId</td><td>{@code GROW_GRAFANA_LOKI_TENANT_ID}, {@code LOKI_TENANT_ID}</td></tr>
 *   <tr><td>username</td><td>{@code GROW_GRAFANA_LOKI_USERNAME}, {@code LOKI_USERNAME},
 *       {@code LOKI_BASIC_AUTH_USER}</td></tr>
 *   <tr><td>password</td><td>{@code GROW_GRAFANA_LOKI_PASSWORD}, {@code LOKI_PASSWORD},
 *       {@code LOKI_BASIC_AUTH_PASSWORD}</td></tr>
 *   <tr><td>bearerToken</td><td>{@code GROW_GRAFANA_LOKI_BEARER_TOKEN},
 *       {@code LOKI_BEARER_TOKEN}</td></tr>
 * </table>
 *
 * <p>plus the common {@code CONNECT_TIMEOUT}, {@code REQUEST_TIMEOUT}, {@code TOTAL_TIMEOUT},
 * {@code MAX_RETRIES}, {@code CA_CERT}, {@code CLIENT_CERT}, {@code CLIENT_KEY}, {@code
 * INSECURE_SKIP_VERIFY} and {@code PROXY} after the {@code GROW_GRAFANA_LOKI_} prefix.
 */
public final class LokiConfig {
  /** Default endpoint (the stack catalog port 3100). */
  public static final String DEFAULT_URL = "http://127.0.0.1:3100";

  /** Longest tenant id Loki accepts, in bytes. */
  public static final int MAX_TENANT_ID_LENGTH = 150;

  /** Vendor variables read for {@code url} after {@code GROW_GRAFANA_LOKI_URL}, in order. */
  static final String[] URL_VENDOR_VARS = {"LOKI_URL"};

  /** Vendor variables read for {@code tenantId}, in order. */
  static final String[] TENANT_VENDOR_VARS = {"LOKI_TENANT_ID"};

  /** Vendor variables read for {@code username}, in order. */
  static final String[] USERNAME_VENDOR_VARS = {"LOKI_USERNAME", "LOKI_BASIC_AUTH_USER"};

  /** Vendor variables read for {@code password}, in order. */
  static final String[] PASSWORD_VENDOR_VARS = {"LOKI_PASSWORD", "LOKI_BASIC_AUTH_PASSWORD"};

  /** Vendor variables read for {@code bearerToken}, in order. */
  static final String[] BEARER_VENDOR_VARS = {"LOKI_BEARER_TOKEN"};

  /** Vendor variables read for the CA bundle after the SDK variable {@code CA_CERT}. */
  static final String[] CA_CERT_VENDOR_VARS = {};

  private final String url;
  private final String tenantId;
  private final String username;
  private final Secret password;
  private final Secret bearerToken;
  private final boolean multiTenantQueries;
  private final List<String> allowedLabels;
  private final boolean disableLabelGuard;
  private final Duration connectTimeout;
  private final Duration requestTimeout;
  private final Duration totalTimeout;
  private final RetryPolicy retry;
  private final TlsOptions tls;
  private final String proxy;
  private final Map<String, String> headers;
  private final String userAgentSuffix;
  private final Hooks hooks;
  private final Logger logger;

  private LokiConfig(Builder b) {
    this.url = b.url != null ? b.url : DEFAULT_URL;
    this.tenantId = emptyToNull(b.tenantId);
    this.username = emptyToNull(b.username);
    this.password = b.password;
    this.bearerToken = b.bearerToken;
    this.multiTenantQueries = b.multiTenantQueries;
    this.allowedLabels = b.allowedLabels == null ? null : List.copyOf(b.allowedLabels);
    this.disableLabelGuard = b.disableLabelGuard;
    this.connectTimeout = b.connectTimeout != null ? b.connectTimeout : Duration.ofSeconds(10);
    this.requestTimeout = b.requestTimeout != null ? b.requestTimeout : Duration.ofSeconds(30);
    this.totalTimeout = b.totalTimeout != null ? b.totalTimeout : Duration.ofSeconds(120);
    this.retry = b.retry != null ? b.retry : RetryPolicy.defaults();
    this.tls = b.tls != null ? b.tls : TlsOptions.defaults();
    this.proxy = b.proxy;
    this.headers = Map.copyOf(b.headers);
    this.userAgentSuffix = b.userAgentSuffix;
    this.hooks = b.hooks != null ? b.hooks : Hooks.NONE;
    this.logger = b.logger != null ? b.logger : Logger.NOOP;
  }

  private static String emptyToNull(String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  /**
   * Starts an empty builder (every field at its default).
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Starts a builder holding every field of this configuration.
   *
   * @return a pre-filled builder
   */
  public Builder toBuilder() {
    Builder b =
        new Builder()
            .url(url)
            .tenantId(tenantId)
            .username(username)
            .password(password)
            .bearerToken(bearerToken)
            .multiTenantQueries(multiTenantQueries)
            .allowedLabels(allowedLabels)
            .disableLabelGuard(disableLabelGuard)
            .connectTimeout(connectTimeout)
            .requestTimeout(requestTimeout)
            .totalTimeout(totalTimeout)
            .retry(retry)
            .tls(tls)
            .proxy(proxy)
            .userAgentSuffix(userAgentSuffix)
            .hooks(hooks)
            .logger(logger);
    headers.forEach(b::header);
    return b;
  }

  /**
   * Reads the configuration from the process environment.
   *
   * @return the validated configuration
   * @throws ConfigException when a variable holds an invalid value (the message names the variable,
   *     never the value)
   */
  public static LokiConfig fromEnv() {
    return builder().fromEnv(EnvReader.system()).build();
  }

  /**
   * Reads the configuration from an explicit variable map (tests).
   *
   * @param env variable map
   * @return the validated configuration
   */
  public static LokiConfig fromEnv(Map<String, String> env) {
    return builder().fromEnv(EnvReader.of(env)).build();
  }

  /**
   * Returns the problem with a tenant id ({@code X-Scope-OrgID}), or empty when it is valid.
   *
   * <p>Loki rules: non-empty, at most 150 bytes, not {@code .} or {@code ..}, only {@code
   * [A-Za-z0-9!-_.*'()]}. A {@code |} (multi-tenant query) is accepted only when {@code
   * multiTenantQueries} is set, and every part must then be valid.
   *
   * @param id tenant id
   * @param multiTenantQueries whether {@code a|b} ids are allowed
   * @return the problem, or empty
   */
  public static Optional<String> tenantIdProblem(String id, boolean multiTenantQueries) {
    if (id == null || id.isEmpty()) {
      return Optional.of("tenant id must not be empty");
    }
    if (id.indexOf('|') >= 0) {
      if (!multiTenantQueries) {
        return Optional.of(
            "tenant id must not contain '|' (multi-tenant queries need multiTenantQueries)");
      }
      for (String part : id.split("\\|", -1)) {
        Optional<String> p = tenantIdProblem(part, false);
        if (p.isPresent()) {
          return p;
        }
      }
      return Optional.empty();
    }
    if (id.getBytes(StandardCharsets.UTF_8).length > MAX_TENANT_ID_LENGTH) {
      return Optional.of("tenant id is too long (max " + MAX_TENANT_ID_LENGTH + " bytes)");
    }
    if (id.equals(".") || id.equals("..")) {
      return Optional.of("tenant id must not be '.' or '..'");
    }
    for (int i = 0; i < id.length(); i++) {
      char c = id.charAt(i);
      boolean ok =
          (c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || "!-_.*'()".indexOf(c) >= 0;
      if (!ok) {
        return Optional.of("tenant id may only contain letters, digits and !-_.*'()");
      }
    }
    return Optional.empty();
  }

  /**
   * Checks URL scheme, positive timeouts, TLS pairs, the tenant id and the credential rules.
   *
   * @throws ConfigException when a rule is violated
   */
  public void validate() {
    URI u;
    try {
      u = new URI(url);
    } catch (URISyntaxException e) {
      throw new ConfigException("config: url is not a valid URL");
    }
    String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
    if (!(scheme.equals("http") || scheme.equals("https")) || u.getHost() == null) {
      throw new ConfigException("config: url must use http or https and include a host");
    }
    requirePositive("connectTimeout", connectTimeout);
    requirePositive("requestTimeout", requestTimeout);
    requirePositive("totalTimeout", totalTimeout);
    tls.validate();
    if (proxy != null) {
      try {
        URI p = new URI(proxy);
        if (p.getHost() == null) {
          throw new ConfigException("config: proxy must be an absolute URL");
        }
      } catch (URISyntaxException e) {
        throw new ConfigException("config: proxy is not a valid URL");
      }
    }
    if (tenantId != null) {
      tenantIdProblem(tenantId, multiTenantQueries)
          .ifPresent(
              p -> {
                throw new ConfigException("config: " + p);
              });
    }
    if ((username == null) != (password == null)) {
      throw new ConfigException("config: username and password must be given together");
    }
    if (password != null && bearerToken != null) {
      throw new ConfigException(
          "config: basic auth (username/password) and bearerToken are mutually exclusive");
    }
    if (bearerToken != null && bearerToken.reveal().isBlank()) {
      throw new ConfigException("config: bearerToken must not be blank");
    }
    if (allowedLabels != null) {
      for (String name : allowedLabels) {
        if (!Labels.isValidLabelName(name)) {
          throw new ConfigException("config: allowedLabels contains an invalid label name");
        }
      }
    }
  }

  private static void requirePositive(String name, Duration d) {
    if (d.isZero() || d.isNegative()) {
      throw new ConfigException("config: " + name + " must be positive");
    }
  }

  /**
   * Translates the common fields into transport settings.
   *
   * @return the transport configuration
   */
  public TransportConfig toTransportConfig() {
    return TransportConfig.builder(url)
        .connectTimeout(connectTimeout)
        .requestTimeout(requestTimeout)
        .totalTimeout(totalTimeout)
        .retry(retry)
        .tls(tls)
        .proxy(proxy)
        .headers(headers)
        .userAgentSuffix(userAgentSuffix)
        .hooks(hooks)
        .logger(logger)
        .knownSecrets(this::knownSecrets)
        .build();
  }

  /**
   * Returns the {@code Authorization} header value when credentials are configured.
   *
   * @return {@code Bearer …} or {@code Basic …}, or empty
   */
  public Optional<String> authorizationHeader() {
    if (bearerToken != null) {
      return Optional.of("Bearer " + bearerToken.reveal());
    }
    if (username != null && password != null) {
      return Optional.of("Basic " + basic(username, password));
    }
    return Optional.empty();
  }

  /**
   * Returns the credential values that are removed from error messages and logs.
   *
   * @return secret values (password, token, encoded Basic credentials)
   */
  public List<String> knownSecrets() {
    List<String> out = new ArrayList<>();
    if (password != null && !password.isCleared()) {
      out.add(password.reveal());
      if (username != null) {
        out.add(basic(username, password));
      }
    }
    if (bearerToken != null && !bearerToken.isCleared()) {
      out.add(bearerToken.reveal());
    }
    return out;
  }

  private static String basic(String user, Secret password) {
    return Base64.getEncoder()
        .encodeToString((user + ":" + password.reveal()).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns the endpoint base URL.
   *
   * @return the URL
   */
  public String url() {
    return url;
  }

  /**
   * Returns the tenant sent as {@code X-Scope-OrgID}.
   *
   * @return the tenant id, or empty (no header)
   */
  public Optional<String> tenantId() {
    return Optional.ofNullable(tenantId);
  }

  /**
   * Returns the Basic-auth user.
   *
   * @return the user, or empty
   */
  public Optional<String> username() {
    return Optional.ofNullable(username);
  }

  /**
   * Returns the Basic-auth password (or Grafana Cloud access token).
   *
   * @return the password, or empty
   */
  public Optional<Secret> password() {
    return Optional.ofNullable(password);
  }

  /**
   * Returns the Bearer token for gateways.
   *
   * @return the token, or empty
   */
  public Optional<Secret> bearerToken() {
    return Optional.ofNullable(bearerToken);
  }

  /**
   * Returns whether tenant ids joined with {@code |} (multi-tenant queries) are allowed.
   *
   * @return true when allowed
   */
  public boolean multiTenantQueries() {
    return multiTenantQueries;
  }

  /**
   * Returns the cardinality guard allowlist: label names that may become index labels.
   *
   * @return the configured list, or {@link Labels#DEFAULT_ALLOWED_LABELS}
   */
  public List<String> allowedLabels() {
    return allowedLabels != null ? allowedLabels : Labels.DEFAULT_ALLOWED_LABELS;
  }

  /**
   * Returns whether {@link LokiClient#push} sends every stream label as an index label.
   *
   * @return true when the guard is off for {@code push} (the log exporter always applies it)
   */
  public boolean disableLabelGuard() {
    return disableLabelGuard;
  }

  /**
   * Returns the connect timeout.
   *
   * @return the timeout
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  /**
   * Returns the per-attempt timeout.
   *
   * @return the timeout
   */
  public Duration requestTimeout() {
    return requestTimeout;
  }

  /**
   * Returns the total timeout per call including retries.
   *
   * @return the timeout
   */
  public Duration totalTimeout() {
    return totalTimeout;
  }

  /**
   * Returns the retry policy.
   *
   * @return the policy
   */
  public RetryPolicy retry() {
    return retry;
  }

  /**
   * Returns the TLS options.
   *
   * @return the options
   */
  public TlsOptions tls() {
    return tls;
  }

  /**
   * Returns the explicit proxy URL.
   *
   * @return the proxy, or empty
   */
  public Optional<String> proxy() {
    return Optional.ofNullable(proxy);
  }

  /**
   * Returns the extra request headers.
   *
   * @return immutable map
   */
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Returns the user agent suffix.
   *
   * @return the suffix, or empty
   */
  public Optional<String> userAgentSuffix() {
    return Optional.ofNullable(userAgentSuffix);
  }

  /**
   * Returns the hooks.
   *
   * @return the hooks
   */
  public Hooks hooks() {
    return hooks;
  }

  /**
   * Returns the logger.
   *
   * @return the logger
   */
  public Logger logger() {
    return logger;
  }

  @Override
  public String toString() {
    return "LokiConfig{url="
        + Redaction.redactUrl(url)
        + ", tenantId="
        + tenantId
        + ", username="
        + username
        + ", password="
        + (password == null ? "null" : Redaction.REDACTED)
        + ", bearerToken="
        + (bearerToken == null ? "null" : Redaction.REDACTED)
        + ", multiTenantQueries="
        + multiTenantQueries
        + ", allowedLabels="
        + allowedLabels()
        + ", disableLabelGuard="
        + disableLabelGuard
        + ", connectTimeout="
        + connectTimeout
        + ", requestTimeout="
        + requestTimeout
        + ", totalTimeout="
        + totalTimeout
        + ", retry="
        + retry
        + ", tls="
        + tls
        + ", proxy="
        + (proxy == null ? "null" : Redaction.redactUrl(proxy))
        + ", headers="
        + Redaction.redactHeaders(headers)
        + ", userAgentSuffix="
        + userAgentSuffix
        + "}";
  }

  /**
   * Builder of the configuration.
   *
   * <p>Unset fields take the environment (when {@link #fromEnv(EnvReader)} is called) or the
   * default.
   */
  public static final class Builder {
    private String url;
    private String tenantId;
    private String username;
    private Secret password;
    private Secret bearerToken;
    private boolean multiTenantQueries;
    private List<String> allowedLabels;
    private boolean disableLabelGuard;
    private Duration connectTimeout;
    private Duration requestTimeout;
    private Duration totalTimeout;
    private RetryPolicy retry;
    private TlsOptions tls;
    private String proxy;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private String userAgentSuffix;
    private Hooks hooks;
    private Logger logger;

    private Builder() {}

    /**
     * Fills every field that was not set explicitly from the environment.
     *
     * @param env environment reader
     * @return this builder
     * @throws ConfigException when a variable holds an invalid value
     */
    public Builder fromEnv(EnvReader env) {
      if (url == null) {
        url = env.string("URL", URL_VENDOR_VARS).orElse(null);
      }
      if (tenantId == null) {
        tenantId = env.string("TENANT_ID", TENANT_VENDOR_VARS).orElse(null);
      }
      if (username == null) {
        username = env.string("USERNAME", USERNAME_VENDOR_VARS).orElse(null);
      }
      if (password == null) {
        password = env.string("PASSWORD", PASSWORD_VENDOR_VARS).map(Secret::of).orElse(null);
      }
      if (bearerToken == null) {
        bearerToken = env.string("BEARER_TOKEN", BEARER_VENDOR_VARS).map(Secret::of).orElse(null);
      }
      if (connectTimeout == null) {
        connectTimeout = env.duration("CONNECT_TIMEOUT").orElse(null);
      }
      if (requestTimeout == null) {
        requestTimeout = env.duration("REQUEST_TIMEOUT").orElse(null);
      }
      if (totalTimeout == null) {
        totalTimeout = env.duration("TOTAL_TIMEOUT").orElse(null);
      }
      if (retry == null && env.integer("MAX_RETRIES").isPresent()) {
        int n = env.integer("MAX_RETRIES").getAsInt();
        if (n < 0) {
          String variable = EnvReader.DEFAULT_PREFIX + "MAX_RETRIES";
          throw new ConfigException("config: invalid value in " + variable + " (expected >= 0)");
        }
        retry = RetryPolicy.builder().maxRetries(n).build();
      }
      if (tls == null) {
        TlsOptions.Builder t = TlsOptions.builder();
        env.string("CA_CERT", CA_CERT_VENDOR_VARS).ifPresent(p -> t.caCertPath(Paths.get(p)));
        env.string("CLIENT_CERT").ifPresent(p -> t.clientCertPath(Paths.get(p)));
        env.string("CLIENT_KEY").ifPresent(p -> t.clientKeyPath(Paths.get(p)));
        env.bool("INSECURE_SKIP_VERIFY").ifPresent(t::insecureSkipVerify);
        tls = t.build();
      }
      if (proxy == null) {
        proxy = env.string("PROXY").orElse(null);
      }
      return this;
    }

    /**
     * Sets the endpoint base URL (default {@code http://127.0.0.1:3100}).
     *
     * @param url {@code http} or {@code https} URL; a base path (reverse proxy) is kept
     * @return this builder
     */
    public Builder url(String url) {
      this.url = url;
      return this;
    }

    /**
     * Sets the tenant sent as {@code X-Scope-OrgID} on every request and tail handshake.
     *
     * @param tenantId tenant id ({@code null} or empty sends no header)
     * @return this builder
     */
    public Builder tenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    /**
     * Sets the Basic-auth user (gateways, Grafana Cloud instance id).
     *
     * @param username user
     * @return this builder
     */
    public Builder username(String username) {
      this.username = username;
      return this;
    }

    /**
     * Sets the Basic-auth password (or Grafana Cloud access token).
     *
     * @param password password
     * @return this builder
     */
    public Builder password(Secret password) {
      this.password = password;
      return this;
    }

    /**
     * Sets the Basic-auth password.
     *
     * @param password password
     * @return this builder
     */
    public Builder password(String password) {
      this.password = password == null ? null : Secret.of(password);
      return this;
    }

    /**
     * Sets the Bearer token sent to a gateway ({@code Authorization: Bearer …}).
     *
     * @param token token
     * @return this builder
     */
    public Builder bearerToken(Secret token) {
      this.bearerToken = token;
      return this;
    }

    /**
     * Sets the Bearer token.
     *
     * @param token token
     * @return this builder
     */
    public Builder bearerToken(String token) {
      this.bearerToken = token == null ? null : Secret.of(token);
      return this;
    }

    /**
     * Allows tenant ids joined with {@code |} (Loki's multi-tenant query syntax; the server must
     * enable {@code multi_tenant_queries_enabled}).
     *
     * @param allow whether to allow them (default false)
     * @return this builder
     */
    public Builder multiTenantQueries(boolean allow) {
      this.multiTenantQueries = allow;
      return this;
    }

    /**
     * Sets the cardinality guard allowlist (label names that may become index labels; everything
     * else goes to structured metadata).
     *
     * @param names label names, or {@code null} for {@link Labels#DEFAULT_ALLOWED_LABELS}
     * @return this builder
     */
    public Builder allowedLabels(List<String> names) {
      this.allowedLabels = names == null ? null : new ArrayList<>(names);
      return this;
    }

    /**
     * Sends every label of {@link LokiClient#push} streams as an index label (the guard is still
     * applied by the log exporter).
     *
     * @param disable whether to disable the guard for {@code push} (default false)
     * @return this builder
     */
    public Builder disableLabelGuard(boolean disable) {
      this.disableLabelGuard = disable;
      return this;
    }

    /**
     * Sets the connect timeout (default 10 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder connectTimeout(Duration d) {
      this.connectTimeout = d;
      return this;
    }

    /**
     * Sets the per-attempt timeout (default 30 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder requestTimeout(Duration d) {
      this.requestTimeout = d;
      return this;
    }

    /**
     * Sets the total timeout per call including retries (default 120 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder totalTimeout(Duration d) {
      this.totalTimeout = d;
      return this;
    }

    /**
     * Sets the retry policy (default {@link RetryPolicy#defaults()}).
     *
     * @param retry policy
     * @return this builder
     */
    public Builder retry(RetryPolicy retry) {
      this.retry = retry;
      return this;
    }

    /**
     * Sets the TLS options; environment TLS variables are then ignored.
     *
     * @param tls options
     * @return this builder
     */
    public Builder tls(TlsOptions tls) {
      this.tls = tls;
      return this;
    }

    /**
     * Sets an explicit proxy URL; environment proxies ({@code HTTPS_PROXY}) are never used.
     *
     * @param proxy proxy URL
     * @return this builder
     */
    public Builder proxy(String proxy) {
      this.proxy = proxy;
      return this;
    }

    /**
     * Adds a header sent with every request (and the tail handshake).
     *
     * @param name header name
     * @param value header value
     * @return this builder
     */
    public Builder header(String name, String value) {
      this.headers.put(name, value);
      return this;
    }

    /**
     * Sets the text appended to the {@code User-Agent}.
     *
     * @param suffix suffix
     * @return this builder
     */
    public Builder userAgentSuffix(String suffix) {
      this.userAgentSuffix = suffix;
      return this;
    }

    /**
     * Sets request hooks.
     *
     * @param hooks hooks
     * @return this builder
     */
    public Builder hooks(Hooks hooks) {
      this.hooks = hooks;
      return this;
    }

    /**
     * Sets the logger (default silent).
     *
     * @param logger logger
     * @return this builder
     */
    public Builder logger(Logger logger) {
      this.logger = logger;
      return this;
    }

    /**
     * Builds and validates the configuration.
     *
     * @return the configuration
     * @throws ConfigException when a rule is violated
     */
    public LokiConfig build() {
      LokiConfig c = new LokiConfig(this);
      c.validate();
      return c;
    }
  }
}
