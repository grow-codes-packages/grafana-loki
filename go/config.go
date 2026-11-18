package loki

import (
	"fmt"
	"strings"
)

// DefaultURL is the default endpoint of Grafana Loki (HTTP API, OTLP and
// WebSocket tail share one port).
const DefaultURL = "http://127.0.0.1:3100"

// maxTenantIDLength is Loki's limit for a tenant id (X-Scope-OrgID), in bytes.
const maxTenantIDLength = 150

// DefaultAllowedLabels is the cardinality guard's default label allowlist:
// only these keys become Loki index labels; every other attribute is sent as
// structured metadata.
var DefaultAllowedLabels = []string{
	"service_name", "service_namespace", "deployment_environment", "host_name", "level",
	"job", "instance", "namespace", "pod", "container",
}

// vendorEnv lists the vendor variables consulted after GROW_GRAFANA_LOKI_*.
var vendorEnv = VendorEnv{URL: []string{"LOKI_URL"}}

// Vendor variables for the service fields, in precedence order.
var (
	vendorTenantEnv   = []string{"LOKI_TENANT_ID"}
	vendorUsernameEnv = []string{"LOKI_USERNAME", "LOKI_BASIC_AUTH_USER"}
	vendorPasswordEnv = []string{"LOKI_PASSWORD", "LOKI_BASIC_AUTH_PASSWORD"}
	vendorBearerEnv   = []string{"LOKI_BEARER_TOKEN"}
)

// Config configures a Client. Build it with a struct literal or ConfigFromEnv;
// zero values mean "use the default". Call Validate to check it.
//
// Loki itself has no authentication: TenantID is sent as X-Scope-OrgID (needed
// when Loki runs with auth_enabled: true), and Basic or Bearer credentials are
// for gateways and hosted Loki (Grafana Cloud: Basic user = instance id,
// password = access token). The client never interprets them.
//
// Environment variables read by ConfigFromEnv (explicit value >
// GROW_GRAFANA_LOKI_* > vendor variables > default):
//
//	URL          GROW_GRAFANA_LOKI_URL, LOKI_URL
//	TenantID     GROW_GRAFANA_LOKI_TENANT_ID, LOKI_TENANT_ID
//	Username     GROW_GRAFANA_LOKI_USERNAME, LOKI_USERNAME, LOKI_BASIC_AUTH_USER
//	Password     GROW_GRAFANA_LOKI_PASSWORD, LOKI_PASSWORD, LOKI_BASIC_AUTH_PASSWORD
//	BearerToken  GROW_GRAFANA_LOKI_BEARER_TOKEN, LOKI_BEARER_TOKEN
//
// plus the common GROW_GRAFANA_LOKI_CONNECT_TIMEOUT, REQUEST_TIMEOUT,
// TOTAL_TIMEOUT, MAX_RETRIES, CA_CERT, CLIENT_CERT, CLIENT_KEY,
// INSECURE_SKIP_VERIFY and PROXY.
type Config struct {
	CommonConfig

	// TenantID is sent as X-Scope-OrgID on every request and on the tail
	// handshake ("" sends no header). Rules: at most 150 bytes, not "." or
	// "..", characters [A-Za-z0-9!-_.*'()] only; "|" (multi-tenant query) only
	// when MultiTenantQueries is set.
	TenantID string
	// Username and Password send HTTP Basic authentication (both or neither).
	Username string
	Password Secret
	// BearerToken sends "Authorization: Bearer <token>" (not together with Basic).
	BearerToken Secret
	// MultiTenantQueries allows tenant ids joined with "|" (Loki's multi-tenant
	// query syntax; the server must enable multi_tenant_queries_enabled).
	MultiTenantQueries bool
	// AllowedLabels is the cardinality guard allowlist used by Push and the log
	// exporter: labels outside it are moved to structured metadata. nil means
	// DefaultAllowedLabels.
	AllowedLabels []string
	// DisableLabelGuard sends every stream label as an index label (Push only;
	// the LogRecord exporter always applies the guard).
	DisableLabelGuard bool
}

// ConfigFromEnv reads the configuration from the process environment.
func ConfigFromEnv() (Config, error) { return ConfigFromEnvWith(Config{}, nil) }

// ConfigFromEnvWith resolves explicit values > GROW_GRAFANA_LOKI_* > vendor
// variables > defaults. env replaces the process environment when non-nil.
func ConfigFromEnvWith(explicit Config, env map[string]string) (Config, error) {
	r := NewEnvReader(EnvPrefix, env)
	c := explicit
	if err := c.applyEnv(r, vendorEnv); err != nil {
		return Config{}, err
	}
	if c.TenantID == "" {
		c.TenantID, _ = r.String("TENANT_ID", vendorTenantEnv...)
	}
	if c.Username == "" {
		c.Username, _ = r.String("USERNAME", vendorUsernameEnv...)
	}
	if c.Password.IsZero() {
		if v, ok := r.String("PASSWORD", vendorPasswordEnv...); ok {
			c.Password = NewSecret(v)
		}
	}
	if c.BearerToken.IsZero() {
		if v, ok := r.String("BEARER_TOKEN", vendorBearerEnv...); ok {
			c.BearerToken = NewSecret(v)
		}
	}
	c.CommonConfig = c.withDefaults(DefaultURL)
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate checks the configuration and returns a CONFIG error.
func (c Config) Validate() error {
	if err := c.withDefaults(DefaultURL).validate(); err != nil {
		return err
	}
	if (c.Username == "") != c.Password.IsZero() {
		return ConfigError("username and password must be given together")
	}
	if c.Username != "" && !c.BearerToken.IsZero() {
		return ConfigError("basic credentials and bearer token are mutually exclusive")
	}
	if c.TenantID != "" {
		if reason := tenantProblem(c.TenantID, c.MultiTenantQueries); reason != "" {
			return ConfigError("tenantId " + reason)
		}
	}
	for _, l := range c.AllowedLabels {
		if !validLabelName(l) {
			return ConfigError(fmt.Sprintf("allowedLabels contains an invalid label name %q", l))
		}
	}
	return nil
}

// tenantProblem returns why id is not a valid tenant id ("" when valid).
func tenantProblem(id string, multi bool) string {
	if strings.Contains(id, "|") {
		if !multi {
			return "must not contain '|' (multi-tenant queries are not enabled)"
		}
		for _, part := range strings.Split(id, "|") {
			if reason := tenantProblem(part, false); reason != "" {
				return reason
			}
		}
		return ""
	}
	switch {
	case id == "":
		return "must not be empty"
	case len(id) > maxTenantIDLength:
		return "is too long (max 150 bytes)"
	case id == "." || id == "..":
		return "must not be '.' or '..'"
	}
	for _, r := range id {
		if !tenantRune(r) {
			return fmt.Sprintf("contains unsupported character %q", r)
		}
	}
	return ""
}

func tenantRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune("!-_.*'()", r)
}

// allowedLabels returns the effective cardinality guard allowlist.
func (c Config) allowedLabels() []string {
	if c.AllowedLabels != nil {
		return c.AllowedLabels
	}
	return DefaultAllowedLabels
}

// String renders the configuration with every credential redacted.
func (c Config) String() string {
	return fmt.Sprintf("Config{%s TenantID:%s Username:%s Password:%s BearerToken:%s MultiTenantQueries:%t}",
		c.describe(), c.TenantID, c.Username, c.Password, c.BearerToken, c.MultiTenantQueries)
}

// GoString renders the configuration with every credential redacted.
func (c Config) GoString() string { return c.String() }
