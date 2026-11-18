package loki

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	t.Run("core.config.env-precedence", func(t *testing.T) {
		env := map[string]string{
			EnvPrefix + "URL": "http://grow:1", "LOKI_URL": "http://vendor:2",
			"LOKI_TENANT_ID": "tenant-a", "LOKI_USERNAME": "u1", "LOKI_BASIC_AUTH_USER": "u2",
			"LOKI_BASIC_AUTH_PASSWORD": "pw-alias",
		}
		cfg, err := ConfigFromEnvWith(Config{}, env)
		if err != nil || cfg.URL != "http://grow:1" || cfg.TenantID != "tenant-a" || cfg.Username != "u1" || cfg.Password.Reveal() != "pw-alias" {
			t.Fatalf("env: %v %s", err, cfg)
		}
		env["LOKI_PASSWORD"] = "pw-vendor"
		env[EnvPrefix+"TENANT_ID"] = "tenant-grow"
		cfg, _ = ConfigFromEnvWith(Config{}, env)
		if cfg.Password.Reveal() != "pw-vendor" || cfg.TenantID != "tenant-grow" {
			t.Fatalf("precedence: %s", cfg)
		}
		cfg, _ = ConfigFromEnvWith(Config{CommonConfig: CommonConfig{URL: "https://explicit"}, TenantID: "x", Username: "e", Password: NewSecret("ep")}, env)
		if cfg.URL != "https://explicit" || cfg.TenantID != "x" || cfg.Username != "e" || cfg.Password.Reveal() != "ep" {
			t.Fatal("explicit must win")
		}
		cfg, err = ConfigFromEnvWith(Config{}, map[string]string{"LOKI_BEARER_TOKEN": "bearer-1"})
		if err != nil || cfg.BearerToken.Reveal() != "bearer-1" || cfg.URL != DefaultURL || cfg.RequestTimeout != DefaultRequestTimeout {
			t.Fatalf("bearer/defaults: %v %s", err, cfg)
		}
		cfg, _ = ConfigFromEnvWith(Config{}, map[string]string{EnvPrefix + "BEARER_TOKEN": "g", "LOKI_BEARER_TOKEN": "v"})
		if cfg.BearerToken.Reveal() != "g" {
			t.Fatal("GROW_ bearer must win")
		}
		_, err = ConfigFromEnvWith(Config{}, map[string]string{EnvPrefix + "TOTAL_TIMEOUT": "forever-xyz"})
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), EnvPrefix+"TOTAL_TIMEOUT") || strings.Contains(err.Error(), "forever-xyz") {
			t.Fatalf("invalid value: %v", err)
		}
		t.Setenv(EnvPrefix+"URL", "http://process-env:9")
		if cfg, err := ConfigFromEnv(); err != nil || cfg.URL != "http://process-env:9" {
			t.Fatalf("process env: %v %s", err, cfg.URL)
		}
	})

	t.Run("validate", func(t *testing.T) {
		bad := []Config{
			{CommonConfig: CommonConfig{URL: "ftp://x"}},
			{Username: "u"},
			{Password: NewSecret("p")},
			{Username: "u", Password: NewSecret("p"), BearerToken: NewSecret("b")},
			{TenantID: "a/b"},
			{TenantID: "."},
			{TenantID: ".."},
			{TenantID: strings.Repeat("a", 151)},
			{TenantID: "a|b"},
			{TenantID: "a|", MultiTenantQueries: true},
			{AllowedLabels: []string{"bad-label"}},
		}
		for _, c := range bad {
			if err := c.Validate(); !errors.Is(err, ErrConfig) {
				t.Errorf("%s: want CONFIG, got %v", c, err)
			}
		}
		good := []Config{
			{TenantID: "tenant_A-1.(x)*!'"},
			{TenantID: "a|b", MultiTenantQueries: true},
			{Username: "u", Password: NewSecret("p")},
			{BearerToken: NewSecret("b"), AllowedLabels: []string{"app"}},
		}
		for _, c := range good {
			if err := c.Validate(); err != nil {
				t.Errorf("%s: %v", c, err)
			}
		}
		if _, err := ConfigFromEnvWith(Config{}, map[string]string{"LOKI_USERNAME": "only-user"}); !errors.Is(err, ErrConfig) {
			t.Fatalf("user without password: %v", err)
		}
		if _, err := NewClient(Config{TenantID: "a/b"}); !errors.Is(err, ErrConfig) {
			t.Fatal("NewClient must validate")
		}
		if _, err := NewClient(Config{CommonConfig: CommonConfig{TLS: TLSConfig{CACertPEM: "bad"}}}); !errors.Is(err, ErrConfig) {
			t.Fatal("bad TLS")
		}
	})

	t.Run("core.redaction.config", func(t *testing.T) {
		cfg := Config{
			CommonConfig: CommonConfig{URL: "https://h", Headers: map[string]string{"X-Api-Key": "hdr-xyz"}},
			Username:     "user", Password: NewSecret("pw-xyz"), BearerToken: NewSecret("tok-xyz"),
		}
		forms := fmt.Sprintf("%v|%+v|%#v|%s", cfg, cfg, cfg, cfg.String())
		for _, s := range []string{"pw-xyz", "tok-xyz", "hdr-xyz"} {
			if strings.Contains(forms, s) {
				t.Fatalf("leaked %s: %s", s, forms)
			}
		}
		if !strings.Contains(forms, "Password:"+Redacted) {
			t.Fatal(forms)
		}
	})
}

func TestClientCore(t *testing.T) {
	ts := newTestServer(t, fixtureRoutes(t, map[string]string{
		pathReady:     "ready.ok",
		pathBuildInfo: "buildInfo.ok",
		pathLabels:    "ping.ok-tenant",
	}))

	t.Run("health ping version", func(t *testing.T) {
		c := newLoki(t, ts.URL, nil)
		hs, err := c.Health(t.Context())
		if err != nil || !hs.Healthy || hs.Status != "ok" || hs.Details["message"] != "ready" {
			t.Fatalf("health %+v %v", hs, err)
		}
		if ok, err := c.Ready(t.Context()); err != nil || !ok {
			t.Fatalf("ready %v %v", ok, err)
		}
		if d, err := c.Ping(t.Context()); err != nil || d <= 0 {
			t.Fatalf("ping %s %v", d, err)
		}
		v, err := c.ServerVersion(t.Context())
		if err != nil || v != "3.7.10" {
			t.Fatalf("version %q %v", v, err)
		}
		bi, err := c.BuildInfo(t.Context())
		if err != nil || bi.Revision != "c792f585" || bi.Branch != "release-3.7.x" {
			t.Fatalf("buildinfo %+v %v", bi, err)
		}
		if c.Config().URL != ts.URL || c.TenantID() != "" {
			t.Fatal("Config()")
		}
		r := ts.request(int(ts.calls.Load()) - 1)
		if r.Header.Get("User-Agent") != UserAgent {
			t.Fatalf("UA %q", r.Header.Get("User-Agent"))
		}
	})

	t.Run("not ready", func(t *testing.T) {
		srv := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Ingester not ready: waiting for 15s after being ready\n"))
		})
		c := newLoki(t, srv.URL, nil)
		hs, err := c.Health(t.Context())
		if err != nil || hs.Healthy || !strings.HasPrefix(hs.Status, "Ingester not ready") || hs.Details["httpStatus"] != "503" {
			t.Fatalf("health %+v %v", hs, err)
		}
		if ok, err := c.Ready(t.Context()); err != nil || ok {
			t.Fatalf("ready %v %v", ok, err)
		}
		empty := newTestServer(t, status(http.StatusInternalServerError))
		ce := newLoki(t, empty.URL, nil)
		if hs, _ := ce.Health(t.Context()); hs.Healthy || hs.Status != "unhealthy" {
			t.Fatalf("empty body: %+v", hs)
		}
		if _, err := ce.Ready(t.Context()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("ready 500: %v", err)
		}
	})

	t.Run("auth headers and tenant", func(t *testing.T) {
		c := newLoki(t, ts.URL, func(cfg *Config) { cfg.Username, cfg.Password = "sdk-user", NewSecret("sdk-pass") })
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		r := ts.request(int(ts.calls.Load()) - 1)
		if u, p, ok := r.BasicAuth(); !ok || u != "sdk-user" || p != "sdk-pass" || r.Header.Get(HeaderTenant) != "" {
			t.Fatalf("basic: %v", r.Header)
		}
		b := newLoki(t, ts.URL, func(cfg *Config) { cfg.BearerToken = NewSecret("bearer-xyz") })
		tc, err := b.WithTenant("tenant-a")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tc.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		r = ts.request(int(ts.calls.Load()) - 1)
		if r.Header.Get("Authorization") != "Bearer bearer-xyz" || r.Header.Get(HeaderTenant) != "tenant-a" || tc.TenantID() != "tenant-a" || b.TenantID() != "" {
			t.Fatalf("bearer/tenant: %v", r.Header)
		}
		if _, err := b.WithTenant("a|b"); !errors.Is(err, ErrValidation) {
			t.Fatalf("pipe tenant: %v", err)
		}
		if _, err := b.WithTenant(""); !errors.Is(err, ErrValidation) {
			t.Fatalf("empty tenant: %v", err)
		}
		// Closing the derived client leaves the parent usable.
		_ = tc.Close()
		if _, err := tc.Ping(t.Context()); !errors.Is(err, ErrCancelled) {
			t.Fatalf("derived after close: %v", err)
		}
		if _, err := b.Ping(t.Context()); err != nil {
			t.Fatalf("parent after derived close: %v", err)
		}
	})

	t.Run("authentication errors", func(t *testing.T) {
		gw := newTestServer(t, serveFixture(t, "ping.gateway-bad-credentials"))
		c := newLoki(t, gw.URL, func(cfg *Config) { cfg.Username, cfg.Password = "u", NewSecret("wrong-password") })
		_, err := c.Ping(t.Context())
		if !errors.Is(err, ErrAuthentication) || strings.Contains(err.Error(), "wrong-password") {
			t.Fatalf("gateway: %v", err)
		}
		mt := newTestServer(t, serveFixture(t, "ping.no-tenant"))
		_, err = newLoki(t, mt.URL, nil).Ping(t.Context())
		if !errors.Is(err, ErrAuthentication) || !strings.Contains(err.Error(), "no org id") {
			t.Fatalf("no tenant: %v", err)
		}
	})

	t.Run("server version errors", func(t *testing.T) {
		nf := newTestServer(t, serveFixture(t, "http.not-found"))
		if _, err := newLoki(t, nf.URL, nil).ServerVersion(t.Context()); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("404: %v", err)
		}
		bad := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) { _, _ = w.Write([]byte("nope")) })
		if _, err := newLoki(t, bad.URL, nil).ServerVersion(t.Context()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("decode: %v", err)
		}
	})

	t.Run("core.close.idempotent", func(t *testing.T) {
		c := newLoki(t, ts.URL, nil)
		if first, second := c.Close(), c.Close(); first != nil || second != nil {
			t.Fatal("close")
		}
		if _, err := c.Ping(t.Context()); !errors.Is(err, ErrCancelled) {
			t.Fatalf("after close: %v", err)
		}
		ran := false
		c.onClose(func() { ran = true })
		if !ran {
			t.Fatal("onClose after close must run at once")
		}
	})

	t.Run("core.health.unreachable", func(t *testing.T) {
		c := newLoki(t, "http://"+closedAddr(t), func(cfg *Config) { cfg.ConnectTimeout = time.Second })
		if hs, err := c.Health(t.Context()); err != nil || hs.Healthy || hs.Status != "unreachable" {
			t.Fatalf("hs=%+v err=%v", hs, err)
		}
		if _, err := c.Ready(t.Context()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("ready: %v", err)
		}
		ctx, cancel := contextCancelled(t)
		cancel()
		if _, err := c.Health(ctx); !errors.Is(err, ErrCancelled) {
			t.Fatalf("cancelled: %v", err)
		}
	})

	t.Run("from env", func(t *testing.T) {
		t.Setenv(EnvPrefix+"URL", ts.URL)
		c, err := NewClientFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Setenv(EnvPrefix+"URL", "mailto:x")
		if _, err := NewClientFromEnv(); !errors.Is(err, ErrConfig) {
			t.Fatalf("bad env: %v", err)
		}
	})
}

func TestIngesterShuttingDown(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("rpc error: code = Unknown desc = Ingester is shutting down\n"))
	})
	c := newLoki(t, srv.URL, func(cfg *Config) { cfg.Retry = fastRetry(1) })
	err := c.Push(t.Context(), []Stream{{Labels: map[string]string{"service_name": "x"}, Entries: []Entry{{Line: "l"}}}}, PushOptions{})
	e := asError(t, err)
	if e.Code != ErrorCodeUnavailable || !e.Retryable || !strings.Contains(e.Message, "WAL disk") {
		t.Fatalf("%+v", e)
	}
	if srv.calls.Load() != 2 {
		t.Fatalf("push must be retried (idempotent): %d calls", srv.calls.Load())
	}
	if got := mapLokiError(errors.New("plain")); got.Error() != "plain" {
		t.Fatal("non-SDK errors pass through")
	}
}
