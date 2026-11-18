package loki

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Loki API paths.
const (
	pathReady     = "/ready"
	pathBuildInfo = "/loki/api/v1/status/buildinfo"
	pathPush      = "/loki/api/v1/push"
	pathQuery     = "/loki/api/v1/query"
	pathRange     = "/loki/api/v1/query_range"
	pathLabels    = "/loki/api/v1/labels"
	pathSeries    = "/loki/api/v1/series"
	pathTail      = "/loki/api/v1/tail"
	pathStats     = "/loki/api/v1/index/stats"
	pathVolume    = "/loki/api/v1/index/volume"
	pathVolRange  = "/loki/api/v1/index/volume_range"
	pathFormat    = "/loki/api/v1/format_query"
	pathOTLPLogs  = "/otlp/v1/logs"
)

// Header names used by Loki.
const (
	// HeaderTenant carries the tenant id (Loki multi-tenancy).
	HeaderTenant = "X-Scope-OrgID"
	// HeaderEncodingFlags asks Loki to categorize labels in query responses.
	HeaderEncodingFlags = "X-Loki-Response-Encoding-Flags"
)

// ingesterShuttingDown is Loki's text when the WAL disk is full or the
// ingester is stopping; it is mapped to a retryable UNAVAILABLE error.
const ingesterShuttingDown = "Ingester is shutting down"

// Client is the Grafana Loki client. It is safe for concurrent use.
//
// Service functions hang off the client directly: Push, Query, QueryRange,
// Labels, LabelValues, Series, Tail, IndexStats, Volume, VolumeRange,
// FormatQuery, Ready and BuildInfo. The logging category is reached through
// NewLogClient / NewOTLPLogClient (LogClient) and LogQuery (LogQueryClient).
// WithTenant returns a client for another tenant sharing the connection pool.
type Client struct {
	cfg       Config
	transport *httpTransport
	owner     bool
	guard     *CardinalityGuard
	closeOnce sync.Once

	mu      sync.Mutex
	closers []func()
	closed  bool
}

// NewClient validates cfg and returns a client.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.CommonConfig = cfg.withDefaults(DefaultURL)
	t, err := newHTTPTransport(cfg.CommonConfig)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, transport: t, owner: true, guard: NewCardinalityGuard(cfg.allowedLabels())}
	for _, s := range []string{cfg.Password.Reveal(), cfg.BearerToken.Reveal()} {
		if len(s) >= 4 {
			t.addSecret(s)
		}
	}
	if cfg.Username != "" {
		t.addSecret(basicToken(cfg.Username, cfg.Password.Reveal()))
	}
	return c, nil
}

// NewClientFromEnv builds a client from the environment (see ConfigFromEnv).
func NewClientFromEnv() (*Client, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return NewClient(cfg)
}

// Config returns the effective configuration.
func (c *Client) Config() Config { return c.cfg }

// TenantID returns the tenant id sent as X-Scope-OrgID ("" for none).
func (c *Client) TenantID() string { return c.cfg.TenantID }

// WithTenant returns a client that sends X-Scope-OrgID: id on every request
// and on the tail handshake. It shares the connection pool and credentials;
// the original client is unchanged. A VALIDATION error reports an invalid id
// (see Config.TenantID). Closing the derived client stops only its own log
// clients and tails; the shared connections close with the original client.
func (c *Client) WithTenant(id string) (*Client, error) {
	if reason := tenantProblem(id, c.cfg.MultiTenantQueries); reason != "" {
		return nil, ValidationError("withTenant", "tenant id "+reason)
	}
	cfg := c.cfg
	cfg.TenantID = id
	return &Client{cfg: cfg, transport: c.transport, guard: c.guard}, nil
}

func basicToken(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
}

// headers returns the auth and tenant headers of every request.
func (c *Client) headers() http.Header {
	h := http.Header{}
	switch {
	case c.cfg.Username != "":
		h.Set("Authorization", "Basic "+basicToken(c.cfg.Username, c.cfg.Password.Reveal()))
	case !c.cfg.BearerToken.IsZero():
		h.Set("Authorization", "Bearer "+c.cfg.BearerToken.Reveal())
	}
	if c.cfg.TenantID != "" {
		h.Set(HeaderTenant, c.cfg.TenantID)
	}
	return h
}

// send executes req with the client headers and Loki error mapping.
func (c *Client) send(ctx context.Context, req *request) (*response, error) {
	if c.isClosed() {
		return nil, &Error{Code: ErrorCodeCancelled, Message: req.Operation + ": client is closed"}
	}
	h := c.headers()
	for k, vs := range req.Header {
		h[k] = vs
	}
	req.Header = h
	res, err := c.transport.do(ctx, req)
	if err != nil {
		return nil, mapLokiError(err)
	}
	return res, nil
}

// mapLokiError applies Loki-specific mappings: "Ingester is shutting down"
// (WAL disk at least 90 % full, or ingester stopping) is a retryable UNAVAILABLE.
func mapLokiError(err error) error {
	var e *Error
	if !errors.As(err, &e) {
		return err
	}
	if strings.Contains(e.Message, ingesterShuttingDown) {
		e.Code = ErrorCodeUnavailable
		e.Retryable = true
		if !strings.Contains(e.Message, "WAL") {
			e.Message += " (the Loki ingester is stopping or its WAL disk is at least 90 % full)"
		}
	}
	return e
}

// Health checks readiness (GET /ready). Healthy is true for HTTP 200; otherwise
// Status is Loki's text (for example "Ingester not ready: waiting for 15s
// after being ready") or "unreachable" on network errors. Configured
// credentials are sent because gateways may protect /ready. It fails only for
// CANCELLED and CONFIG.
func (c *Client) Health(ctx context.Context) (HealthStatus, error) {
	res, err := c.transport.do(ctx, &request{
		Method: http.MethodGet, Path: pathReady, Header: c.headers(), Operation: "health", NoRetry: true, RawStatus: true,
	})
	if err != nil {
		switch CodeOf(err) {
		case ErrorCodeCancelled, ErrorCodeConfig:
			return HealthStatus{}, err
		}
		return HealthStatus{Status: "unreachable", Details: map[string]string{"error": err.Error()}}, nil
	}
	text := truncate(strings.TrimSpace(c.transport.redact(firstLine(res.Body))), maxVendorText)
	hs := HealthStatus{Details: map[string]string{"httpStatus": strconv.Itoa(res.Status)}}
	if text != "" {
		hs.Details["message"] = text
	}
	if res.Status == http.StatusOK {
		hs.Healthy, hs.Status = true, "ok"
		return hs, nil
	}
	hs.Status = text
	if hs.Status == "" {
		hs.Status = "unhealthy"
	}
	return hs, nil
}

// Ready reports whether Loki answers GET /ready with 200. A not-ready server
// (HTTP 503 during startup) returns false without an error; network errors
// and other statuses return an error.
func (c *Client) Ready(ctx context.Context) (bool, error) {
	res, err := c.send(ctx, &request{Method: http.MethodGet, Path: pathReady, Operation: "ready", RawStatus: true, NoRetry: true})
	if err != nil {
		return false, err
	}
	switch res.Status {
	case http.StatusOK:
		return true, nil
	case http.StatusServiceUnavailable:
		return false, nil
	}
	return false, mapLokiError(StatusError("ready", res.Status, res.RequestID, firstLine(res.Body), c.transport.knownSecrets()))
}

// Ping makes an authenticated round trip (GET /loki/api/v1/labels?since=5m)
// and returns its latency. AUTHENTICATION on bad gateway credentials or a
// missing tenant on a multi-tenant Loki.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	if _, err := c.send(ctx, &request{Method: http.MethodGet, Path: pathLabels, Query: map[string][]string{"since": {"5m"}}, Operation: "ping"}); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// BuildInfo describes the Loki build (GET /loki/api/v1/status/buildinfo).
type BuildInfo struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

// BuildInfo returns the server build information. UNSUPPORTED when the
// endpoint does not exist (HTTP 404).
func (c *Client) BuildInfo(ctx context.Context) (BuildInfo, error) {
	var info BuildInfo
	res, err := c.send(ctx, &request{Method: http.MethodGet, Path: pathBuildInfo, Operation: "buildInfo"})
	if err != nil {
		if CodeOf(err) == ErrorCodeNotFound {
			return info, UnsupportedError("buildInfo", "the server exposes no build information")
		}
		return info, err
	}
	err = res.decodeJSON("buildInfo", &info)
	return info, err
}

// ServerVersion returns the Loki version (BuildInfo().Version).
func (c *Client) ServerVersion(ctx context.Context) (string, error) {
	info, err := c.BuildInfo(ctx)
	if err != nil {
		return "", err
	}
	return info.Version, nil
}

// onClose registers fn to run on Close (immediately when already closed).
func (c *Client) onClose(fn func()) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		fn()
		return
	}
	c.closers = append(c.closers, fn)
	c.mu.Unlock()
}

// Close flushes and closes the log clients created by this client, stops its
// tails and (for a client from NewClient) releases the connections. Later
// calls fail with CANCELLED. Close is idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		// Flush log clients and stop tails while the client still works.
		c.mu.Lock()
		closers := c.closers
		c.closers = nil
		c.mu.Unlock()
		for _, fn := range closers {
			safeCall(fn)
		}
		c.mu.Lock()
		c.closed = true
		late := c.closers
		c.closers = nil
		c.mu.Unlock()
		for _, fn := range late {
			safeCall(fn)
		}
		if c.owner {
			c.transport.close()
		}
	})
	return nil
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed || c.transport.closed.Load()
}

func firstLine(b []byte) string {
	line, _, _ := bytes.Cut(bytes.TrimSpace(b), []byte("\n"))
	return string(line)
}
