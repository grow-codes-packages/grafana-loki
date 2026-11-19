package loki

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// fixture is a recorded request/response pair from the real Loki 3.7.10
// container (testdata/fixtures, recorded by the prep verify harness).
type fixture struct {
	Request struct {
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Query   map[string]any    `json:"query"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"request"`
	Response struct {
		Status      int             `json:"status"`
		ContentType string          `json:"contentType"`
		Body        json.RawMessage `json:"body"`
		Frame       json.RawMessage `json:"frame"`
		Payload     string          `json:"payload"`
	} `json:"response"`
}

func loadFixture(t *testing.T, name string) fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "fixtures", name+".json"))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return f
}

// body returns the recorded response body bytes.
func (f fixture) body() []byte {
	raw := f.Response.Body
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []byte(s)
	}
	var bin struct {
		Base64 string `json:"base64"`
	}
	if json.Unmarshal(raw, &bin) == nil && bin.Base64 != "" {
		b, _ := base64.StdEncoding.DecodeString(bin.Base64)
		return b
	}
	return raw
}

// write serves the recorded response.
func (f fixture) write(w http.ResponseWriter) {
	if f.Response.ContentType != "" {
		w.Header().Set("Content-Type", f.Response.ContentType)
	}
	status := f.Response.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(f.body())
}

// serveFixture returns a handler that always answers with fixture name.
func serveFixture(t *testing.T, name string) func(w http.ResponseWriter, r *http.Request, n int) {
	t.Helper()
	f := loadFixture(t, name)
	return func(w http.ResponseWriter, _ *http.Request, _ int) { f.write(w) }
}

// fixtureRoutes serves a fixture per request path (404 for others).
func fixtureRoutes(t *testing.T, routes map[string]string) func(w http.ResponseWriter, r *http.Request, n int) {
	t.Helper()
	loaded := map[string]fixture{}
	for path, name := range routes {
		loaded[path] = loadFixture(t, name)
	}
	notFound := loadFixture(t, "http.not-found")
	return func(w http.ResponseWriter, r *http.Request, _ int) {
		if f, ok := loaded[r.URL.Path]; ok {
			f.write(w)
			return
		}
		notFound.write(w)
	}
}

// newLoki builds a client for url with fast retries; mutate adjusts the config.
func newLoki(t *testing.T, url string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{CommonConfig: CommonConfig{URL: url, Retry: fastRetry(2)}}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func contextCancelled(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithCancel(t.Context())
}
