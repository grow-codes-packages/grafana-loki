package integration

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	loki "github.com/grow-codes-packages/grafana-loki/go"
)

// env reads the bootstrap file once and returns a variable.
func env(t *testing.T, name string) string {
	t.Helper()
	bootstrap := os.Getenv("GROW_BOOTSTRAP_ENV")
	if bootstrap == "" {
		bootstrap = "/bootstrap/env"
	}
	loadBootstrapEnv(t, bootstrap)
	if os.Getenv("GROW_GRAFANA_LOKI_URL") == "" {
		t.Skip("GROW_GRAFANA_LOKI_URL is not set; run through docker/compose.yml")
	}
	return os.Getenv(name)
}

func runID(t *testing.T) string {
	t.Helper()
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newClient(t *testing.T, cfg loki.Config) *loki.Client {
	t.Helper()
	c, err := loki.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func mustRange(t *testing.T, c *loki.Client, q string, min int) *loki.QueryResult {
	t.Helper()
	var res *loki.QueryResult
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		res, err = c.QueryRange(t.Context(), q, loki.QueryRangeOptions{Since: 15 * time.Minute, Limit: 1000, Direction: loki.DirectionForward})
		if err != nil {
			t.Fatalf("queryRange %s: %v", q, err)
		}
		if countEntries(res) >= min || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if countEntries(res) < min {
		t.Fatalf("queryRange %s: %d entries, want >= %d", q, countEntries(res), min)
	}
	return res
}

func countEntries(r *loki.QueryResult) int {
	n := 0
	for _, s := range r.Streams {
		n += len(s.Entries)
	}
	return n
}

func TestLokiCoreAndQuery(t *testing.T) {
	url := env(t, "GROW_GRAFANA_LOKI_URL")
	c, err := loki.NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx := t.Context()
	run := runID(t)
	svc := "grow-it-go-" + run

	// Core: health, ready, ping, version, build info.
	if hs, err := c.Health(ctx); err != nil || !hs.Healthy || hs.Status != "ok" {
		t.Fatalf("health %+v %v", hs, err)
	}
	if ok, err := c.Ready(ctx); err != nil || !ok {
		t.Fatalf("ready %v %v", ok, err)
	}
	if _, err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	want := env(t, "GROW_GRAFANA_LOKI_EXPECTED_VERSION")
	if v, err := c.ServerVersion(ctx); err != nil || (want != "" && v != want) {
		t.Fatalf("version %q %v (want %s)", v, err, want)
	}
	if bi, err := c.BuildInfo(ctx); err != nil || bi.Version == "" || bi.Revision == "" {
		t.Fatalf("buildinfo %+v %v", bi, err)
	}

	// Push: JSON (guard moves "run" to structured metadata), gzip and protobuf+snappy.
	now := time.Now()
	labels := map[string]string{"service_name": svc, "level": "info", "run": run}
	nasty := "quote\" back\\slash tab\t ü 😀 " + run
	err = c.Push(ctx, []loki.Stream{{Labels: labels, Entries: []loki.Entry{
		{Timestamp: now, Line: `{"msg":"json line ` + run + `","status":503}`, StructuredMetadata: map[string]string{"request_id": "req-" + run}},
		{Timestamp: now.Add(time.Millisecond), Line: `{"msg":"json ok ` + run + `","status":200}`},
		{Timestamp: now.Add(2 * time.Millisecond), Line: nasty},
	}}}, loki.PushOptions{})
	if err != nil {
		t.Fatalf("push json: %v", err)
	}
	if err := c.Push(ctx, []loki.Stream{{Labels: labels, Entries: []loki.Entry{{Timestamp: now.Add(3 * time.Millisecond), Line: "gzip line " + run}}}}, loki.PushOptions{Gzip: true}); err != nil {
		t.Fatalf("push gzip: %v", err)
	}
	err = c.Push(ctx, []loki.Stream{{Labels: map[string]string{"service_name": svc, "level": "error"}, Entries: []loki.Entry{
		{Timestamp: now.Add(4 * time.Millisecond), Line: "proto line " + run, StructuredMetadata: map[string]string{"run": run, "enc": "protobuf"}},
	}}}, loki.PushOptions{Format: loki.PushFormatProtobuf})
	if err != nil {
		t.Fatalf("push protobuf: %v", err)
	}
	sel := loki.Selector(map[string]string{"service_name": svc})
	res := mustRange(t, c, sel.String(), 5)
	var gotProto, gotMeta bool
	for _, s := range res.Streams {
		if s.Labels["run"] != "" {
			t.Fatalf("run must not be an index label: %v", s.Labels)
		}
		for _, e := range s.Entries {
			if e.Line == "proto line "+run && e.StructuredMetadata["enc"] == "protobuf" && s.Labels["level"] == "error" {
				gotProto = true
			}
			if e.StructuredMetadata["request_id"] == "req-"+run && e.StructuredMetadata["run"] == run {
				gotMeta = true
			}
		}
	}
	if !gotProto || !gotMeta {
		t.Fatalf("round trip proto=%v meta=%v: %+v", gotProto, gotMeta, res.Streams)
	}

	// LogQL builder: escaping, json + where, structured metadata filter; FormatQuery parses it.
	q := sel.LineContains("json line").JSON().Where("status", ">=", 500).String()
	if formatted, err := c.FormatQuery(ctx, q); err != nil || !strings.Contains(formatted, "status>=500") {
		t.Fatalf("formatQuery %q %v", formatted, err)
	}
	if r := mustRange(t, c, q, 1); countEntries(r) != 1 || r.Streams[0].Entries[0].Parsed["status"] != "503" {
		t.Fatalf("json where: %+v", r.Streams)
	}
	if r := mustRange(t, c, sel.LineContains(nasty).String(), 1); r.Streams[0].Entries[0].Line != nasty {
		t.Fatal("escaped literal round trip")
	}
	if r := mustRange(t, c, sel.Where("run", "=", run).Where("request_id", "=", "req-"+run).String(), 1); countEntries(r) != 1 {
		t.Fatal("structured metadata filter")
	}
	if r, err := c.QueryRange(ctx, sel.String(), loki.QueryRangeOptions{Since: 15 * time.Minute, Limit: 2}); err != nil || countEntries(r) != 2 {
		t.Fatalf("limit: %v", err)
	}
	if _, err := c.FormatQuery(ctx, `{service_name=`); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("formatQuery parse error: %v", err)
	}

	// Metric queries.
	mq := `sum(count_over_time(` + sel.String() + `[10m]))`
	inst, err := c.Query(ctx, mq, loki.QueryOptions{})
	if err != nil || inst.ResultType != loki.ResultTypeVector || len(inst.Vector) != 1 || inst.Vector[0].Value.Value < 5 {
		t.Fatalf("instant %+v %v", inst, err)
	}
	if _, err := c.Query(ctx, sel.String(), loki.QueryOptions{}); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("log selector as instant query: %v", err)
	}
	mat, err := c.QueryRange(ctx, mq, loki.QueryRangeOptions{Since: 10 * time.Minute, Step: time.Minute})
	if err != nil || mat.ResultType != loki.ResultTypeMatrix || len(mat.Matrix) != 1 {
		t.Fatalf("matrix %+v %v", mat, err)
	}

	// Labels, label values, series.
	ls, err := c.Labels(ctx, loki.LabelsOptions{Since: 15 * time.Minute})
	if err != nil || !slices.Contains(ls, "service_name") || slices.Contains(ls, "request_id") {
		t.Fatalf("labels %v %v", ls, err)
	}
	vals, err := c.LabelValues(ctx, "level", loki.LabelsOptions{Since: 15 * time.Minute, Query: sel.String()})
	if err != nil || !slices.Contains(vals, "error") || !slices.Contains(vals, "info") {
		t.Fatalf("label values %v %v", vals, err)
	}
	series, err := c.Series(ctx, []string{sel.String()}, loki.SeriesOptions{Since: 15 * time.Minute})
	if err != nil || len(series) != 2 || series[0]["service_name"] != svc {
		t.Fatalf("series %v %v", series, err)
	}

	// Volume (ingester data), then flush chunks for the index-only APIs.
	vol, err := c.Volume(ctx, sel.String(), loki.VolumeOptions{Start: now.Add(-10 * time.Minute), End: time.Now().Add(time.Minute), TargetLabels: []string{"level"}, AggregateBy: "labels"})
	if err != nil || len(vol.Vector) == 0 || vol.Vector[0].Value.Value <= 0 {
		t.Fatalf("volume %s %d %v", vol.ResultType, len(vol.Vector), err)
	}
	vr, err := c.VolumeRange(ctx, sel.String(), loki.VolumeOptions{Start: now.Add(-10 * time.Minute), End: time.Now().Add(time.Minute), Step: time.Minute})
	if err != nil || (len(vr.Matrix) == 0 && len(vr.Vector) == 0) {
		t.Fatalf("volume range %s %d/%d %v", vr.ResultType, len(vr.Matrix), len(vr.Vector), err)
	}
	flush(t, url)
	var st loki.IndexStats
	for i := 0; i < 40; i++ {
		st, err = c.IndexStats(ctx, sel.String(), loki.IndexOptions{Start: now.Add(-10 * time.Minute), End: time.Now().Add(time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		if st.Entries >= 5 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if st.Streams < 1 || st.Entries < 5 {
		t.Fatalf("index stats %+v", st)
	}

	// Errors.
	if err := c.Push(ctx, []loki.Stream{{Labels: labels, Entries: []loki.Entry{{Timestamp: now.Add(-30 * 24 * time.Hour), Line: "old"}}}}, loki.PushOptions{}); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("too old: %v", err)
	}
	if _, err := c.QueryRange(ctx, `{service_name=`, loki.QueryRangeOptions{}); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("parse error: %v", err)
	}
}

func flush(t *testing.T, base string) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, strings.TrimSuffix(base, "/")+"/flush", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	_ = resp.Body.Close()
}

func TestLokiTail(t *testing.T) {
	env(t, "GROW_GRAFANA_LOKI_URL")
	c, err := loki.NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	run := runID(t)
	svc := "grow-it-tail-" + run
	sel := loki.Selector(map[string]string{"service_name": svc}).String()

	frames := make(chan loki.TailFrame, 8)
	s, err := c.Tail(t.Context(), sel, func(f loki.TailFrame) { frames <- f }, loki.LiveTailOptions{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := c.Push(t.Context(), []loki.Stream{{Labels: map[string]string{"service_name": svc}, Entries: []loki.Entry{{Line: "tail marker " + run, StructuredMetadata: map[string]string{"k": "v"}}}}}, loki.PushOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-frames:
		e := f.Streams[0].Entries[0]
		if e.Line != "tail marker "+run || e.StructuredMetadata["k"] != "v" || f.Streams[0].Labels["service_name"] != svc {
			t.Fatalf("frame %+v", f)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no tail frame")
	}
	s.Stop()
	if s.Err() != nil {
		t.Fatal(s.Err())
	}

	// LogQuery.Tail (category interface) with a free-text query.
	recs := make(chan loki.LogRecord, 8)
	h, err := c.LogQuery().Tail(t.Context(), "category tail "+run, func(r loki.LogRecord) { recs <- r }, loki.TailOptions{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := c.Push(t.Context(), []loki.Stream{{Labels: map[string]string{"service_name": svc, "level": "warn"}, Entries: []loki.Entry{{Line: "category tail " + run}}}}, loki.PushOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-recs:
		if r.Body != "category tail "+run || r.Resource["service.name"] != svc || r.Severity != loki.SeverityWarn {
			t.Fatalf("record %+v", r)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no tail record")
	}
	h.Stop()

	// Loki closes a metric-query tail; the SDK rejects it before dialing.
	if _, err := c.Tail(t.Context(), "count_over_time("+sel+"[1m])", func(loki.TailFrame) {}, loki.LiveTailOptions{}); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("metric tail: %v", err)
	}
	if _, err := c.Tail(t.Context(), `{service_name=`, func(loki.TailFrame) {}, loki.LiveTailOptions{}); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("bad tail query: %v", err)
	}
}

func TestLokiLogging(t *testing.T) {
	env(t, "GROW_GRAFANA_LOKI_URL")
	c, err := loki.NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	run := runID(t)
	svc := "grow-it-log-" + run
	lq := c.LogQuery()
	opts := loki.LogClientOptions{Resource: map[string]loki.AttrValue{"service.name": svc, "deployment.environment": "it", "service.version": "1.0.0"}}

	// Push exporter (JSON): send, logger facade, slog handler, flush, search back.
	lc, err := c.NewLogClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	lc.Send(ctx, loki.LogInput{Message: "send " + run, Severity: loki.SeverityError, Attributes: map[string]loki.AttrValue{"run": run, "n": 7, "password": "hunter2"},
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7"})
	lc.SendBatch(ctx, loki.Entries([]loki.LogInput{{Message: "batch-0 " + run}, {Message: "batch-1 " + run}}))
	lc.Logger("it", map[string]loki.AttrValue{"component": "logger"}).Child(map[string]loki.AttrValue{"child": true}).Warn(ctx, "logger "+run, "k", "v")
	slog.New(loki.NewSlogHandler(lc, nil)).InfoContext(ctx, "slog "+run, "user", "u1")
	if err := lc.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if st := lc.Stats(); st.Sent != 5 || st.Failed != 0 {
		t.Fatalf("stats %+v", st)
	}
	recs, err := loki.WaitForLogs(ctx, lq, run, 5, loki.WaitForLogOptions{Timeout: 30 * time.Second, Interval: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var sent loki.LogRecord
	for _, r := range recs {
		if r.Body == "send "+run {
			sent = r
		}
	}
	if sent.Severity != loki.SeverityError || sent.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || sent.SpanID != "00f067aa0ba902b7" ||
		sent.Attributes["n"] != "7" || sent.Attributes["password"] != loki.Redacted || sent.Resource["service.name"] != svc ||
		sent.Resource["deployment.environment"] != "it" || sent.Attributes["service_version"] != "1.0.0" {
		t.Fatalf("normalized record %+v", sent)
	}

	// Search order + pagination with cursors, WaitForLog with a predicate.
	page, err := lq.Search(ctx, loki.Selector(map[string]string{"service_name": svc}).String(), loki.SearchOptions{Limit: 2, Order: loki.OrderAsc})
	if err != nil || len(page.Records) != 2 || page.NextCursor == "" || page.Records[0].Timestamp.After(page.Records[1].Timestamp) {
		t.Fatalf("page %+v %v", page, err)
	}
	all, err := loki.SearchAll(ctx, lq, loki.Selector(map[string]string{"service_name": svc}).String(), loki.SearchOptions{Limit: 2}, 0)
	if err != nil || len(all) != 5 {
		t.Fatalf("search all %d %v", len(all), err)
	}
	r, err := lq.WaitForLog(ctx, run, loki.WaitForLogOptions{Timeout: 10 * time.Second, Predicate: func(r loki.LogRecord) bool { return strings.HasPrefix(r.Body, "slog") }})
	if err != nil || r.Attributes["user"] != "u1" {
		t.Fatalf("waitForLog %+v %v", r, err)
	}

	// OTLP exporter and protobuf push exporter.
	oc, err := c.NewOTLPLogClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	oc.Send(ctx, loki.LogInput{Message: "otlp " + run, Severity: loki.SeverityWarn, Attributes: map[string]loki.AttrValue{"request.id": "r-" + run, "code": 503},
		TraceID: "0af7651916cd43dd8448eb211c80319c", SpanID: "b7ad6b7169203331"})
	pc, err := c.NewLogClientWithExporter(loki.NewPushExporter(c, loki.PushFormatProtobuf), opts)
	if err != nil {
		t.Fatal(err)
	}
	pc.Send(ctx, loki.LogInput{Message: "protobuf exporter " + run})
	if err := oc.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pc.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if oc.Stats().Sent != 1 || pc.Stats().Sent != 1 {
		t.Fatalf("otlp %+v protobuf %+v", oc.Stats(), pc.Stats())
	}
	otlp, err := lq.WaitForLog(ctx, `{service_name="`+svc+`"} | trace_id="0af7651916cd43dd8448eb211c80319c"`, loki.WaitForLogOptions{Timeout: 30 * time.Second, Interval: 300 * time.Millisecond})
	if err != nil || otlp.Body != "otlp "+run || otlp.Attributes["request_id"] != "r-"+run || otlp.Severity != loki.SeverityWarn || otlp.SpanID != "b7ad6b7169203331" {
		t.Fatalf("otlp record %+v %v", otlp, err)
	}
	if _, err := lq.WaitForLog(ctx, "protobuf exporter "+run, loki.WaitForLogOptions{Timeout: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}

	// Streams and labels.
	streams, err := lq.ListStreams(ctx)
	if err != nil || !slices.ContainsFunc(streams, func(s string) bool { return strings.Contains(s, `service_name="`+svc+`"`) }) {
		t.Fatalf("streams %v", err)
	}
	labels, err := lq.ListLabels(ctx)
	if err != nil || !slices.Contains(labels, "deployment_environment") {
		t.Fatalf("labels %v %v", labels, err)
	}
	if _, err := lq.WaitForLog(ctx, "never-"+run, loki.WaitForLogOptions{Timeout: 700 * time.Millisecond, Interval: 200 * time.Millisecond}); !errors.Is(err, loki.ErrTimeout) {
		t.Fatalf("timeout: %v", err)
	}

	// Close flushes queued records.
	lc.Send(ctx, loki.LogInput{Message: "on close " + run})
	if err := c.Close(); err != nil || c.Close() != nil {
		t.Fatal("close")
	}
	c2, _ := loki.NewClientFromEnv()
	defer func() { _ = c2.Close() }()
	if _, err := c2.LogQuery().WaitForLog(ctx, "on close "+run, loki.WaitForLogOptions{Timeout: 30 * time.Second}); err != nil {
		t.Fatalf("close must flush: %v", err)
	}
}

func TestLokiMultiTenant(t *testing.T) {
	mt := env(t, "GROW_GRAFANA_LOKI_MULTI_TENANT_URL")
	ta, tb := env(t, "GROW_GRAFANA_LOKI_TENANT_A"), env(t, "GROW_GRAFANA_LOKI_TENANT_B")
	if mt == "" || ta == "" || tb == "" {
		t.Skip("multi-tenant Loki not configured")
	}
	ctx := t.Context()
	run := runID(t)
	c := newClient(t, loki.Config{CommonConfig: loki.CommonConfig{URL: mt}})
	if hs, err := c.Health(ctx); err != nil || !hs.Healthy {
		t.Fatalf("health without tenant %+v %v", hs, err)
	}
	if _, err := c.Ping(ctx); !errors.Is(err, loki.ErrAuthentication) {
		t.Fatalf("no tenant: %v", err)
	}
	stream := []loki.Stream{{Labels: map[string]string{"service_name": "mt-" + run}, Entries: []loki.Entry{{Line: "tenant line " + run}}}}
	if err := c.Push(ctx, stream, loki.PushOptions{}); !errors.Is(err, loki.ErrAuthentication) {
		t.Fatalf("push without tenant: %v", err)
	}
	a, err := c.WithTenant(ta)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.WithTenant(tb)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Push(ctx, stream, loki.PushOptions{}); err != nil {
		t.Fatal(err)
	}
	sel := `{service_name="mt-` + run + `"}`
	mustRange(t, a, sel, 1)
	if r, err := b.QueryRange(ctx, sel, loki.QueryRangeOptions{Since: 15 * time.Minute}); err != nil || countEntries(r) != 0 {
		t.Fatalf("tenant B must not see tenant A data: %v", err)
	}
	if vals, err := b.LabelValues(ctx, "service_name", loki.LabelsOptions{Since: 15 * time.Minute}); err != nil || slices.Contains(vals, "mt-"+run) {
		t.Fatalf("tenant B label values %v %v", vals, err)
	}
	// Tenant header on the tail handshake.
	frames := make(chan loki.TailFrame, 2)
	s, err := a.Tail(ctx, sel, func(f loki.TailFrame) { frames <- f }, loki.LiveTailOptions{Start: time.Now().Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-frames:
	case <-time.After(20 * time.Second):
		t.Fatal("tenant tail got nothing")
	}
	s.Stop()
	if _, err := c.Tail(ctx, sel, func(loki.TailFrame) {}, loki.LiveTailOptions{}); !errors.Is(err, loki.ErrAuthentication) {
		t.Fatalf("tail without tenant: %v", err)
	}
	if _, err := c.WithTenant(ta + "|" + tb); !errors.Is(err, loki.ErrValidation) {
		t.Fatalf("pipe tenant: %v", err)
	}
	if _, err := loki.NewClient(loki.Config{CommonConfig: loki.CommonConfig{URL: mt}, TenantID: "bad/tenant"}); !errors.Is(err, loki.ErrConfig) {
		t.Fatalf("invalid tenant config: %v", err)
	}
}

func TestLokiGateway(t *testing.T) {
	gw := env(t, "GROW_GRAFANA_LOKI_AUTH_PROXY_URL")
	user, pass := env(t, "GROW_GRAFANA_LOKI_AUTH_PROXY_USERNAME"), env(t, "GROW_GRAFANA_LOKI_AUTH_PROXY_PASSWORD")
	bearer := env(t, "GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN")
	if gw == "" {
		t.Skip("auth gateway not configured")
	}
	ctx := t.Context()
	basic := newClient(t, loki.Config{CommonConfig: loki.CommonConfig{URL: gw}, Username: user, Password: loki.NewSecret(pass)})
	if _, err := basic.Ping(ctx); err != nil {
		t.Fatalf("basic: %v", err)
	}
	run := runID(t)
	bc := newClient(t, loki.Config{CommonConfig: loki.CommonConfig{URL: gw}, BearerToken: loki.NewSecret(bearer)})
	if err := bc.Push(ctx, []loki.Stream{{Labels: map[string]string{"service_name": "gw-" + run}, Entries: []loki.Entry{{Line: "gateway " + run}}}}, loki.PushOptions{Gzip: true}); err != nil {
		t.Fatalf("bearer push: %v", err)
	}
	mustRange(t, bc, `{service_name="gw-`+run+`"}`, 1)

	var mu sync.Mutex
	var seen []string
	wrong := newClient(t, loki.Config{CommonConfig: loki.CommonConfig{URL: gw, Hooks: &loki.Hooks{OnRequest: func(i loki.RequestInfo) {
		mu.Lock()
		seen = append(seen, i.Headers.Get("Authorization"))
		mu.Unlock()
	}}}, Username: user, Password: loki.NewSecret("wrong-" + run)})
	_, err := wrong.Ping(ctx)
	if !errors.Is(err, loki.ErrAuthentication) || strings.Contains(err.Error(), "wrong-"+run) {
		t.Fatalf("wrong credentials: %v", err)
	}
	mu.Lock()
	if len(seen) == 0 || seen[0] != loki.Redacted {
		t.Fatalf("hooks must see redacted headers: %v", seen)
	}
	mu.Unlock()
	if _, err := newClient(t, loki.Config{CommonConfig: loki.CommonConfig{URL: gw}}).Tail(ctx, `{a="b"}`, func(loki.TailFrame) {}, loki.LiveTailOptions{}); !errors.Is(err, loki.ErrAuthentication) {
		t.Fatalf("tail without credentials: %v", err)
	}

	// Fault injection: 429 / 503 with Retry-After are retried, then surface typed errors.
	retry := &loki.RetryPolicy{MaxRetries: 1, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Multiplier: 2, MaxRetryAfter: 5 * time.Second}
	for fault, want := range map[string]error{"429": loki.ErrRateLimited, "503": loki.ErrUnavailable, "502": loki.ErrUnavailable} {
		attempts := 0
		fc := newClient(t, loki.Config{CommonConfig: loki.CommonConfig{
			URL: gw, Retry: retry, Headers: map[string]string{"X-Grow-Fault": fault},
			Hooks: &loki.Hooks{OnResponse: func(loki.ResponseInfo) { attempts++ }},
		}, BearerToken: loki.NewSecret(bearer)})
		_, err := fc.Labels(ctx, loki.LabelsOptions{})
		if !errors.Is(err, want) || attempts != 2 {
			t.Fatalf("fault %s: %v after %d attempts", fault, err, attempts)
		}
		var e *loki.Error
		if errors.As(err, &e) && fault != "502" && e.RetryAfter != time.Second {
			t.Fatalf("fault %s: retryAfter %s", fault, e.RetryAfter)
		}
	}
}
