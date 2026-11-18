package loki

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func sampleRecords() []LogRecord {
	flags := uint8(1)
	at := time.Unix(1795035000, 123)
	return []LogRecord{
		{
			Timestamp: at, ObservedTimestamp: at.Add(time.Millisecond), Severity: SeverityError, Body: "boom",
			Attributes: map[string]AttrValue{"request.id": "r-1", "status": 503, "nested": map[string]AttrValue{"k": []AttrValue{true, 1.5}}},
			Resource:   map[string]AttrValue{"service.name": "api", "deployment.environment": "prod", "service.version": "1.2.3"},
			TraceID:    "4BF92F3577B34DA6A3CE929D0E0E4736", SpanID: "00F067AA0BA902B7", TraceFlags: &flags,
		},
		{Timestamp: at, Body: "hello", Resource: map[string]AttrValue{"service.name": "api", "deployment.environment": "prod", "service.version": "1.2.3"}},
		{Timestamp: at, Severity: SeverityWarn + 1, SeverityText: "warning", Body: "other", Resource: map[string]AttrValue{"service.name": "web"}, Attributes: map[string]AttrValue{"level": "custom"}},
	}
}

func TestRecordsToStreams(t *testing.T) {
	streams := RecordsToStreams(sampleRecords(), nil)
	if len(streams) != 3 {
		t.Fatalf("streams %+v", streams)
	}
	byLine := map[string]Stream{}
	for _, s := range streams {
		byLine[s.Entries[0].Line] = s
	}
	boom := byLine["boom"]
	md := boom.Entries[0].StructuredMetadata
	if boom.Labels["service_name"] != "api" || boom.Labels["deployment_environment"] != "prod" || boom.Labels["level"] != "error" || boom.Labels["service_version"] != "" {
		t.Fatalf("labels %v", boom.Labels)
	}
	if md["service_version"] != "1.2.3" || md["request_id"] != "r-1" || md["status"] != "503" || md["nested"] != `{"k":[true,1.5]}` ||
		md[MetadataTraceID] != "4bf92f3577b34da6a3ce929d0e0e4736" || md[MetadataSpanID] != "00f067aa0ba902b7" || md[MetadataTraceFlags] != "1" ||
		md[MetadataSeverityText] != "ERROR" || md[MetadataSeverityNumber] != "17" || md[MetadataObservedTimestamp] != "1795035000001000123" {
		t.Fatalf("metadata %v", md)
	}
	if hello := byLine["hello"]; hello.Labels["level"] != "info" || hello.Entries[0].StructuredMetadata[MetadataSeverityNumber] != "9" {
		t.Fatalf("hello %+v", hello)
	}
	other := byLine["other"]
	if other.Labels["level"] != "custom" || other.Entries[0].StructuredMetadata[MetadataSeverityText] != "warning" {
		t.Fatalf("other %+v", other)
	}
	// Records with the same label set share one stream.
	same := RecordsToStreams(sampleRecords()[:2], NewCardinalityGuard(nil))
	if len(same) != 2 {
		t.Fatalf("api error and api info differ only by level: %d", len(same))
	}
}

func TestPushExporter(t *testing.T) {
	var bodies [][]byte
	var headers []http.Header
	ok := loadFixture(t, "push.json")
	rejected := loadFixture(t, "push.too-old")
	ts := newTestServer(t, func(w http.ResponseWriter, r *http.Request, n int) {
		b, _ := io.ReadAll(r.Body)
		bodies, headers = append(bodies, b), append(headers, r.Header.Clone())
		if n == 2 {
			rejected.write(w)
			return
		}
		ok.write(w)
	})
	c := newLoki(t, ts.URL, nil)
	exp := NewPushExporter(c, "")
	res, err := exp.Export(t.Context(), sampleRecords(), ExportOptions{Gzip: true})
	if err != nil || res.Accepted != 3 {
		t.Fatalf("%+v %v", res, err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(bodies[0]))
	if err != nil || headers[0].Get("Content-Encoding") != "gzip" {
		t.Fatal(err)
	}
	plain, _ := io.ReadAll(zr)
	if body := decodePushBody(t, plain); len(body.Streams) != 3 {
		t.Fatalf("%s", plain)
	}
	res, err = exp.Export(t.Context(), sampleRecords()[:1], ExportOptions{})
	if !errors.Is(err, ErrValidation) || res.Rejected != 1 || res.Error == nil {
		t.Fatalf("rejected: %+v %v", res, err)
	}
	if res, err := exp.Export(t.Context(), nil, ExportOptions{}); err != nil || res.Accepted != 0 {
		t.Fatal(err)
	}
	if exp.Shutdown(t.Context()) != nil {
		t.Fatal("shutdown")
	}
	down := newLoki(t, "http://"+closedAddr(t), func(cfg *Config) { cfg.Retry = fastRetry(0) })
	if res, err := NewPushExporter(down, PushFormatProtobuf).Export(t.Context(), sampleRecords(), ExportOptions{}); !errors.Is(err, ErrUnavailable) || res.Rejected != 0 {
		t.Fatalf("retryable errors are not rejections: %+v %v", res, err)
	}
}

func TestOTLPExporter(t *testing.T) {
	var body []byte
	var hdr http.Header
	ts := newTestServer(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		body, _ = io.ReadAll(r.Body)
		hdr = r.Header.Clone()
		if r.URL.Path != pathOTLPLogs {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	c := newLoki(t, ts.URL, func(cfg *Config) { cfg.TenantID = "t1" })
	exp := NewOTLPExporter(c)
	recs := sampleRecords()
	recs = append(recs, LogRecord{Body: "zero time", Attributes: map[string]AttrValue{"x": nil}})
	res, err := exp.Export(t.Context(), recs, ExportOptions{})
	if err != nil || res.Accepted != 4 || hdr.Get(HeaderTenant) != "t1" || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("%+v %v %v", res, err, hdr)
	}
	var req struct {
		ResourceLogs []struct {
			Resource struct {
				Attributes []otlpKV `json:"attributes"`
			} `json:"resource"`
			ScopeLogs []struct {
				LogRecords []map[string]any `json:"logRecords"`
			} `json:"scopeLogs"`
		} `json:"resourceLogs"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.ResourceLogs) != 3 {
		t.Fatalf("groups %s", body)
	}
	lr := req.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if lr["traceId"] != "4bf92f3577b34da6a3ce929d0e0e4736" || lr["severityNumber"] != float64(17) || lr["flags"] != float64(1) ||
		lr["timeUnixNano"] != "1795035000000000123" || lr["observedTimeUnixNano"] == nil {
		t.Fatalf("record %v", lr)
	}
	attrs, _ := json.Marshal(lr["attributes"])
	if !bytes.Contains(attrs, []byte(`{"key":"status","value":{"intValue":"503"}}`)) || !bytes.Contains(attrs, []byte(`"kvlistValue"`)) ||
		!bytes.Contains(attrs, []byte(`{"boolValue":true}`)) || !bytes.Contains(attrs, []byte(`{"doubleValue":1.5}`)) {
		t.Fatalf("attributes %s", attrs)
	}
	if _, err := exp.Export(t.Context(), recs[:1], ExportOptions{Gzip: true}); err != nil || hdr.Get("Content-Encoding") != "gzip" {
		t.Fatal(err)
	}
	if res, err := exp.Export(t.Context(), nil, ExportOptions{}); err != nil || res.Accepted != 0 || exp.Shutdown(t.Context()) != nil {
		t.Fatal(err)
	}
	if otlpAny(struct{}{})["stringValue"] == nil {
		t.Fatal("fallback")
	}
	for name, code := range map[string]ErrorCode{"otlpLogs.empty-json": ErrorCodeValidation, "otlpLogs.mt-no-tenant": ErrorCodeAuthentication, "otlpLogs.invalid-body": ErrorCodeValidation} {
		srv := newTestServer(t, serveFixture(t, name))
		res, err := NewOTLPExporter(newLoki(t, srv.URL, nil)).Export(t.Context(), recs, ExportOptions{})
		if CodeOf(err) != code || res.Rejected != 4 {
			t.Errorf("%s: %+v %v", name, res, err)
		}
	}
}

func TestLogClients(t *testing.T) {
	var pushes, otlp int
	ts := newTestServer(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		switch r.URL.Path {
		case pathPush:
			pushes++
		case pathOTLPLogs:
			otlp++
		}
		w.WriteHeader(http.StatusNoContent)
	})
	c, err := NewClient(Config{CommonConfig: CommonConfig{URL: ts.URL, Retry: fastRetry(0)}})
	if err != nil {
		t.Fatal(err)
	}
	opts := LogClientOptions{Batch: BatchOptions{FlushInterval: time.Hour}, Resource: map[string]AttrValue{"service.name": "svc"}}
	lc, err := c.NewLogClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	oc, err := c.NewOTLPLogClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	lc.Logger("test", nil).Info(context.Background(), "hello", "password", "hunter2")
	oc.Send(context.Background(), LogInput{Message: "otlp"})
	if err := lc.Flush(t.Context()); err != nil || pushes != 1 {
		t.Fatalf("flush %v %d", err, pushes)
	}
	lc.Send(context.Background(), LogInput{Message: "queued until close"})
	// Close of the client flushes every log client it created.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if pushes != 2 || otlp != 1 || lc.Stats().Sent != 2 || oc.Stats().Sent != 1 {
		t.Fatalf("pushes=%d otlp=%d stats=%+v", pushes, otlp, lc.Stats())
	}
	if _, err := c.NewLogClient(LogClientOptions{Batch: BatchOptions{MaxQueueSize: -1}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("bad options: %v", err)
	}
}
