package loki

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
)

// captured is a request body recorded by a test handler.
type captured struct {
	header http.Header
	body   []byte
}

func captureServer(t *testing.T, fixtureName string) (*testServer, *[]captured) {
	t.Helper()
	f := loadFixture(t, fixtureName)
	var got []captured
	ts := newTestServer(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, captured{header: r.Header.Clone(), body: b})
		f.write(w)
	})
	return ts, &got
}

func decodePushBody(t *testing.T, b []byte) pushJSON {
	t.Helper()
	var body struct {
		Streams []struct {
			Stream map[string]string `json:"stream"`
			Values [][]any           `json:"values"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(b, &body); err != nil {
		t.Fatalf("push body: %v: %s", err, b)
	}
	out := pushJSON{}
	for _, s := range body.Streams {
		out.Streams = append(out.Streams, pushJSONStream{Stream: s.Stream, Values: s.Values})
	}
	return out
}

func TestPushJSON(t *testing.T) {
	ts, got := captureServer(t, "push.json-structured-metadata")
	c := newLoki(t, ts.URL, nil)
	at := time.Unix(0, 1795034767066713072)
	err := c.Push(t.Context(), []Stream{{
		Labels: map[string]string{"service_name": "sdk-verify", "env": "test", "level": "info"},
		Entries: []Entry{
			{Timestamp: at, Line: `{"msg":"x"}`, StructuredMetadata: map[string]string{"request_id": "req-1", "env": "override"}},
			{Line: "now"},
		},
	}}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	req := (*got)[0]
	if req.header.Get("Content-Type") != "application/json" || req.header.Get("Content-Encoding") != "" {
		t.Fatalf("headers %v", req.header)
	}
	body := decodePushBody(t, req.body)
	s := body.Streams[0]
	if s.Stream["service_name"] != "sdk-verify" || s.Stream["level"] != "info" || s.Stream["env"] != "" {
		t.Fatalf("guard must keep only allowlisted labels: %v", s.Stream)
	}
	v0 := s.Values[0]
	md := v0[2].(map[string]any)
	if v0[0] != "1795034767066713072" || md["request_id"] != "req-1" || md["env"] != "override" {
		t.Fatalf("entry 0: %v", v0)
	}
	if md1 := s.Values[1][2].(map[string]any); md1["env"] != "test" {
		t.Fatalf("moved label: %v", s.Values[1])
	}
	if ts1, _ := ParseUnixNano(s.Values[1][0].(string)); time.Since(ts1) > time.Minute {
		t.Fatal("zero timestamp must default to now")
	}

	t.Run("gzip", func(t *testing.T) {
		ts, got := captureServer(t, "push.json-gzip")
		c := newLoki(t, ts.URL, nil)
		if err := c.Push(t.Context(), []Stream{{Labels: map[string]string{"job": "j"}, Entries: []Entry{{Line: "z"}}}}, PushOptions{Gzip: true}); err != nil {
			t.Fatal(err)
		}
		req := (*got)[0]
		zr, err := gzip.NewReader(bytes.NewReader(req.body))
		if err != nil || req.header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("gzip: %v %v", err, req.header)
		}
		plain, _ := io.ReadAll(zr)
		if b := decodePushBody(t, plain); b.Streams[0].Stream["job"] != "j" || len(b.Streams[0].Values[0]) != 2 {
			t.Fatalf("gzip body %s", plain)
		}
	})

	t.Run("guard disabled and empty labels", func(t *testing.T) {
		ts, got := captureServer(t, "push.empty-labels")
		c := newLoki(t, ts.URL, func(cfg *Config) { cfg.DisableLabelGuard = true })
		err := c.Push(t.Context(), []Stream{
			{Labels: map[string]string{"app": "a"}, Entries: []Entry{{Line: "1"}}},
			{Entries: []Entry{{Line: "2"}}},
		}, PushOptions{Format: PushFormatJSON})
		if err != nil {
			t.Fatal(err)
		}
		b := decodePushBody(t, (*got)[0].body)
		if b.Streams[0].Stream["app"] != "a" || b.Streams[1].Stream == nil {
			t.Fatalf("%s", (*got)[0].body)
		}
	})

	t.Run("empty push sends nothing", func(t *testing.T) {
		ts, _ := captureServer(t, "push.json")
		if err := newLoki(t, ts.URL, nil).Push(t.Context(), nil, PushOptions{}); err != nil || ts.calls.Load() != 0 {
			t.Fatalf("%v %d", err, ts.calls.Load())
		}
	})
}

func TestPushProtobuf(t *testing.T) {
	ts, got := captureServer(t, "push.protobuf-snappy")
	c := newLoki(t, ts.URL, nil)
	at := time.Unix(1795034767, 66713072)
	err := c.Push(t.Context(), []Stream{{
		Labels:  map[string]string{"service_name": "sdk-verify", "level": "info"},
		Entries: []Entry{{Timestamp: at, Line: "proto line", StructuredMetadata: map[string]string{"b": "2", "a": "1"}}},
	}}, PushOptions{Format: PushFormatProtobuf, Gzip: true})
	if err != nil {
		t.Fatal(err)
	}
	req := (*got)[0]
	if req.header.Get("Content-Type") != "application/x-protobuf" || req.header.Get("Content-Encoding") != "" {
		t.Fatalf("headers %v", req.header)
	}
	raw, err := snappy.Decode(nil, req.body)
	if err != nil {
		t.Fatalf("snappy: %v", err)
	}
	want := encodePushProtobuf([]Stream{{
		Labels:  map[string]string{"service_name": "sdk-verify", "level": "info"},
		Entries: []Entry{{Timestamp: at, Line: "proto line", StructuredMetadata: map[string]string{"a": "1", "b": "2"}}},
	}})
	if !bytes.Equal(raw, want) {
		t.Fatal("protobuf payload mismatch")
	}
	// Decode by hand: PushRequest.streams[0].labels and entry fields.
	fields := pbFields(t, raw)
	stream := pbFields(t, fields[0].data)
	if string(stream[0].data) != `{level="info", service_name="sdk-verify"}` || stream[0].num != 1 {
		t.Fatalf("labels %q", stream[0].data)
	}
	entry := pbFields(t, stream[1].data)
	ts2 := pbFields(t, entry[0].data)
	if ts2[0].varint != 1795034767 || ts2[1].varint != 66713072 || string(entry[1].data) != "proto line" {
		t.Fatalf("entry %+v ts %+v", entry, ts2)
	}
	pair := pbFields(t, entry[2].data)
	if string(pair[0].data) != "a" || string(pair[1].data) != "1" || entry[2].num != 3 {
		t.Fatalf("metadata %+v", pair)
	}
	// Epoch and pre-epoch timestamps encode (zero fields omitted, nanos positive).
	if b := encodeEntryProtobuf(Entry{Timestamp: time.Unix(0, 0)}); b[1] != 0 {
		t.Fatalf("epoch: %x", b)
	}
	neg := pbFields(t, pbFields(t, encodeEntryProtobuf(Entry{Timestamp: time.Unix(-1, 500)}))[0].data)
	if int64(neg[0].varint) != -1 || neg[1].varint != 500 {
		t.Fatalf("pre-epoch %+v", neg)
	}
}

type pbField struct {
	num    int
	varint uint64
	data   []byte
}

func pbFields(t *testing.T, b []byte) []pbField {
	t.Helper()
	var out []pbField
	read := func() uint64 {
		var v uint64
		for shift := 0; ; shift += 7 {
			c := b[0]
			b = b[1:]
			v |= uint64(c&0x7f) << shift
			if c < 0x80 {
				return v
			}
		}
	}
	for len(b) > 0 {
		tag := read()
		f := pbField{num: int(tag >> 3)}
		switch tag & 7 {
		case 0:
			f.varint = read()
		case 2:
			n := int(read())
			f.data, b = b[:n], b[n:]
		default:
			t.Fatalf("wire type %d", tag&7)
		}
		out = append(out, f)
	}
	return out
}

func TestPushErrors(t *testing.T) {
	cases := map[string]ErrorCode{
		"push.too-old":                  ErrorCodeValidation,
		"push.too-far-future":           ErrorCodeValidation,
		"push.invalid-label-name":       ErrorCodeValidation,
		"push.mt-no-tenant":             ErrorCodeAuthentication,
		"push.mt-invalid-tenant-slash":  ErrorCodeValidation,
		"push.protobuf-not-snappy":      ErrorCodeValidation,
		"transport.gateway-fault-502":   ErrorCodeUnavailable,
		"push.unsupported-content-type": ErrorCodeValidation,
	}
	for name, code := range cases {
		t.Run(name, func(t *testing.T) {
			ts := newTestServer(t, serveFixture(t, name))
			c := newLoki(t, ts.URL, func(cfg *Config) { cfg.Retry = fastRetry(0) })
			err := c.Push(t.Context(), []Stream{{Labels: map[string]string{"job": "x"}, Entries: []Entry{{Line: "l"}}}}, PushOptions{})
			if e := asError(t, err); e.Code != code || e.HTTPStatus == 0 {
				t.Fatalf("%+v", e)
			}
		})
	}
	t.Run("too old text", func(t *testing.T) {
		ts := newTestServer(t, serveFixture(t, "push.too-old"))
		err := newLoki(t, ts.URL, nil).Push(t.Context(), []Stream{{Entries: []Entry{{Line: "l"}}}}, PushOptions{})
		if !strings.Contains(err.Error(), "timestamp too old") || !strings.HasPrefix(err.Error(), "push: bad request: ") {
			t.Fatal(err)
		}
	})
	t.Run("429 is retried with Retry-After", func(t *testing.T) {
		f := loadFixture(t, "transport.gateway-fault-429")
		ok := loadFixture(t, "push.json")
		ts := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, n int) {
			if n == 1 {
				w.Header().Set("Retry-After", "0")
				f.write(w)
				return
			}
			ok.write(w)
		})
		if err := newLoki(t, ts.URL, nil).Push(t.Context(), []Stream{{Entries: []Entry{{Line: "l"}}}}, PushOptions{}); err != nil || ts.calls.Load() != 2 {
			t.Fatalf("%v %d", err, ts.calls.Load())
		}
	})
	t.Run("blocked ingestion 260", func(t *testing.T) {
		ts := newTestServer(t, status(statusIngestionBlocked))
		err := newLoki(t, ts.URL, nil).Push(t.Context(), []Stream{{Entries: []Entry{{Line: "l"}}}}, PushOptions{})
		if e := asError(t, err); e.Code != ErrorCodeUnavailable || e.Retryable || e.HTTPStatus != 260 {
			t.Fatalf("%+v", e)
		}
	})
	t.Run("client side validation", func(t *testing.T) {
		ts := newTestServer(t, status(http.StatusNoContent))
		c := newLoki(t, ts.URL, func(cfg *Config) { cfg.DisableLabelGuard = true })
		bad := []Stream{
			{Labels: map[string]string{"bad-label": "x"}},
			{Labels: map[string]string{"__name__": "x"}},
			{Labels: map[string]string{"app": ""}},
			{Labels: map[string]string{"app": "\xff"}},
			{Labels: map[string]string{"9app": "x"}},
			{Entries: []Entry{{Line: "\xff"}}},
			{Entries: []Entry{{Line: "l", StructuredMetadata: map[string]string{"": "v"}}}},
			{Entries: []Entry{{Line: "l", StructuredMetadata: map[string]string{"k": "\xff"}}}},
		}
		for i, s := range bad {
			if err := c.Push(t.Context(), []Stream{s}, PushOptions{}); !errors.Is(err, ErrValidation) {
				t.Errorf("case %d: %v", i, err)
			}
		}
		if err := c.Push(t.Context(), []Stream{{Entries: []Entry{{Line: "l"}}}}, PushOptions{Format: "xml"}); !errors.Is(err, ErrValidation) {
			t.Errorf("format: %v", err)
		}
		if ts.calls.Load() != 0 {
			t.Fatal("invalid pushes must not reach the server")
		}
	})
}

func TestCardinalityGuard(t *testing.T) {
	g := NewCardinalityGuard([]string{"service_name", "level", "level"})
	if got := g.Allowed(); len(got) != 2 || got[0] != "level" {
		t.Fatalf("allowed %v", got)
	}
	labels, md := g.Split(map[string]AttrValue{
		"service.name": "api", "level": "", "user.id": 42, "ok": true, "ratio": 0.5,
		"tags": []any{"a"}, "nil": nil, "": "skip", "1x": "d",
	})
	if labels["service_name"] != "api" || len(labels) != 1 {
		t.Fatalf("labels %v", labels)
	}
	if md["user_id"] != "42" || md["ok"] != "true" || md["ratio"] != "0.5" || md["tags"] != `["a"]` || md["level"] != "" || md["nil"] != "" || md["_1x"] != "d" {
		t.Fatalf("metadata %v", md)
	}
	if NewCardinalityGuard(nil).IsAllowed("pod") != true || NormalizeLabelName("") != "" {
		t.Fatal("defaults")
	}
	if err := ValidateLabel("ok_name", "v"); err != nil {
		t.Fatal(err)
	}
	if LabelSetString(map[string]string{"b": `q"x`, "a": "1"}) != `{a="1", b="q\"x"}` {
		t.Fatal(LabelSetString(map[string]string{"b": `q"x`, "a": "1"}))
	}
}
