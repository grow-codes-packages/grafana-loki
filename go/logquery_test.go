package loki

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeLoki is an in-memory query_range that honours start/end/limit/direction
// like Loki (end exclusive, limit across streams).
type fakeLoki struct {
	mu      sync.Mutex
	streams map[string][]int64 // label value of "s" → timestamps
	queries []url.Values
}

func (f *fakeLoki) handler(w http.ResponseWriter, r *http.Request, _ int) {
	q := r.URL.Query()
	f.mu.Lock()
	f.queries = append(f.queries, q)
	f.mu.Unlock()
	start, _ := strconv.ParseInt(q.Get("start"), 10, 64)
	end := int64(1 << 62)
	if e := q.Get("end"); e != "" {
		end, _ = strconv.ParseInt(e, 10, 64)
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	forward := q.Get("direction") == "forward"
	type hit struct {
		s  string
		ts int64
	}
	var hits []hit
	for s, tss := range f.streams {
		for _, ts := range tss {
			if ts >= start && ts < end {
				hits = append(hits, hit{s, ts})
			}
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].ts != hits[j].ts {
			return (hits[i].ts < hits[j].ts) == forward
		}
		return hits[i].s < hits[j].s
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	byStream := map[string][][]any{}
	for _, h := range hits {
		byStream[h.s] = append(byStream[h.s], []any{strconv.FormatInt(h.ts, 10), fmt.Sprintf("line %s %d", h.s, h.ts),
			map[string]any{"structuredMetadata": map[string]string{"run": "r1"}}})
	}
	var result []map[string]any
	for s, vals := range byStream {
		result = append(result, map[string]any{"stream": map[string]string{"s": s, "service_name": "svc"}, "values": vals})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "streams", "result": result}})
}

func TestSearchPagination(t *testing.T) {
	base := int64(1795035000000000000)
	f := &fakeLoki{streams: map[string][]int64{
		"a": {base + 1, base + 2, base + 2, base + 3, base + 5},
		"b": {base + 2, base + 4, base + 5},
	}}
	ts := newTestServer(t, f.handler)
	lq := newLoki(t, ts.URL, nil).LogQuery()
	window := SearchOptions{Start: time.Unix(0, base), End: time.Unix(0, base+100)}

	for _, order := range []SortOrder{OrderDesc, OrderAsc} {
		opts := window
		opts.Order, opts.Limit = order, 2
		var all []int64
		pages := 0
		for {
			page, err := lq.Search(t.Context(), `{service_name="svc"}`, opts)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			for _, r := range page.Records {
				all = append(all, r.Timestamp.UnixNano()-base)
			}
			if page.NextCursor == "" {
				break
			}
			opts.Cursor = page.NextCursor
			if pages > 10 {
				t.Fatal("pagination does not end")
			}
		}
		if len(all) != 8 {
			t.Fatalf("%s: got %v", order, all)
		}
		if !sort.SliceIsSorted(all, func(i, j int) bool {
			if order == OrderAsc {
				return all[i] < all[j]
			}
			return all[i] > all[j]
		}) {
			t.Fatalf("%s: not sorted %v", order, all)
		}
	}
	// SearchAll (template helper) follows the cursors too.
	recs, err := SearchAll(t.Context(), lq, "line", SearchOptions{Start: window.Start, End: window.End, Limit: 3}, 0)
	if err != nil || len(recs) != 8 {
		t.Fatalf("%d %v", len(recs), err)
	}
	if got := f.queries[len(f.queries)-1].Get("query"); got != `{service_name=~".+"} |= "line"` {
		t.Fatalf("free text query %q", got)
	}
	// Without bounds, cursor pages set a one-hour window below the boundary themselves.
	for _, order := range []SortOrder{OrderDesc, OrderAsc} {
		all, err := SearchAll(t.Context(), lq, `{s="a"}`, SearchOptions{Limit: 1, Order: order}, 0)
		if err != nil || len(all) != 8 {
			t.Fatalf("%s: %d %v", order, len(all), err)
		}
	}

	for _, bad := range []SearchOptions{{Order: "sideways"}, {Cursor: "!!"}, {Cursor: encodeRaw("x:1")}, {Cursor: encodeRaw("1:-1")}, {Cursor: encodeRaw("1")}} {
		if _, err := lq.Search(t.Context(), "x", bad); !errors.Is(err, ErrValidation) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
	down := newLoki(t, "http://"+closedAddr(t), func(cfg *Config) { cfg.Retry = fastRetry(0) }).LogQuery()
	if _, err := down.Search(t.Context(), "x", SearchOptions{}); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func encodeRaw(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func TestEntryToRecord(t *testing.T) {
	f := loadFixture(t, "queryRange.otlp-record")
	var env struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(f.body(), &env); err != nil {
		t.Fatal(err)
	}
	res, err := decodeQueryData(env.Data)
	if err != nil || len(res.Streams) == 0 {
		t.Fatal(err)
	}
	s := res.Streams[0]
	r := EntryToRecord(s.Labels, s.Entries[0])
	if r.Resource["service.name"] == nil || r.TraceID == "" || r.SpanID == "" || r.Severity == SeverityUnspecified || r.SeverityText == "" {
		t.Fatalf("%+v", r)
	}
	for _, k := range []string{"trace_id", "span_id", "severity_number", "severity_text", "detected_level", "flags"} {
		if _, ok := r.Attributes[k]; ok {
			t.Errorf("attribute %s must be mapped, not kept", k)
		}
	}

	at := time.Unix(1, 0)
	cases := []struct {
		labels map[string]string
		md     map[string]string
		sev    Severity
		text   string
	}{
		{map[string]string{"level": "warn"}, nil, SeverityWarn, "WARN"},
		{nil, map[string]string{"detected_level": "error"}, SeverityError, "ERROR"},
		{nil, map[string]string{"severity_text": "fatal", "severity_number": "99"}, SeverityFatal, "fatal"},
		{nil, map[string]string{"trace_flags": "x", "observed_timestamp": "y"}, SeverityInfo, "INFO"},
		{map[string]string{"host_name": "h", "custom": "c"}, map[string]string{"trace_flags": "1", "observed_timestamp": "5"}, SeverityInfo, "INFO"},
	}
	for i, c := range cases {
		r := entryToRecord(c.labels, StreamEntry{Timestamp: at, Line: "l", StructuredMetadata: c.md, Parsed: map[string]string{"p": "1"}})
		if r.Severity != c.sev || r.SeverityText != c.text || r.Attributes["p"] != "1" {
			t.Errorf("case %d: %+v", i, r)
		}
		if i == 4 && (r.Resource["host.name"] != "h" || r.Resource["custom"] != "c" || *r.TraceFlags != 1 || r.ObservedTimestamp.UnixNano() != 5) {
			t.Errorf("case 4: %+v", r)
		}
	}
}

func TestLogQueryOther(t *testing.T) {
	ts := newTestServer(t, fixtureRoutes(t, map[string]string{pathSeries: "series.ok", pathLabels: "labels.ok"}))
	lq := newLoki(t, ts.URL, nil).LogQuery()
	streams, err := lq.ListStreams(t.Context())
	if err != nil || len(streams) == 0 || streams[0][0] != '{' || !sort.StringsAreSorted(streams) {
		t.Fatalf("%v %v", streams, err)
	}
	labels, err := lq.ListLabels(t.Context())
	if err != nil || !sort.StringsAreSorted(labels) || len(labels) == 0 {
		t.Fatalf("%v %v", labels, err)
	}
	nf := newLoki(t, newTestServer(t, serveFixture(t, "http.not-found")).URL, nil).LogQuery()
	if _, err := nf.ListStreams(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := nf.ListLabels(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}

	// WaitForLog polls Search.
	base := time.Now().UnixNano()
	f := &fakeLoki{streams: map[string][]int64{"a": {base}}}
	wl := newLoki(t, newTestServer(t, f.handler).URL, nil).LogQuery()
	rec, err := wl.WaitForLog(t.Context(), "line a", WaitForLogOptions{Timeout: 2 * time.Second, Interval: 10 * time.Millisecond,
		Search: SearchOptions{Start: time.Unix(0, base-1), End: time.Unix(0, base+1)}})
	if err != nil || rec.Attributes["run"] != "r1" || rec.Resource["service.name"] != "svc" {
		t.Fatalf("%+v %v", rec, err)
	}
	if _, err := wl.WaitForLog(t.Context(), "nothing", WaitForLogOptions{Timeout: 50 * time.Millisecond, Interval: 10 * time.Millisecond,
		Search: SearchOptions{Start: time.Unix(0, base+5), End: time.Unix(0, base+6)}}); !errors.Is(err, ErrTimeout) {
		t.Fatal(err)
	}
}

func TestLogQueryTail(t *testing.T) {
	frame := string(loadFixture(t, "tail.categorized-labels").Response.Frame)
	ts, reqs := wsServer(t, func(ctx context.Context, c *websocket.Conn, _ *http.Request) {
		_ = c.Write(ctx, websocket.MessageText, []byte(frame))
		_, _, _ = c.Read(ctx)
	})
	lq := newLoki(t, ts.URL, nil).LogQuery()
	got := make(chan LogRecord, 4)
	h, err := lq.Tail(t.Context(), "marker", func(r LogRecord) { got <- r }, TailOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r := <-got
	if r.Body == "" {
		t.Fatalf("%+v", r)
	}
	if q := (<-reqs).URL.Query().Get("query"); q != `{service_name=~".+"} |= "marker"` {
		t.Fatalf("query %q", q)
	}
	h.Stop()
	if _, err := lq.Tail(t.Context(), "x", nil, TailOptions{}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	down := newLoki(t, "http://"+closedAddr(t), nil).LogQuery()
	if _, err := down.Tail(t.Context(), "x", func(LogRecord) {}, TailOptions{}); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}
