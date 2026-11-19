package loki

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	ts := newTestServer(t, serveFixture(t, "query.metric-vector"))
	c := newLoki(t, ts.URL, nil)
	at := time.Unix(1795035067, 0)
	res, err := c.Query(t.Context(), `sum(count_over_time({run="x"}[5m]))`, QueryOptions{Time: at, Limit: 10, Direction: DirectionForward})
	if err != nil {
		t.Fatal(err)
	}
	if res.ResultType != ResultTypeVector || len(res.Vector) != 3 || res.Vector[2].Value.Value != 8 || res.Vector[2].Metric["level"] != "info" ||
		res.Vector[0].Value.Timestamp.UnixMilli() != 1795035067710 || res.Vector[0].Metric == nil || len(res.Stats) == 0 {
		t.Fatalf("%+v", res.Vector)
	}
	q := ts.request(0).URL.Query()
	if q.Get("time") != "1795035067000000000" || q.Get("limit") != "10" || q.Get("direction") != "forward" ||
		ts.request(0).Header.Get(HeaderEncodingFlags) != "categorize-labels" {
		t.Fatalf("query %v", q)
	}

	for _, bad := range []string{"", `{app="x"}`, ` {app="x"} |= "e"`} {
		if _, err := c.Query(t.Context(), bad, QueryOptions{}); !errors.Is(err, ErrValidation) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	if _, err := c.Query(t.Context(), "vector(1)", QueryOptions{Direction: "sideways"}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	pe := newTestServer(t, serveFixture(t, "query.parse-error"))
	if _, err := newLoki(t, pe.URL, nil).Query(t.Context(), "sum(", QueryOptions{}); !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "parse error") {
		t.Fatal(err)
	}
}

func TestQueryRange(t *testing.T) {
	t.Run("categorized streams", func(t *testing.T) {
		ts := newTestServer(t, serveFixture(t, "queryRange.categorized-labels"))
		c := newLoki(t, ts.URL, nil)
		start := time.Unix(1795034700, 0)
		res, err := c.QueryRange(t.Context(), `{run="c023892b83"} |= "json line" | json`, QueryRangeOptions{
			Start: start, End: start.Add(time.Hour), Step: 30 * time.Second, Interval: 1500 * time.Millisecond, Limit: 5, Direction: DirectionBackward,
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.ResultType != ResultTypeStreams || len(res.EncodingFlags) != 1 || len(res.Streams) != 1 {
			t.Fatalf("%+v", res)
		}
		s := res.Streams[0]
		e := s.Entries[1]
		if s.Labels["service_name"] != "sdk-verify" || e.StructuredMetadata["trace_id"] != "0af7651916cd43dd8448eb211c80319c" ||
			e.Parsed["status"] != "503" || e.Timestamp.UnixNano() != 1795034767066713072 {
			t.Fatalf("%+v", s)
		}
		q := ts.request(0).URL.Query()
		if q.Get("start") != "1795034700000000000" || q.Get("step") != "30s" || q.Get("interval") != "1500ms" || q.Get("limit") != "5" || q.Get("direction") != "backward" || q.Get("since") != "" {
			t.Fatalf("params %v", q)
		}
	})
	t.Run("plain streams, matrix, empty", func(t *testing.T) {
		for name, check := range map[string]func(*QueryResult) bool{
			"queryRange.streams-forward": func(r *QueryResult) bool {
				return len(r.Streams) > 0 && r.Streams[0].Entries[0].StructuredMetadata == nil && r.Streams[0].Labels["detected_level"] != ""
			},
			"queryRange.metric-matrix": func(r *QueryResult) bool {
				return r.ResultType == ResultTypeMatrix && len(r.Matrix) > 0 && len(r.Matrix[0].Values) > 0 && r.Matrix[0].Values[0].Timestamp.Unix() == 1795034820
			},
			"queryRange.empty":      func(r *QueryResult) bool { return len(r.Streams) == 0 && r.ResultType == ResultTypeStreams },
			"queryRange.json-where": func(r *QueryResult) bool { return len(r.Streams) > 0 },
			"volumeRange.after-flush": func(r *QueryResult) bool {
				return len(r.Matrix) == 1 && r.Matrix[0].Metric["run"] != ""
			},
		} {
			ts := newTestServer(t, serveFixture(t, name))
			res, err := newLoki(t, ts.URL, nil).QueryRange(t.Context(), `{a="b"}`, QueryRangeOptions{Since: 90 * time.Second})
			if err != nil || !check(res) {
				t.Errorf("%s: %v %+v", name, err, res)
			}
			if got := ts.request(0).URL.Query().Get("since"); got != "90s" {
				t.Errorf("since %q", got)
			}
		}
	})
	t.Run("errors", func(t *testing.T) {
		c := newLoki(t, "http://127.0.0.1:1", nil)
		now := time.Now()
		if _, err := c.QueryRange(t.Context(), " ", QueryRangeOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.QueryRange(t.Context(), `{a="b"}`, QueryRangeOptions{Start: now, End: now}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := c.QueryRange(t.Context(), `{a="b"}`, QueryRangeOptions{Direction: "up"}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		for name, code := range map[string]ErrorCode{
			"queryRange.limit-too-high":           ErrorCodeValidation,
			"queryRange.empty-compatible-matcher": ErrorCodeValidation,
			"queryRange.mt-no-tenant":             ErrorCodeAuthentication,
			"queryRange.bad-time":                 ErrorCodeValidation,
			"queryRange.end-before-start":         ErrorCodeValidation,
			"queryRange.mt-pipe-tenants":          ErrorCodeUnavailable,
		} {
			ts := newTestServer(t, serveFixture(t, name))
			_, err := newLoki(t, ts.URL, func(cfg *Config) { cfg.Retry = fastRetry(0) }).QueryRange(t.Context(), `{a="b"}`, QueryRangeOptions{})
			if CodeOf(err) != code {
				t.Errorf("%s: %v", name, err)
			}
		}
	})
	t.Run("invalid responses", func(t *testing.T) {
		bodies := []string{
			`nope`,
			`{"data":{"resultType":"streams","result":[{"stream":{},"values":[["1"]]}]}}`,
			`{"data":{"resultType":"streams","result":[{"stream":{},"values":[[1,"l"]]}]}}`,
			`{"data":{"resultType":"streams","result":[{"stream":{},"values":[["x","l"]]}]}}`,
			`{"data":{"resultType":"streams","result":[{"stream":{},"values":[["1",2]]}]}}`,
			`{"data":{"resultType":"streams","result":[{"stream":{},"values":[["1","l",[]]]}]}}`,
			`{"data":{"resultType":"streams","result":{}}}`,
			`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1]}]}}`,
			`{"data":{"resultType":"vector","result":[{"metric":{},"value":["x","1"]}]}}`,
			`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1,2]}]}}`,
			`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1,"abc"]}]}}`,
			`{"data":{"resultType":"matrix","result":[{"metric":{},"values":[[1]]}]}}`,
			`{"data":{"resultType":"scalar","result":[1]}}`,
			`{"data":{"resultType":"weird","result":[1]}}`,
			`{"data":[]}`,
		}
		for _, b := range bodies {
			ts := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) { _, _ = w.Write([]byte(b)) })
			if _, err := newLoki(t, ts.URL, nil).QueryRange(t.Context(), `{a="b"}`, QueryRangeOptions{}); !errors.Is(err, ErrUnavailable) {
				t.Errorf("%s: %v", b, err)
			}
		}
		for _, b := range []string{`{"data":null}`, `{}`, `{"data":{"resultType":"streams","result":null}}`, `{"data":{"resultType":"scalar","result":[1795035067.5,"2"]}}`} {
			ts := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) { _, _ = w.Write([]byte(b)) })
			res, err := newLoki(t, ts.URL, nil).QueryRange(t.Context(), `{a="b"}`, QueryRangeOptions{})
			if err != nil {
				t.Errorf("%s: %v", b, err)
			}
			if strings.Contains(b, "scalar") && (res.Scalar == nil || res.Scalar.Value != 2 || res.Scalar.Timestamp.UnixMilli() != 1795035067500) {
				t.Errorf("scalar %+v", res.Scalar)
			}
		}
	})
}

func TestLabelsSeries(t *testing.T) {
	ts := newTestServer(t, fixtureRoutes(t, map[string]string{
		pathLabels:                          "labels.ok",
		"/loki/api/v1/label/level/values":   "labelValues.with-query",
		"/loki/api/v1/label/nothing/values": "labelValues.unknown-label",
		pathSeries:                          "series.ok",
	}))
	c := newLoki(t, ts.URL, nil)
	start := time.Unix(100, 0)

	labels, err := c.Labels(t.Context(), LabelsOptions{Start: start, End: start.Add(time.Hour), Query: `{run="x"}`})
	if err != nil || len(labels) == 0 || labels[0] != "dedup" {
		t.Fatalf("%v %v", labels, err)
	}
	if q := ts.request(0).URL.Query(); q.Get("start") != "100000000000" || q.Get("query") != `{run="x"}` || q.Get("since") != "" {
		t.Fatalf("labels params %v", q)
	}
	vals, err := c.LabelValues(t.Context(), "level", LabelsOptions{Since: 1500 * time.Millisecond})
	if err != nil || len(vals) != 2 || vals[0] != "error" {
		t.Fatalf("%v %v", vals, err)
	}
	if q := ts.request(1).URL.Query(); q.Get("since") != "1500ms" {
		t.Fatalf("since %v", q)
	}
	if vals, err := c.LabelValues(t.Context(), "nothing", LabelsOptions{}); err != nil || vals == nil || len(vals) != 0 {
		t.Fatalf("missing data must decode as empty: %v %v", vals, err)
	}
	if _, err := c.LabelValues(t.Context(), "bad-name", LabelsOptions{}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}

	series, err := c.Series(t.Context(), []string{`{run="a"}`, `{run="b"}`}, SeriesOptions{Start: start, End: start.Add(time.Minute)})
	if err != nil || len(series) == 0 || series[0]["run"] != "c023892b83" {
		t.Fatalf("%v %v", series, err)
	}
	r := ts.request(3)
	if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
		t.Fatalf("series %s %v", r.Method, r.Header)
	}
	if _, err := c.Series(t.Context(), nil, SeriesOptions{}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}

	t.Run("series body and empty", func(t *testing.T) {
		var form url.Values
		srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request, _ int) {
			b, _ := io.ReadAll(r.Body)
			form, _ = url.ParseQuery(string(b))
			_, _ = w.Write([]byte(`{"status":"success"}`))
		})
		got, err := newLoki(t, srv.URL, nil).Series(t.Context(), []string{`{a="1"}`, `{b="2"}`}, SeriesOptions{Since: time.Minute})
		if err != nil || got == nil || len(got) != 0 || len(form["match[]"]) != 2 || form.Get("since") != "60s" {
			t.Fatalf("%v %v %v", got, err, form)
		}
		bad := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) { _, _ = w.Write([]byte(`x`)) })
		if _, err := newLoki(t, bad.URL, nil).Series(t.Context(), []string{`{a="1"}`}, SeriesOptions{}); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		if _, err := newLoki(t, bad.URL, nil).Labels(t.Context(), LabelsOptions{}); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		inv := newTestServer(t, serveFixture(t, "series.invalid-matcher"))
		if _, err := newLoki(t, inv.URL, nil).Series(t.Context(), []string{`{`}, SeriesOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := newLoki(t, inv.URL, nil).Labels(t.Context(), LabelsOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
}

func TestVolumeRangeVectorBecomesMatrix(t *testing.T) {
	ts := newTestServer(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"run":"x"},"value":[1795033774.86,"731"]}]}}`))
	})
	vr, err := newLoki(t, ts.URL, nil).VolumeRange(t.Context(), `{run="x"}`, VolumeOptions{Step: time.Minute})
	if err != nil || vr.ResultType != ResultTypeMatrix || len(vr.Vector) != 0 || len(vr.Matrix) != 1 ||
		vr.Matrix[0].Metric["run"] != "x" || len(vr.Matrix[0].Values) != 1 || vr.Matrix[0].Values[0].Value != 731 {
		t.Fatalf("%+v %v", vr, err)
	}
}

func TestIndexAndVolume(t *testing.T) {
	ts := newTestServer(t, fixtureRoutes(t, map[string]string{
		pathStats:    "indexStats.after-flush",
		pathVolume:   "volume.target-labels",
		pathVolRange: "volumeRange.before-flush",
		pathFormat:   "formatQuery.ok",
	}))
	c := newLoki(t, ts.URL, nil)
	start := time.Unix(1000, 0)

	st, err := c.IndexStats(t.Context(), `{run="x"}`, IndexOptions{Start: start, End: start.Add(time.Hour)})
	if err != nil || st.Streams != 8 || st.Entries != 13 || st.Chunks != 8 {
		t.Fatalf("%+v %v", st, err)
	}
	vol, err := c.Volume(t.Context(), `{run="x"}`, VolumeOptions{Start: start, Limit: 3, TargetLabels: []string{"level", "env"}, AggregateBy: "labels"})
	if err != nil || len(vol.Vector) != 1 || vol.Vector[0].Metric["level"] != "" {
		t.Fatalf("%+v %v", vol, err)
	}
	q := ts.request(1).URL.Query()
	if q.Get("targetLabels") != "level,env" || q.Get("aggregateBy") != "labels" || q.Get("limit") != "3" || q.Get("step") != "" {
		t.Fatalf("volume params %v", q)
	}
	vr, err := c.VolumeRange(t.Context(), `{run="x"}`, VolumeOptions{Step: time.Minute})
	if err != nil || vr.ResultType != ResultTypeMatrix || len(vr.Matrix) != 1 {
		t.Fatalf("%+v %v", vr, err)
	}
	if ts.request(2).URL.Query().Get("step") != "60s" {
		t.Fatal("step")
	}
	f, err := c.FormatQuery(t.Context(), `{app="x"}|="err"|json|status>=500`)
	if err != nil || f != `{app="x"} |= "err" | json | status>=500` {
		t.Fatalf("%q %v", f, err)
	}

	for _, fn := range []func() error{
		func() error { _, err := c.IndexStats(t.Context(), "", IndexOptions{}); return err },
		func() error { _, err := c.Volume(t.Context(), " ", VolumeOptions{}); return err },
		func() error {
			_, err := c.VolumeRange(t.Context(), `{a="b"}`, VolumeOptions{AggregateBy: "pods"})
			return err
		},
		func() error {
			_, err := c.Volume(t.Context(), `{a="b"}`, VolumeOptions{AggregateBy: "pods"})
			return err
		},
	} {
		if err := fn(); !errors.Is(err, ErrValidation) {
			t.Errorf("want VALIDATION: %v", err)
		}
	}
	inv := newTestServer(t, fixtureRoutes(t, map[string]string{pathStats: "indexStats.invalid-query", pathFormat: "formatQuery.parse-error"}))
	ci := newLoki(t, inv.URL, nil)
	if _, err := ci.IndexStats(t.Context(), `{`, IndexOptions{}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	_, err = ci.FormatQuery(t.Context(), `{app="x"`)
	if !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "parse error") {
		t.Fatal(err)
	}
	if _, err := ci.Volume(t.Context(), `{a="b"}`, VolumeOptions{}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	empty := newTestServer(t, serveFixture(t, "indexStats.before-flush"))
	if st, err := newLoki(t, empty.URL, nil).IndexStats(t.Context(), `{a="b"}`, IndexOptions{}); err != nil || st != (IndexStats{}) {
		t.Fatalf("%+v %v", st, err)
	}
}

func TestTimeHelpers(t *testing.T) {
	for in, want := range map[string]int64{"1795034820": 1795034820e9, "1795035067.71": 1795035067710000000, "1.0000000019": 1000000001, "1e3": 1000e9} {
		got, err := parseUnixSeconds(in)
		if err != nil || got.UnixNano() != want {
			t.Errorf("%s: %d %v", in, got.UnixNano(), err)
		}
	}
	for _, bad := range []string{"x", "NaN", "1.x"} {
		if _, err := parseUnixSeconds(bad); err == nil {
			t.Errorf("%s must fail", bad)
		}
	}
	if _, err := decodePoint(json.RawMessage(`"x"`)); err == nil {
		t.Fatal("point")
	}
}

func TestQueryStats(t *testing.T) {
	b, _ := json.Marshal(struct{ S QueryStats }{})
	if string(b) != `{"S":null}` || QueryStats(`{"a":1}`).String() != `{"a":1}` {
		t.Fatal(string(b))
	}
	b, _ = json.Marshal(QueryStats(`{"a":1}`))
	if string(b) != `{"a":1}` {
		t.Fatal(string(b))
	}
}
