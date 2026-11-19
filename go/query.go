package loki

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Direction is the order of log entries in query results.
type Direction string

// Directions.
const (
	// DirectionBackward returns the newest entries first (Loki's default).
	DirectionBackward Direction = "backward"
	// DirectionForward returns the oldest entries first.
	DirectionForward Direction = "forward"
)

// MaxTailDelay is Loki's maximum tail delay_for.
const MaxTailDelay = 5 * time.Second

// QueryOptions configure Query (instant metric queries).
type QueryOptions struct {
	// Time is the evaluation time (zero = now).
	Time time.Time
	// Limit is the maximum number of entries (kept for parity; metric queries ignore it).
	Limit int
	// Direction is kept for parity; metric queries ignore it.
	Direction Direction
}

// QueryRangeOptions configure QueryRange.
type QueryRangeOptions struct {
	// Start is inclusive; zero = End - Since, or Loki's default (one hour before End).
	Start time.Time
	// End is exclusive; zero = now.
	End time.Time
	// Since is a relative start used when Start is zero.
	Since time.Duration
	// Step is the resolution of metric queries.
	Step time.Duration
	// Interval samples stream results (only every Interval an entry).
	Interval time.Duration
	// Limit is the maximum number of entries across all streams (Loki default 100, max 5000 by default).
	Limit int
	// Direction of entries (default backward).
	Direction Direction
}

// LabelsOptions configure Labels and LabelValues.
type LabelsOptions struct {
	// Start and End bound the time range (zero = Loki's default, the last 6 hours).
	Start time.Time
	End   time.Time
	// Since is a relative start used when Start is zero.
	Since time.Duration
	// Query is an optional stream selector that scopes the result.
	Query string
}

// SeriesOptions configure Series.
type SeriesOptions struct {
	Start time.Time
	End   time.Time
	Since time.Duration
}

// IndexOptions configure IndexStats.
type IndexOptions struct {
	Start time.Time
	End   time.Time
}

// VolumeOptions configure Volume and VolumeRange.
type VolumeOptions struct {
	Start time.Time
	End   time.Time
	// Step is the resolution of VolumeRange.
	Step time.Duration
	// Limit is the maximum number of series.
	Limit int
	// TargetLabels aggregates by these label names.
	TargetLabels []string
	// AggregateBy is "series" (default) or "labels".
	AggregateBy string
}

// Result types of a QueryResult.
const (
	ResultTypeStreams = "streams"
	ResultTypeVector  = "vector"
	ResultTypeMatrix  = "matrix"
	ResultTypeScalar  = "scalar"
)

// QueryResult is the data of a query, query_range, volume or volume_range response.
type QueryResult struct {
	// ResultType is "streams", "vector", "matrix" or "scalar".
	ResultType string
	// Streams is set for log queries.
	Streams []StreamResult
	// Vector is set for instant metric queries.
	Vector []Sample
	// Matrix is set for range metric queries.
	Matrix []SampleSeries
	// Scalar is set for scalar results.
	Scalar *Point
	// EncodingFlags echoes the response encoding flags (for example "categorize-labels").
	EncodingFlags []string
	// Stats is Loki's raw query statistics JSON (may be empty).
	Stats QueryStats
}

// StreamResult is one stream of a log query result or tail frame.
type StreamResult struct {
	// Labels are the stream labels. With categorized labels (the SDK default)
	// they are index labels only; otherwise Loki merges metadata and parsed labels in.
	Labels  map[string]string
	Entries []StreamEntry
}

// StreamEntry is one log line returned by Loki.
type StreamEntry struct {
	Timestamp time.Time
	Line      string
	// StructuredMetadata of the entry (categorized responses only).
	StructuredMetadata map[string]string
	// Parsed labels extracted by parsers such as | json (categorized responses only).
	Parsed map[string]string
}

// Point is one metric sample value.
type Point struct {
	Timestamp time.Time
	Value     float64
}

// Sample is one series of an instant vector.
type Sample struct {
	Metric map[string]string
	Value  Point
}

// SampleSeries is one series of a range matrix.
type SampleSeries struct {
	Metric map[string]string
	Values []Point
}

// QueryStats is Loki's raw statistics object of a query response, kept as JSON.
type QueryStats []byte

// String returns the JSON text.
func (s QueryStats) String() string { return string(s) }

// MarshalJSON returns the raw JSON (null when empty).
func (s QueryStats) MarshalJSON() ([]byte, error) {
	if len(s) == 0 {
		return []byte("null"), nil
	}
	return s, nil
}

// IndexStats is the response of IndexStats (from the TSDB index only: zero
// until the ingester flushed its chunks; Bytes is tracked per chunk in KB).
type IndexStats struct {
	Streams int64 `json:"streams"`
	Chunks  int64 `json:"chunks"`
	Bytes   int64 `json:"bytes"`
	Entries int64 `json:"entries"`
}

// isLogQuery reports whether q is a log query (starts with a stream selector).
func isLogQuery(q string) bool {
	return strings.HasPrefix(strings.TrimSpace(q), "{")
}

// Query runs an instant query (GET /loki/api/v1/query). Loki answers log
// queries only as range queries, so a log query ({...}) is rejected with
// VALIDATION before any request: use QueryRange or LogQuery().Search.
func (c *Client) Query(ctx context.Context, logql string, opts QueryOptions) (*QueryResult, error) {
	if strings.TrimSpace(logql) == "" {
		return nil, ValidationError("query", "query must not be empty")
	}
	if isLogQuery(logql) {
		return nil, ValidationError("query", "log queries are not supported as instant queries; use QueryRange")
	}
	q := url.Values{"query": {logql}}
	setTime(q, "time", opts.Time)
	setLimit(q, opts.Limit)
	if err := setDirection(q, "query", opts.Direction); err != nil {
		return nil, err
	}
	return c.getResult(ctx, "query", pathQuery, q)
}

// QueryRange runs a range query (GET /loki/api/v1/query_range). Log queries
// return Streams (each stream's entries ordered by Direction; streams are not
// ordered globally), metric queries return Matrix. Labels are categorized:
// stream labels are index labels, structured metadata and parsed labels come
// separately per entry.
func (c *Client) QueryRange(ctx context.Context, logql string, opts QueryRangeOptions) (*QueryResult, error) {
	if strings.TrimSpace(logql) == "" {
		return nil, ValidationError("queryRange", "query must not be empty")
	}
	if !opts.Start.IsZero() && !opts.End.IsZero() && !opts.End.After(opts.Start) {
		return nil, ValidationError("queryRange", "end must be after start")
	}
	q := url.Values{"query": {logql}}
	setTime(q, "start", opts.Start)
	setTime(q, "end", opts.End)
	if opts.Start.IsZero() {
		setSince(q, opts.Since)
	}
	setStep(q, "step", opts.Step)
	setStep(q, "interval", opts.Interval)
	setLimit(q, opts.Limit)
	if err := setDirection(q, "queryRange", opts.Direction); err != nil {
		return nil, err
	}
	return c.getResult(ctx, "queryRange", pathRange, q)
}

// Labels lists label names (GET /loki/api/v1/labels). Structured metadata
// keys are not labels. An empty result is an empty slice.
func (c *Client) Labels(ctx context.Context, opts LabelsOptions) ([]string, error) {
	return c.getStrings(ctx, "labels", pathLabels, labelsQuery(opts))
}

// LabelValues lists the values of label name (GET
// /loki/api/v1/label/{name}/values). Loki answers unknown and invalid names
// with an empty list, so the name is validated client side.
func (c *Client) LabelValues(ctx context.Context, name string, opts LabelsOptions) ([]string, error) {
	if !validLabelName(name) {
		return nil, ValidationError("labelValues", fmt.Sprintf("invalid label name %q", name))
	}
	return c.getStrings(ctx, "labelValues "+name, "/loki/api/v1/label/"+url.PathEscape(name)+"/values", labelsQuery(opts))
}

func labelsQuery(opts LabelsOptions) url.Values {
	q := url.Values{}
	setTime(q, "start", opts.Start)
	setTime(q, "end", opts.End)
	if opts.Start.IsZero() {
		setSince(q, opts.Since)
	}
	if opts.Query != "" {
		q.Set("query", opts.Query)
	}
	return q
}

// Series lists the label sets of the streams matching any of the selectors
// (POST /loki/api/v1/series with match[]). At least one matcher is required
// (Loki would otherwise return every series).
func (c *Client) Series(ctx context.Context, matchers []string, opts SeriesOptions) ([]map[string]string, error) {
	if len(matchers) == 0 {
		return nil, ValidationError("series", "at least one matcher is required")
	}
	q := url.Values{"match[]": matchers}
	setTime(q, "start", opts.Start)
	setTime(q, "end", opts.End)
	if opts.Start.IsZero() {
		setSince(q, opts.Since)
	}
	res, err := c.send(ctx, &request{
		Method: http.MethodPost, Path: pathSeries, Body: []byte(q.Encode()),
		ContentType: "application/x-www-form-urlencoded", Idempotent: true, Operation: "series",
	})
	if err != nil {
		return nil, err
	}
	var env struct {
		Data []map[string]string `json:"data"`
	}
	if err := res.decodeJSON("series", &env); err != nil {
		return nil, err
	}
	if env.Data == nil {
		env.Data = []map[string]string{}
	}
	return env.Data, nil
}

// IndexStats returns index statistics for a selector (GET
// /loki/api/v1/index/stats). Values come from the TSDB index and stay zero
// until the ingester flushed the chunks.
func (c *Client) IndexStats(ctx context.Context, query string, opts IndexOptions) (IndexStats, error) {
	var st IndexStats
	if strings.TrimSpace(query) == "" {
		return st, ValidationError("indexStats", "query must not be empty")
	}
	q := url.Values{"query": {query}}
	setTime(q, "start", opts.Start)
	setTime(q, "end", opts.End)
	res, err := c.send(ctx, &request{Method: http.MethodGet, Path: pathStats, Query: q, Operation: "indexStats"})
	if err != nil {
		return st, err
	}
	err = res.decodeJSON("indexStats", &st)
	return st, err
}

// Volume returns the ingested bytes per series or label (GET
// /loki/api/v1/index/volume) as a vector. Needs limits_config.volume_enabled
// on the server; otherwise Loki returns an empty vector ("no data", not an error).
func (c *Client) Volume(ctx context.Context, query string, opts VolumeOptions) (*QueryResult, error) {
	q, err := volumeQuery("volume", query, opts, false)
	if err != nil {
		return nil, err
	}
	return c.getResult(ctx, "volume", pathVolume, q)
}

// VolumeRange is Volume over time (GET /loki/api/v1/index/volume_range). The
// result is always a matrix (ResultType "matrix", data in Matrix): Loki answers
// with a vector when every series has a single point, and the SDK converts that
// vector to a one-point-per-series matrix.
func (c *Client) VolumeRange(ctx context.Context, query string, opts VolumeOptions) (*QueryResult, error) {
	q, err := volumeQuery("volumeRange", query, opts, true)
	if err != nil {
		return nil, err
	}
	res, err := c.getResult(ctx, "volumeRange", pathVolRange, q)
	if err != nil {
		return nil, err
	}
	return vectorToMatrix(res), nil
}

// vectorToMatrix turns a vector result into the equivalent matrix (one point
// per series); other result types are returned unchanged.
func vectorToMatrix(res *QueryResult) *QueryResult {
	if res.ResultType != ResultTypeVector {
		return res
	}
	matrix := make([]SampleSeries, 0, len(res.Vector))
	for _, s := range res.Vector {
		matrix = append(matrix, SampleSeries{Metric: s.Metric, Values: []Point{s.Value}})
	}
	res.ResultType = ResultTypeMatrix
	res.Matrix = matrix
	res.Vector = nil
	return res
}

func volumeQuery(op, query string, opts VolumeOptions, withStep bool) (url.Values, error) {
	if strings.TrimSpace(query) == "" {
		return nil, ValidationError(op, "query must not be empty")
	}
	switch opts.AggregateBy {
	case "", "series", "labels":
	default:
		return nil, ValidationError(op, `aggregateBy must be "series" or "labels"`)
	}
	q := url.Values{"query": {query}}
	setTime(q, "start", opts.Start)
	setTime(q, "end", opts.End)
	if withStep {
		setStep(q, "step", opts.Step)
	}
	setLimit(q, opts.Limit)
	if len(opts.TargetLabels) > 0 {
		q.Set("targetLabels", strings.Join(opts.TargetLabels, ","))
	}
	if opts.AggregateBy != "" {
		q.Set("aggregateBy", opts.AggregateBy)
	}
	return q, nil
}

// FormatQuery asks Loki to parse and pretty-print a LogQL query (GET
// /loki/api/v1/format_query). VALIDATION when the query does not parse.
func (c *Client) FormatQuery(ctx context.Context, logql string) (string, error) {
	res, err := c.send(ctx, &request{Method: http.MethodGet, Path: pathFormat, Query: url.Values{"query": {logql}}, Operation: "formatQuery"})
	if err != nil {
		return "", err
	}
	var env struct {
		Data string `json:"data"`
	}
	err = res.decodeJSON("formatQuery", &env)
	return env.Data, err
}

func (c *Client) getStrings(ctx context.Context, op, path string, q url.Values) ([]string, error) {
	res, err := c.send(ctx, &request{Method: http.MethodGet, Path: path, Query: q, Operation: op})
	if err != nil {
		return nil, err
	}
	var env struct {
		Data []string `json:"data"`
	}
	if err := res.decodeJSON(op, &env); err != nil {
		return nil, err
	}
	if env.Data == nil {
		env.Data = []string{}
	}
	return env.Data, nil
}

func (c *Client) getResult(ctx context.Context, op, path string, q url.Values) (*QueryResult, error) {
	res, err := c.send(ctx, &request{
		Method: http.MethodGet, Path: path, Query: q, Operation: op,
		Header: http.Header{HeaderEncodingFlags: {"categorize-labels"}},
	})
	if err != nil {
		return nil, err
	}
	var env struct {
		Data json.RawMessage `json:"data"`
	}
	if err := res.decodeJSON(op, &env); err != nil {
		return nil, err
	}
	out, err := decodeQueryData(env.Data)
	if err != nil {
		return nil, &Error{Code: ErrorCodeUnavailable, Message: op + ": invalid response: " + err.Error(), HTTPStatus: res.Status, RequestID: res.RequestID}
	}
	return out, nil
}

// decodeQueryData decodes the "data" object of a query response.
func decodeQueryData(raw json.RawMessage) (*QueryResult, error) {
	out := &QueryResult{}
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return out, nil
	}
	var data struct {
		ResultType    string          `json:"resultType"`
		Result        json.RawMessage `json:"result"`
		EncodingFlags []string        `json:"encodingFlags"`
		Stats         json.RawMessage `json:"stats"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	out.ResultType, out.EncodingFlags, out.Stats = data.ResultType, data.EncodingFlags, QueryStats(data.Stats)
	if len(data.Result) == 0 || string(data.Result) == "null" {
		return out, nil
	}
	var err error
	switch data.ResultType {
	case ResultTypeStreams:
		out.Streams, err = decodeStreams(data.Result)
	case ResultTypeVector:
		var items []struct {
			Metric map[string]string `json:"metric"`
			Value  json.RawMessage   `json:"value"`
		}
		if err = json.Unmarshal(data.Result, &items); err == nil {
			out.Vector = make([]Sample, len(items))
			for i, it := range items {
				var p Point
				if p, err = decodePoint(it.Value); err != nil {
					break
				}
				out.Vector[i] = Sample{Metric: nonNilMap(it.Metric), Value: p}
			}
		}
	case ResultTypeMatrix:
		var items []struct {
			Metric map[string]string `json:"metric"`
			Values []json.RawMessage `json:"values"`
		}
		if err = json.Unmarshal(data.Result, &items); err == nil {
			out.Matrix = make([]SampleSeries, len(items))
			for i, it := range items {
				pts := make([]Point, len(it.Values))
				for j, v := range it.Values {
					if pts[j], err = decodePoint(v); err != nil {
						return nil, err
					}
				}
				out.Matrix[i] = SampleSeries{Metric: nonNilMap(it.Metric), Values: pts}
			}
		}
	case ResultTypeScalar:
		var p Point
		if p, err = decodePoint(data.Result); err == nil {
			out.Scalar = &p
		}
	default:
		err = fmt.Errorf("unknown result type %q", data.ResultType)
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// decodeStreams decodes streams with 2-element ([ts, line]) or categorized
// 3-element ([ts, line, {structuredMetadata, parsed}]) values.
func decodeStreams(raw json.RawMessage) ([]StreamResult, error) {
	var items []struct {
		Stream map[string]string   `json:"stream"`
		Values [][]json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}
	out := make([]StreamResult, len(items))
	for i, it := range items {
		entries := make([]StreamEntry, len(it.Values))
		for j, v := range it.Values {
			e, err := decodeStreamEntry(v)
			if err != nil {
				return nil, err
			}
			entries[j] = e
		}
		out[i] = StreamResult{Labels: nonNilMap(it.Stream), Entries: entries}
	}
	return out, nil
}

func decodeStreamEntry(v []json.RawMessage) (StreamEntry, error) {
	var e StreamEntry
	if len(v) < 2 {
		return e, fmt.Errorf("stream value has %d elements, want 2 or 3", len(v))
	}
	var ts string
	if err := json.Unmarshal(v[0], &ts); err != nil {
		return e, fmt.Errorf("stream timestamp: %w", err)
	}
	t, err := ParseUnixNano(ts)
	if err != nil {
		return e, err
	}
	if err := json.Unmarshal(v[1], &e.Line); err != nil {
		return e, fmt.Errorf("stream line: %w", err)
	}
	e.Timestamp = t
	if len(v) > 2 {
		var cat struct {
			StructuredMetadata map[string]string `json:"structuredMetadata"`
			Parsed             map[string]string `json:"parsed"`
		}
		if err := json.Unmarshal(v[2], &cat); err != nil {
			return e, fmt.Errorf("stream categorized labels: %w", err)
		}
		e.StructuredMetadata, e.Parsed = cat.StructuredMetadata, cat.Parsed
	}
	return e, nil
}

// decodePoint decodes [<float seconds>, "<value>"].
func decodePoint(raw json.RawMessage) (Point, error) {
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
		return Point{}, fmt.Errorf("invalid sample %s", truncate(string(raw), 60))
	}
	ts, err := parseUnixSeconds(string(bytes.Trim(pair[0], `"`)))
	if err != nil {
		return Point{}, err
	}
	var s string
	if err := json.Unmarshal(pair[1], &s); err != nil {
		return Point{}, fmt.Errorf("sample value: %w", err)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return Point{}, fmt.Errorf("sample value %q is not a number", s)
	}
	return Point{Timestamp: ts, Value: f}, nil
}

// ParseUnixNano parses a decimal Unix nanosecond timestamp (Loki's wire form).
func ParseUnixNano(s string) (time.Time, error) {
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid nanosecond timestamp %q", s)
	}
	return time.Unix(0, ns).UTC(), nil
}

// parseUnixSeconds parses "<seconds>[.<fraction>]" without float rounding.
func parseUnixSeconds(s string) (time.Time, error) {
	whole, frac, _ := strings.Cut(s, ".")
	sec, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		f, ferr := strconv.ParseFloat(s, 64)
		if ferr != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
		}
		return time.Unix(0, int64(f*1e9)).UTC(), nil
	}
	var ns int64
	if frac != "" {
		if len(frac) > 9 {
			frac = frac[:9]
		}
		frac += strings.Repeat("0", 9-len(frac))
		if ns, err = strconv.ParseInt(frac, 10, 64); err != nil {
			return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
		}
	}
	return time.Unix(sec, ns).UTC(), nil
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// formatNano renders t as decimal Unix nanoseconds.
func formatNano(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func setTime(q url.Values, key string, t time.Time) {
	if !t.IsZero() {
		q.Set(key, formatNano(t))
	}
}

func setLimit(q url.Values, n int) {
	if n > 0 {
		q.Set("limit", strconv.Itoa(n))
	}
}

// setStep renders d as a duration ("60s", "1500ms"). The duration form is
// required by index/volume_range (it ignores plain numbers) and accepted by
// query_range.
func setStep(q url.Values, key string, d time.Duration) {
	if d > 0 {
		q.Set(key, formatDuration(d))
	}
}

// setSince renders d as a relative start.
func setSince(q url.Values, d time.Duration) {
	if d > 0 {
		q.Set("since", formatDuration(d))
	}
}

// formatDuration renders d in a form both Prometheus and Go parsers accept:
// whole seconds as "<n>s", otherwise whole milliseconds as "<n>ms" (min 1ms).
func formatDuration(d time.Duration) string {
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return strconv.FormatInt(max(int64(d/time.Millisecond), 1), 10) + "ms"
}

func setDirection(q url.Values, op string, d Direction) error {
	switch d {
	case "":
		return nil
	case DirectionBackward, DirectionForward:
		q.Set("direction", string(d))
		return nil
	}
	return ValidationError(op, fmt.Sprintf("direction must be %q or %q", DirectionBackward, DirectionForward))
}
