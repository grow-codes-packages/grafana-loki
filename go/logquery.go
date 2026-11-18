package loki

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxEntriesPerQuery is Loki's default max_entries_limit_per_query.
const maxEntriesPerQuery = 5000

// streamsSelector matches every stream that has a service_name (Loki adds one
// to every stream, "unknown_service" by default).
const streamsSelector = `{service_name=~".+"}`

// resourceLabelKeys maps Loki label names back to OpenTelemetry resource keys.
var resourceLabelKeys = map[string]string{
	"service_name":           "service.name",
	"service_namespace":      "service.namespace",
	"service_version":        "service.version",
	"service_instance_id":    "service.instance.id",
	"deployment_environment": "deployment.environment",
	"host_name":              "host.name",
}

// LogQuery is the LogQueryClient of the logging category on top of Loki's
// query API. Queries are raw LogQL; a query that does not start with a
// stream selector is free text and becomes {service_name=~".+"} |= "<text>".
type LogQuery struct {
	client *Client
}

var _ LogQueryClient = (*LogQuery)(nil)

// LogQuery returns the LogQueryClient of this client (same tenant and credentials).
func (c *Client) LogQuery() *LogQuery { return &LogQuery{client: c} }

// toLogQL turns a free-text query into LogQL.
func toLogQL(query string) string {
	q := strings.TrimSpace(query)
	if isLogQuery(q) {
		return q
	}
	return streamsSelector + " |= " + Quote(q)
}

// searchCursor is the decoded form of an opaque search cursor:
// "<last timestamp ns>:<entries at that timestamp already returned>".
type searchCursor struct {
	ts   int64
	skip int
}

func encodeCursor(c searchCursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(c.ts, 10) + ":" + strconv.Itoa(c.skip)))
}

func decodeCursor(s string) (searchCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return searchCursor{}, ValidationError("logs.search", "invalid cursor")
	}
	tsText, skipText, ok := strings.Cut(string(raw), ":")
	ts, err1 := strconv.ParseInt(tsText, 10, 64)
	skip, err2 := strconv.Atoi(skipText)
	if !ok || err1 != nil || err2 != nil || skip < 0 {
		return searchCursor{}, ValidationError("logs.search", "invalid cursor")
	}
	return searchCursor{ts: ts, skip: skip}, nil
}

// sortedEntry is one entry with its stream labels, for merge sorting.
type sortedEntry struct {
	labels    map[string]string
	labelsKey string
	entry     StreamEntry
}

// mergeStreams flattens streams and sorts the entries by timestamp (asc or
// desc), then by label set and line for a stable order.
func mergeStreams(streams []StreamResult, asc bool) []sortedEntry {
	var out []sortedEntry
	for _, s := range streams {
		key := LabelSetString(s.Labels)
		for _, e := range s.Entries {
			out = append(out, sortedEntry{labels: s.Labels, labelsKey: key, entry: e})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if ta, tb := a.entry.Timestamp.UnixNano(), b.entry.Timestamp.UnixNano(); ta != tb {
			if asc {
				return ta < tb
			}
			return ta > tb
		}
		if a.labelsKey != b.labelsKey {
			return a.labelsKey < b.labelsKey
		}
		return a.entry.Line < b.entry.Line
	})
	return out
}

// Search runs a range query and returns normalized records in the requested
// order (default newest first) merged across streams. Pagination uses an
// opaque cursor "<last ns>:<count at that ns>", so entries sharing a
// nanosecond are neither repeated nor skipped. Total is not available.
func (q *LogQuery) Search(ctx context.Context, query string, opts SearchOptions) (LogSearchResult, error) {
	opts = opts.WithDefaults()
	if opts.Order != OrderAsc && opts.Order != OrderDesc {
		return LogSearchResult{}, ValidationError("logs.search", fmt.Sprintf("order must be %q or %q", OrderAsc, OrderDesc))
	}
	asc := opts.Order == OrderAsc
	ro := QueryRangeOptions{Start: opts.Start, End: opts.End, Direction: DirectionBackward}
	if asc {
		ro.Direction = DirectionForward
	}
	var cur searchCursor
	if opts.Cursor != "" {
		var err error
		if cur, err = decodeCursor(opts.Cursor); err != nil {
			return LogSearchResult{}, err
		}
		boundary := time.Unix(0, cur.ts).UTC()
		if asc {
			ro.Start = boundary // inclusive
			if ro.End.IsZero() {
				ro.End = time.Now().Add(time.Minute)
			}
		} else {
			ro.End = boundary.Add(time.Nanosecond) // end is exclusive
			if ro.Start.IsZero() {
				ro.Start = boundary.Add(-time.Hour)
			}
		}
	}
	ro.Limit = min(opts.Limit+cur.skip, maxEntriesPerQuery)
	res, err := q.client.QueryRange(ctx, toLogQL(query), ro)
	if err != nil {
		return LogSearchResult{}, err
	}
	entries := mergeStreams(res.Streams, asc)
	if opts.Cursor != "" {
		skipped := 0
		for skipped < cur.skip && skipped < len(entries) && entries[skipped].entry.Timestamp.UnixNano() == cur.ts {
			skipped++
		}
		entries = entries[skipped:]
	}
	full := len(entries) >= opts.Limit
	if len(entries) > opts.Limit {
		entries = entries[:opts.Limit]
	}
	out := LogSearchResult{Records: make([]LogRecord, len(entries))}
	for i, e := range entries {
		out.Records[i] = entryToRecord(e.labels, e.entry)
	}
	if full && len(entries) > 0 {
		last := entries[len(entries)-1].entry.Timestamp.UnixNano()
		n := 0
		for i := len(entries) - 1; i >= 0 && entries[i].entry.Timestamp.UnixNano() == last; i-- {
			n++
		}
		if opts.Cursor != "" && last == cur.ts {
			n += cur.skip
		}
		out.NextCursor = encodeCursor(searchCursor{ts: last, skip: n})
	}
	return out, nil
}

// EntryToRecord normalizes one Loki entry into a LogRecord: Body = line,
// Resource = stream labels (service_name → service.name and other known
// OpenTelemetry keys mapped back), Attributes = structured metadata + parsed
// labels without the fields mapped below, Severity from severity_number,
// severity_text, detected_level or the level label, TraceID / SpanID /
// TraceFlags from trace_id / span_id / trace_flags (or flags).
func EntryToRecord(labels map[string]string, e StreamEntry) LogRecord {
	return entryToRecord(labels, e)
}

func entryToRecord(labels map[string]string, e StreamEntry) LogRecord {
	rec := LogRecord{
		Timestamp:  e.Timestamp,
		Body:       e.Line,
		Attributes: map[string]AttrValue{},
		Resource:   map[string]AttrValue{},
	}
	for k, v := range labels {
		if rk, ok := resourceLabelKeys[k]; ok {
			rec.Resource[rk] = v
		} else {
			rec.Resource[k] = v
		}
	}
	meta := map[string]string{}
	for k, v := range e.Parsed {
		meta[k] = v
	}
	for k, v := range e.StructuredMetadata {
		meta[k] = v
	}
	take := func(keys ...string) string {
		var out string
		for _, k := range keys {
			if v, ok := meta[k]; ok {
				if out == "" {
					out = v
				}
				delete(meta, k)
			}
		}
		return out
	}
	rec.TraceID = strings.ToLower(take(MetadataTraceID))
	rec.SpanID = strings.ToLower(take(MetadataSpanID))
	if f := take(MetadataTraceFlags, "flags"); f != "" {
		if n, err := strconv.ParseUint(f, 10, 8); err == nil {
			flags := uint8(n)
			rec.TraceFlags = &flags
		}
	}
	if o := take(MetadataObservedTimestamp); o != "" {
		if t, err := ParseUnixNano(o); err == nil {
			rec.ObservedTimestamp = t
		}
	}
	sevNum, sevText, detected := take(MetadataSeverityNumber), take(MetadataSeverityText), take("detected_level")
	level := labels["level"]
	switch n, err := strconv.Atoi(sevNum); {
	case err == nil && n >= 1 && n <= 24:
		rec.Severity = Severity(n)
	default:
		for _, t := range []string{sevText, detected, level} {
			if s, ok := ParseSeverity(t); ok {
				rec.Severity = s
				break
			}
		}
	}
	if rec.Severity == SeverityUnspecified {
		rec.Severity = SeverityInfo
	}
	rec.SeverityText = sevText
	if rec.SeverityText == "" {
		rec.SeverityText = rec.Severity.String()
	}
	for k, v := range meta {
		rec.Attributes[k] = v
	}
	return rec
}

// Tail follows new records matching query over the WebSocket tail (see
// Client.Tail). Start defaults to now (live only).
func (q *LogQuery) Tail(ctx context.Context, query string, onRecord func(LogRecord), opts TailOptions) (TailHandle, error) {
	if onRecord == nil {
		return nil, ValidationError("logs.tail", "onRecord must not be nil")
	}
	s, err := q.client.Tail(ctx, toLogQL(query), func(f TailFrame) {
		for _, st := range f.Streams {
			for _, e := range st.Entries {
				onRecord(entryToRecord(st.Labels, e))
			}
		}
	}, LiveTailOptions{Start: opts.Start})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// ListStreams lists the label sets of the streams seen in the last hour
// (Series over {service_name=~".+"}), rendered as {a="b", c="d"} and sorted.
func (q *LogQuery) ListStreams(ctx context.Context) ([]string, error) {
	series, err := q.client.Series(ctx, []string{streamsSelector}, SeriesOptions{Since: time.Hour})
	if err != nil {
		return nil, err
	}
	out := make([]string, len(series))
	for i, s := range series {
		out[i] = LabelSetString(s)
	}
	sort.Strings(out)
	return out, nil
}

// ListLabels lists label names (Client.Labels with Loki's default window), sorted.
func (q *LogQuery) ListLabels(ctx context.Context) ([]string, error) {
	labels, err := q.client.Labels(ctx, LabelsOptions{})
	if err != nil {
		return nil, err
	}
	sort.Strings(labels)
	return labels, nil
}

// WaitForLog polls Search until a record matches (TIMEOUT when none does in time).
func (q *LogQuery) WaitForLog(ctx context.Context, query string, opts WaitForLogOptions) (LogRecord, error) {
	return WaitForLog(ctx, q, query, opts)
}
