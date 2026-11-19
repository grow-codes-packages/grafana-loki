package loki

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang/snappy"
)

// statusIngestionBlocked is Loki's push status when ingestion is
// administratively blocked (blocked_ingestion limits).
const statusIngestionBlocked = 260

// Stream is one Loki log stream (LokiStream): a label set and its entries.
type Stream struct {
	// Labels are the stream's index labels. Names must match
	// [a-zA-Z_][a-zA-Z0-9_]* and must not start with "__"; values must be
	// non-empty. Labels outside the cardinality guard allowlist are moved to
	// every entry's structured metadata (see Config.AllowedLabels).
	Labels map[string]string
	// Entries are the log lines of the stream (any order).
	Entries []Entry
}

// Entry is one log line of a Stream.
type Entry struct {
	// Timestamp of the line (zero = now). Sent as Unix nanoseconds.
	Timestamp time.Time
	// Line is the log line.
	Line string
	// StructuredMetadata is per-entry metadata (string → string; not indexed).
	StructuredMetadata map[string]string
}

// PushFormat selects the wire format of Push.
type PushFormat string

// Push formats.
const (
	// PushFormatJSON posts application/json (the default).
	PushFormatJSON PushFormat = "json"
	// PushFormatProtobuf posts a Snappy-compressed logproto.PushRequest
	// (application/x-protobuf), encoded from the Apache-2.0 pkg/push/push.proto field numbers.
	PushFormatProtobuf PushFormat = "protobuf"
)

// PushOptions configure Push. The zero value posts uncompressed JSON.
type PushOptions struct {
	// Format is the wire format (default PushFormatJSON).
	Format PushFormat
	// Gzip compresses a JSON body (Content-Encoding: gzip). Protobuf bodies
	// are always Snappy-compressed and ignore it.
	Gzip bool
}

// Push sends streams to Loki (POST /loki/api/v1/push). Labels are validated
// client side and the cardinality guard moves labels outside the allowlist to
// structured metadata (unless Config.DisableLabelGuard). The whole request
// fails or succeeds; Loki rejects entries older than its
// reject_old_samples_max_age or too far in the future with VALIDATION.
// Retrying is safe: Loki drops exact duplicates, so the transport replays the
// identical payload on 429/5xx. An empty streams slice sends nothing.
func (c *Client) Push(ctx context.Context, streams []Stream, opts PushOptions) error {
	return c.push(ctx, streams, opts, !c.cfg.DisableLabelGuard)
}

func (c *Client) push(ctx context.Context, streams []Stream, opts PushOptions, guard bool) error {
	if len(streams) == 0 {
		return nil
	}
	prepared := make([]Stream, 0, len(streams))
	now := time.Now()
	for i, s := range streams {
		if guard {
			s = c.guard.Apply(s)
		}
		if err := validateStream(s); err != nil {
			return ValidationError("push", fmt.Sprintf("stream %d: %s", i, err))
		}
		entries := make([]Entry, len(s.Entries))
		for j, e := range s.Entries {
			if e.Timestamp.IsZero() {
				e.Timestamp = now
			}
			entries[j] = e
		}
		prepared = append(prepared, Stream{Labels: s.Labels, Entries: entries})
	}
	req := &request{Method: http.MethodPost, Path: pathPush, Operation: "push", Idempotent: true, Header: http.Header{}}
	switch opts.Format {
	case "", PushFormatJSON:
		body, err := encodePushJSON(prepared)
		if err != nil {
			return ValidationError("push", "streams cannot be encoded as JSON")
		}
		req.ContentType = "application/json"
		if opts.Gzip {
			body = gzipBytes(body)
			req.Header.Set("Content-Encoding", "gzip")
		}
		req.Body = body
	case PushFormatProtobuf:
		req.ContentType = "application/x-protobuf"
		req.Body = snappy.Encode(nil, encodePushProtobuf(prepared))
	default:
		return ValidationError("push", fmt.Sprintf("unknown push format %q", string(opts.Format)))
	}
	res, err := c.send(ctx, req)
	if err != nil {
		return err
	}
	if res.Status == statusIngestionBlocked {
		return &Error{
			Code:       ErrorCodeUnavailable,
			Message:    "push: ingestion is blocked by the Loki administrator (HTTP 260)",
			HTTPStatus: res.Status,
			RequestID:  res.RequestID,
		}
	}
	return nil
}

// validateStream checks label names and values and metadata keys.
func validateStream(s Stream) error {
	for name, value := range s.Labels {
		if err := ValidateLabel(name, value); err != nil {
			return err
		}
	}
	for j, e := range s.Entries {
		if !utf8.ValidString(e.Line) {
			return fmt.Errorf("entry %d: line is not valid UTF-8", j)
		}
		for k, v := range e.StructuredMetadata {
			if k == "" {
				return fmt.Errorf("entry %d: structured metadata name must not be empty", j)
			}
			if !utf8.ValidString(k) || !utf8.ValidString(v) {
				return fmt.Errorf("entry %d: structured metadata must be valid UTF-8", j)
			}
		}
	}
	return nil
}

// ValidateLabel checks one Loki index label: the name must match
// [a-zA-Z_][a-zA-Z0-9_]* and not start with "__" (reserved), the value must be
// non-empty valid UTF-8. It returns a VALIDATION error.
func ValidateLabel(name, value string) error {
	switch {
	case !validLabelName(name):
		return ValidationError("label", fmt.Sprintf("invalid label name %q (must match [a-zA-Z_][a-zA-Z0-9_]*)", name))
	case strings.HasPrefix(name, "__"):
		return ValidationError("label", fmt.Sprintf("label name %q is reserved (starts with __)", name))
	case value == "":
		return ValidationError("label", fmt.Sprintf("label %q has an empty value", name))
	case !utf8.ValidString(value):
		return ValidationError("label", fmt.Sprintf("label %q value is not valid UTF-8", name))
	}
	return nil
}

func validLabelName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		switch {
		case ch == '_', ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// NormalizeLabelName turns an attribute key into a Loki label / structured
// metadata name the way Loki does for OTLP attributes: every character outside
// [a-zA-Z0-9_] becomes "_" ("service.name" → "service_name") and a leading
// digit gets a "_" prefix. An empty key stays empty.
func NormalizeLabelName(key string) string {
	if key == "" {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		ch := key[i]
		switch {
		case ch == '_', ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
			b.WriteByte(ch)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out[0] >= '0' && out[0] <= '9' {
		out = "_" + out
	}
	return out
}

// CardinalityGuard keeps Loki's index small: only allowlisted keys become
// labels, everything else is sent as structured metadata.
type CardinalityGuard struct {
	allowed map[string]bool
	names   []string
}

// NewCardinalityGuard returns a guard for the allowlist (nil = DefaultAllowedLabels).
func NewCardinalityGuard(allowed []string) *CardinalityGuard {
	if allowed == nil {
		allowed = DefaultAllowedLabels
	}
	g := &CardinalityGuard{allowed: make(map[string]bool, len(allowed))}
	for _, a := range allowed {
		if !g.allowed[a] {
			g.allowed[a] = true
			g.names = append(g.names, a)
		}
	}
	sort.Strings(g.names)
	return g
}

// Allowed returns the allowlist, sorted.
func (g *CardinalityGuard) Allowed() []string { return append([]string(nil), g.names...) }

// IsAllowed reports whether name may be an index label.
func (g *CardinalityGuard) IsAllowed(name string) bool { return g.allowed[name] }

// Apply returns a copy of s whose labels outside the allowlist are moved to
// every entry's structured metadata (an entry's own metadata wins).
func (g *CardinalityGuard) Apply(s Stream) Stream {
	moved := map[string]string{}
	labels := make(map[string]string, len(s.Labels))
	for k, v := range s.Labels {
		if g.allowed[k] {
			labels[k] = v
		} else {
			moved[k] = v
		}
	}
	if len(moved) == 0 {
		return Stream{Labels: labels, Entries: s.Entries}
	}
	entries := make([]Entry, len(s.Entries))
	for i, e := range s.Entries {
		md := make(map[string]string, len(moved)+len(e.StructuredMetadata))
		for k, v := range moved {
			md[k] = v
		}
		for k, v := range e.StructuredMetadata {
			md[k] = v
		}
		e.StructuredMetadata = md
		entries[i] = e
	}
	return Stream{Labels: labels, Entries: entries}
}

// Split divides attributes into index labels (allowlisted, non-empty values)
// and structured metadata. Keys are normalized with NormalizeLabelName;
// non-string values are JSON-encoded (numbers and booleans as their text).
func (g *CardinalityGuard) Split(attrs map[string]AttrValue) (labels, metadata map[string]string) {
	labels, metadata = map[string]string{}, map[string]string{}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic when two keys normalize to the same name
	for _, k := range keys {
		name := NormalizeLabelName(k)
		if name == "" {
			continue
		}
		v := attrString(attrs[k])
		if g.allowed[name] && v != "" && !strings.HasPrefix(name, "__") {
			labels[name] = v
		} else {
			metadata[name] = v
		}
	}
	return labels, metadata
}

// attrString renders an attribute value as a structured metadata string.
func attrString(v AttrValue) string {
	switch x := NormalizeAttrValue(v).(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		b, _ := json.Marshal(x) // normalized values always encode
		return string(b)
	}
}

// pushJSON is the JSON push body.
type pushJSON struct {
	Streams []pushJSONStream `json:"streams"`
}

type pushJSONStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

func encodePushJSON(streams []Stream) ([]byte, error) {
	body := pushJSON{Streams: make([]pushJSONStream, len(streams))}
	for i, s := range streams {
		labels := s.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		values := make([][]any, len(s.Entries))
		for j, e := range s.Entries {
			v := []any{strconv.FormatInt(e.Timestamp.UnixNano(), 10), e.Line}
			if len(e.StructuredMetadata) > 0 {
				v = append(v, e.StructuredMetadata)
			}
			values[j] = v
		}
		body.Streams[i] = pushJSONStream{Stream: labels, Values: values}
	}
	return json.Marshal(body)
}

func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b) // writes to a bytes.Buffer cannot fail
	_ = zw.Close()
	return buf.Bytes()
}

// LabelSetString renders labels in Prometheus label-set syntax with sorted
// names and Go-quoted values: {a="b", c="d"}.
func LabelSetString(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = k + "=" + strconv.Quote(labels[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// Protobuf encoding of logproto.PushRequest, hand-written from the field
// numbers of Loki's Apache-2.0 pkg/push/push.proto (v3.7.10):
//
//	PushRequest      { repeated StreamAdapter streams = 1; }
//	StreamAdapter    { string labels = 1; repeated EntryAdapter entries = 2; }
//	EntryAdapter     { google.protobuf.Timestamp timestamp = 1; string line = 2;
//	                   repeated LabelPairAdapter structuredMetadata = 3; }
//	LabelPairAdapter { string name = 1; string value = 2; }
//	Timestamp        { int64 seconds = 1; int32 nanos = 2; }
func encodePushProtobuf(streams []Stream) []byte {
	var out []byte
	for _, s := range streams {
		var sb []byte
		sb = pbString(sb, 1, LabelSetString(s.Labels))
		for _, e := range s.Entries {
			sb = pbBytes(sb, 2, encodeEntryProtobuf(e))
		}
		out = pbBytes(out, 1, sb)
	}
	return out
}

func encodeEntryProtobuf(e Entry) []byte {
	ns := e.Timestamp.UnixNano()
	secs, nanos := ns/1e9, ns%1e9
	if nanos < 0 {
		secs, nanos = secs-1, nanos+1e9
	}
	var ts []byte
	if secs != 0 {
		ts = pbVarint(pbTag(ts, 1, 0), uint64(secs)) // #nosec G115 -- protobuf int64 is two's complement
	}
	if nanos != 0 {
		ts = pbVarint(pbTag(ts, 2, 0), uint64(nanos))
	}
	var b []byte
	b = pbBytes(b, 1, ts)
	b = pbString(b, 2, e.Line)
	keys := make([]string, 0, len(e.StructuredMetadata))
	for k := range e.StructuredMetadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var pair []byte
		pair = pbString(pair, 1, k)
		pair = pbString(pair, 2, e.StructuredMetadata[k])
		b = pbBytes(b, 3, pair)
	}
	return b
}

func pbTag(b []byte, field int, wire uint64) []byte {
	return pbVarint(b, uint64(field)<<3|wire) // #nosec G115 -- field numbers are small constants
}

func pbVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func pbBytes(b []byte, field int, data []byte) []byte {
	b = pbTag(b, field, 2)
	b = pbVarint(b, uint64(len(data)))
	return append(b, data...)
}

func pbString(b []byte, field int, s string) []byte {
	return pbBytes(b, field, []byte(s))
}
