package loki

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// otlpScopeName is the instrumentation scope name used in OTLP payloads.
const otlpScopeName = "grow-codes-grafana-loki"

// Structured metadata keys the exporter writes for LogRecord fields (the same
// names Loki derives from OTLP records).
const (
	MetadataTraceID           = "trace_id"
	MetadataSpanID            = "span_id"
	MetadataTraceFlags        = "trace_flags"
	MetadataSeverityText      = "severity_text"
	MetadataSeverityNumber    = "severity_number"
	MetadataObservedTimestamp = "observed_timestamp"
)

// PushExporter is the LogExporter of the logging category: it groups records
// into Loki streams with the cardinality guard and posts them to
// /loki/api/v1/push.
//
// Mapping: resource and attribute keys are normalized ("service.name" →
// "service_name"); allowlisted keys become index labels, plus "level" (lower-case
// severity name) when allowed; everything else, and trace_id, span_id,
// trace_flags, severity_text, severity_number and observed_timestamp, becomes
// structured metadata. The record body is the log line.
type PushExporter struct {
	client *Client
	format PushFormat
}

var _ LogExporter = (*PushExporter)(nil)

// LokiLogExporter is the cross-language name of PushExporter (the name the
// TypeScript, Python and Java packages use).
type LokiLogExporter = PushExporter //nolint:revive // cross-language name shared with the other packages

// LokiOtlpExporter is the cross-language name of OTLPExporter.
type LokiOtlpExporter = OTLPExporter //nolint:revive // cross-language name shared with the other packages

// NewLokiLogExporter is NewPushExporter under its cross-language name.
func NewLokiLogExporter(client *Client, format PushFormat) *LokiLogExporter {
	return NewPushExporter(client, format)
}

// NewLokiOtlpExporter is NewOTLPExporter under its cross-language name.
func NewLokiOtlpExporter(client *Client) *LokiOtlpExporter { return NewOTLPExporter(client) }

// NewPushExporter returns an exporter for client using format ("" = JSON).
func NewPushExporter(client *Client, format PushFormat) *PushExporter {
	return &PushExporter{client: client, format: format}
}

// RecordsToStreams converts records into Loki streams using guard (nil =
// DefaultAllowedLabels). Streams are ordered by their label set.
func RecordsToStreams(records []LogRecord, guard *CardinalityGuard) []Stream {
	if guard == nil {
		guard = NewCardinalityGuard(nil)
	}
	byKey := map[string]*Stream{}
	var keys []string
	for _, r := range records {
		labels, md := recordLabels(r, guard)
		key := LabelSetString(labels)
		s, ok := byKey[key]
		if !ok {
			s = &Stream{Labels: labels}
			byKey[key] = s
			keys = append(keys, key)
		}
		s.Entries = append(s.Entries, Entry{Timestamp: r.Timestamp, Line: r.Body, StructuredMetadata: md})
	}
	sort.Strings(keys)
	out := make([]Stream, len(keys))
	for i, k := range keys {
		out[i] = *byKey[k]
	}
	return out
}

func recordLabels(r LogRecord, guard *CardinalityGuard) (labels, metadata map[string]string) {
	labels, metadata = guard.Split(r.Resource)
	al, am := guard.Split(r.Attributes)
	for k, v := range al {
		labels[k] = v
	}
	for k, v := range am {
		metadata[k] = v
	}
	sev := r.Severity
	if sev == SeverityUnspecified {
		sev = SeverityInfo
	}
	if _, set := labels["level"]; !set && guard.IsAllowed("level") {
		labels["level"] = strings.ToLower(Severity(wireSeverity(sev)).String())
	}
	sevText := r.SeverityText
	if sevText == "" {
		sevText = sev.String()
	}
	metadata[MetadataSeverityText] = sevText
	metadata[MetadataSeverityNumber] = strconv.Itoa(int(sev))
	if r.TraceID != "" {
		metadata[MetadataTraceID] = strings.ToLower(r.TraceID)
	}
	if r.SpanID != "" {
		metadata[MetadataSpanID] = strings.ToLower(r.SpanID)
	}
	if r.TraceFlags != nil {
		metadata[MetadataTraceFlags] = strconv.Itoa(int(*r.TraceFlags))
	}
	if !r.ObservedTimestamp.IsZero() {
		metadata[MetadataObservedTimestamp] = formatNano(r.ObservedTimestamp)
	}
	for k, v := range metadata {
		if v == "" {
			delete(metadata, k) // Loki drops empty metadata values anyway
		}
	}
	return labels, metadata
}

// Export pushes records. A rejected batch (4xx) reports every record as
// rejected; retryable errors (429, 5xx, network) are returned for the batch
// processor to retry.
func (e *PushExporter) Export(ctx context.Context, records []LogRecord, opts ExportOptions) (ExportResult, error) {
	if len(records) == 0 {
		return ExportResult{}, nil
	}
	streams := RecordsToStreams(records, e.client.guard)
	// Labels are already guarded; a second pass could move "level" away.
	if err := e.client.push(ctx, streams, PushOptions{Format: e.format, Gzip: opts.Gzip}, false); err != nil {
		return exportFailure(len(records), err)
	}
	return ExportResult{Accepted: len(records)}, nil
}

func exportFailure(n int, err error) (ExportResult, error) {
	var e *Error
	if errors.As(err, &e) && !e.Retryable {
		return ExportResult{Rejected: n, Error: e}, e
	}
	return ExportResult{}, err
}

// Shutdown is a no-op (the client owns the connections).
func (e *PushExporter) Shutdown(context.Context) error { return nil }

// OTLPExporter is a LogExporter that posts OTLP/JSON logs to Loki's native
// OTLP endpoint (/otlp/v1/logs). Loki promotes service.name,
// deployment.environment and other default resource attributes to labels and
// stores everything else as structured metadata.
type OTLPExporter struct {
	client *Client
	scope  string
}

var _ LogExporter = (*OTLPExporter)(nil)

// NewOTLPExporter returns an OTLP exporter for client.
func NewOTLPExporter(client *Client) *OTLPExporter {
	return &OTLPExporter{client: client, scope: otlpScopeName}
}

// Export posts records grouped by resource.
func (e *OTLPExporter) Export(ctx context.Context, records []LogRecord, opts ExportOptions) (ExportResult, error) {
	if len(records) == 0 {
		return ExportResult{}, nil
	}
	if err := e.client.pushOTLP(ctx, records, opts.Gzip, e.scope); err != nil {
		return exportFailure(len(records), err)
	}
	return ExportResult{Accepted: len(records)}, nil
}

// OTLPPushOptions are the options of Client.PushOTLP.
type OTLPPushOptions struct {
	// Gzip compresses the body.
	Gzip bool
}

// PushOTLP posts records as OTLP/JSON to Loki's native OTLP endpoint
// (POST /otlp/v1/logs). Loki promotes its default resource attributes
// (service.name, deployment.environment, ...) to labels and stores everything
// else as structured metadata. Empty records send nothing. Like Push it is
// retried on 429/5xx (Loki drops exact duplicates).
func (c *Client) PushOTLP(ctx context.Context, records []LogRecord, opts OTLPPushOptions) error {
	if len(records) == 0 {
		return nil
	}
	return c.pushOTLP(ctx, records, opts.Gzip, otlpScopeName)
}

func (c *Client) pushOTLP(ctx context.Context, records []LogRecord, gzip bool, scope string) error {
	body, err := json.Marshal(otlpRequest(records, scope))
	if err != nil {
		return ValidationError("otlpLogs", "records cannot be encoded")
	}
	req := &request{Method: http.MethodPost, Path: pathOTLPLogs, Operation: "otlpLogs", Idempotent: true, ContentType: "application/json", Header: http.Header{}}
	if gzip {
		body = gzipBytes(body)
		req.Header.Set("Content-Encoding", "gzip")
	}
	req.Body = body
	_, err = c.send(ctx, req)
	return err
}

// Shutdown is a no-op (the client owns the connections).
func (e *OTLPExporter) Shutdown(context.Context) error { return nil }

type otlpKV struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func otlpAttrs(m map[string]AttrValue) []otlpKV {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]otlpKV, 0, len(keys))
	for _, k := range keys {
		if v := NormalizeAttrValue(m[k]); v != nil {
			out = append(out, otlpKV{Key: k, Value: otlpAny(v)})
		}
	}
	return out
}

// otlpAny renders a normalized attribute value as an OTLP/JSON AnyValue.
func otlpAny(v AttrValue) map[string]any {
	switch x := v.(type) {
	case bool:
		return map[string]any{"boolValue": x}
	case int64:
		return map[string]any{"intValue": strconv.FormatInt(x, 10)}
	case float64:
		return map[string]any{"doubleValue": x}
	case []AttrValue:
		vals := make([]map[string]any, len(x))
		for i, e := range x {
			vals[i] = otlpAny(e)
		}
		return map[string]any{"arrayValue": map[string]any{"values": vals}}
	case map[string]AttrValue:
		return map[string]any{"kvlistValue": map[string]any{"values": otlpAttrs(x)}}
	case string:
		return map[string]any{"stringValue": x}
	}
	return map[string]any{"stringValue": attrString(v)}
}

func otlpRequest(records []LogRecord, scope string) map[string]any {
	type group struct {
		resource map[string]AttrValue
		logs     []map[string]any
	}
	groups := map[string]*group{}
	var order []string
	for _, r := range records {
		key, _ := json.Marshal(NormalizeAttributes(r.Resource)) // map keys are sorted
		g, ok := groups[string(key)]
		if !ok {
			g = &group{resource: r.Resource}
			groups[string(key)] = g
			order = append(order, string(key))
		}
		ts := r.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		sev := r.Severity
		if sev == SeverityUnspecified {
			sev = SeverityInfo
		}
		text := r.SeverityText
		if text == "" {
			text = sev.String()
		}
		lr := map[string]any{
			"timeUnixNano":   formatNano(ts),
			"severityNumber": int(sev),
			"severityText":   text,
			"body":           map[string]any{"stringValue": r.Body},
			"attributes":     otlpAttrs(r.Attributes),
		}
		if !r.ObservedTimestamp.IsZero() {
			lr["observedTimeUnixNano"] = formatNano(r.ObservedTimestamp)
		}
		if r.TraceID != "" {
			lr["traceId"] = strings.ToLower(r.TraceID)
		}
		if r.SpanID != "" {
			lr["spanId"] = strings.ToLower(r.SpanID)
		}
		if r.TraceFlags != nil {
			lr["flags"] = int(*r.TraceFlags)
		}
		g.logs = append(g.logs, lr)
	}
	rls := make([]map[string]any, len(order))
	for i, k := range order {
		g := groups[k]
		rls[i] = map[string]any{
			"resource":  map[string]any{"attributes": otlpAttrs(g.resource)},
			"scopeLogs": []map[string]any{{"scope": map[string]any{"name": scope, "version": Version}, "logRecords": g.logs}},
		}
	}
	return map[string]any{"resourceLogs": rls}
}

// NewLogClient returns a batching LogClient that pushes to Loki through a
// JSON PushExporter. Close of the Client flushes and closes it.
func (c *Client) NewLogClient(opts LogClientOptions) (*BatchingLogClient, error) {
	return c.NewLogClientWithExporter(NewPushExporter(c, PushFormatJSON), opts)
}

// NewOTLPLogClient returns a batching LogClient that exports through Loki's
// OTLP endpoint (/otlp/v1/logs). Close of the Client flushes and closes it.
func (c *Client) NewOTLPLogClient(opts LogClientOptions) (*BatchingLogClient, error) {
	return c.NewLogClientWithExporter(NewOTLPExporter(c), opts)
}

// NewLogClientWithExporter returns a batching LogClient for any exporter (for
// example NewPushExporter(c, PushFormatProtobuf)). Close of the Client flushes
// and closes it.
func (c *Client) NewLogClientWithExporter(exporter LogExporter, opts LogClientOptions) (*BatchingLogClient, error) {
	lc, err := NewBatchingLogClient(exporter, opts)
	if err != nil {
		return nil, err
	}
	c.onClose(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = lc.Close(ctx)
	})
	return lc, nil
}
