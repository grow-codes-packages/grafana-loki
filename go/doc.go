// Package loki is the Grow Codes SDK for Grafana Loki.
//
// Create a client from the environment (GROW_GRAFANA_LOKI_URL or LOKI_URL,
// GROW_GRAFANA_LOKI_TENANT_ID or LOKI_TENANT_ID, Basic or Bearer credentials
// for gateways) or from a Config literal:
//
//	client, err := loki.NewClientFromEnv()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	err = client.Push(ctx, []loki.Stream{{
//		Labels:  map[string]string{"service_name": "api", "level": "info"},
//		Entries: []loki.Entry{{Line: "hello", StructuredMetadata: map[string]string{"request_id": "r-1"}}},
//	}}, loki.PushOptions{})
//
//	q := loki.Selector(map[string]string{"service_name": "api"}).LineContains("hello").String()
//	res, err := client.QueryRange(ctx, q, loki.QueryRangeOptions{Since: time.Hour})
//
// Service functions: Push (JSON or protobuf + Snappy, with label validation
// and a cardinality guard), Query, QueryRange, Labels, LabelValues, Series,
// Tail (WebSocket), IndexStats, Volume, VolumeRange, FormatQuery, Ready,
// BuildInfo and WithTenant. The logging category is available through
// Client.NewLogClient / NewOTLPLogClient (batching LogClient, Logger facade,
// slog handler, zapadapter) and Client.LogQuery (LogQueryClient: Search with
// cursors, Tail, ListStreams, ListLabels, WaitForLog).
//
// Every error is an *Error; compare codes with errors.Is against the Err*
// sentinels (errors.Is(err, loki.ErrNotFound)). Credentials are wrapped in
// Secret and never appear in errors, hooks or logs.
//
// The protobuf push encoder is written from the field numbers of Loki's
// Apache-2.0 pkg/push/push.proto; no Loki code is imported. Grafana Loki is a
// trademark of Grafana Labs. This package is not affiliated with or endorsed
// by Grafana Labs.
package loki
