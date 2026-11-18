// Package loki is the Grow Codes SDK for Grafana Loki.
//
// Create a client from the environment (GROW_GRAFANA_LOKI_URL,
// GROW_GRAFANA_LOKI_TOKEN, ...) or from a Config literal:
//
//	client, err := loki.NewClientFromEnv()
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	status, err := client.Health(ctx)
//
// Every error is an *Error; compare codes with errors.Is against the Err*
// sentinels (errors.Is(err, loki.ErrNotFound)). Credentials are wrapped in
// Secret and never appear in errors, hooks or logs.
//
// Grafana Loki is a trademark of Grafana Labs. This package is not affiliated
// with or endorsed by Grafana Labs.
package loki
