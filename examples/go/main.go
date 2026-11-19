// Command example shows the Grafana Loki Go SDK end to end: configuration
// from the environment, health, push, LogQL builder, range query, the
// batching log client with the Logger facade, search and a short live tail.
//
//	GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100 go run .
//
// Optional: GROW_GRAFANA_LOKI_TENANT_ID (X-Scope-OrgID), GROW_GRAFANA_LOKI_USERNAME /
// GROW_GRAFANA_LOKI_PASSWORD or GROW_GRAFANA_LOKI_BEARER_TOKEN for gateways and Grafana Cloud.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	loki "github.com/grow-codes-packages/grafana-loki/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loki.ConfigFromEnv()
	if err != nil {
		return err // CONFIG error naming the variable, never its value
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	fmt.Println("config:", cfg) // credentials print as [REDACTED]

	client, err := loki.NewClient(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }() // flushes log clients, stops tails

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	health, err := client.Health(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("health: healthy=%t status=%s\n", health.Healthy, health.Status)
	version, err := client.ServerVersion(ctx)
	if err != nil {
		return err
	}
	fmt.Println("loki version:", version)

	// Push one stream. "app" is not in the label allowlist, so the cardinality
	// guard sends it as structured metadata.
	service := fmt.Sprintf("sdk-example-go-%d", time.Now().UnixNano())
	err = client.Push(ctx, []loki.Stream{{
		Labels: map[string]string{"service_name": service, "level": "error", "app": "checkout"},
		Entries: []loki.Entry{
			{Line: `{"msg":"payment failed","status":502}`, StructuredMetadata: map[string]string{"request_id": "req-1"}},
			{Line: `{"msg":"payment ok","status":200}`},
		},
	}}, loki.PushOptions{Gzip: true})
	switch {
	case errors.Is(err, loki.ErrAuthentication):
		return fmt.Errorf("check the tenant id or gateway credentials: %w", err)
	case err != nil:
		return err
	}

	// Build LogQL safely and run a range query.
	query, err := loki.Selector(map[string]string{"service_name": service}).
		LineContains("payment").JSON().Where("status", ">=", 500).Build()
	if err != nil {
		return err
	}
	res, err := client.QueryRange(ctx, query, loki.QueryRangeOptions{Since: 5 * time.Minute, Limit: 10})
	if err != nil {
		return err
	}
	for _, s := range res.Streams {
		for _, e := range s.Entries {
			fmt.Printf("%s %s metadata=%v parsed=%v\n", e.Timestamp.Format(time.RFC3339Nano), e.Line, e.StructuredMetadata, e.Parsed)
		}
	}

	// Metric query.
	vec, err := client.Query(ctx, `sum(count_over_time({service_name="`+service+`"}[5m]))`, loki.QueryOptions{})
	if err != nil {
		return err
	}
	if len(vec.Vector) > 0 {
		fmt.Println("lines in the last 5m:", vec.Vector[0].Value.Value)
	}

	// Live tail: print what arrives in the next seconds.
	tail, err := client.Tail(ctx, `{service_name="`+service+`"}`, func(f loki.TailFrame) {
		for _, s := range f.Streams {
			for _, e := range s.Entries {
				fmt.Println("tail:", e.Line)
			}
		}
	}, loki.LiveTailOptions{})
	if err != nil {
		return err
	}
	defer tail.Stop()

	// Batching log client + Logger facade (records are redacted before export).
	logs, err := client.NewLogClient(loki.LogClientOptions{
		Resource: map[string]loki.AttrValue{"service.name": service, "deployment.environment": "example"},
	})
	if err != nil {
		return err
	}
	logger := logs.Logger("checkout", map[string]loki.AttrValue{"region": "eu"})
	logger.Info(ctx, "order placed", "order_id", 42, "password", "never-sent")
	logger.Error(ctx, "card declined", "order_id", 43)
	if err := logs.Flush(ctx); err != nil {
		return err
	}

	rec, err := client.LogQuery().WaitForLog(ctx, `{service_name="`+service+`"} |= "card declined"`, loki.WaitForLogOptions{Timeout: 10 * time.Second})
	if err != nil {
		return err
	}
	fmt.Printf("found: %s severity=%s attributes=%v\n", rec.Body, rec.Severity, rec.Attributes)
	time.Sleep(2 * time.Second) // give the tail a moment
	fmt.Println("batch stats:", logs.Stats())
	return nil
}
