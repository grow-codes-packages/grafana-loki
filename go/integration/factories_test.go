package integration

import (
	"context"
	"strings"
	"testing"

	loki "github.com/grow-codes-packages/grafana-loki/go"
	"github.com/grow-codes-packages/grafana-loki/go/testkit"
)

// capabilities the deployment under test offers (see conformance "requires").
// Loki serves every logging capability: query (query_range), tail (WebSocket),
// labels and streams (series), and the server version (buildinfo).
var capabilities = []string{"version", "query", "tail", "labels", "streams"}

// categories runs "core" plus this service's categories.
func categories() []string {
	return append([]string{"core"}, strings.Split("logging", ",")...)
}

// factories builds the conformance targets against the single-tenant Loki
// (GROW_GRAFANA_LOKI_URL): core → *loki.Client, logging → a batching
// LogClient over JSON push plus the LogQuery client.
func factories(t *testing.T) testkit.Factories {
	t.Helper()
	return testkit.Factories{
		"core": testkit.CoreFactory(func(context.Context) (testkit.CoreTarget, error) {
			return loki.NewClientFromEnv()
		}),
		"logging": func(ctx context.Context, env *testkit.Env) (any, error) {
			c, err := loki.NewClientFromEnv()
			if err != nil {
				return nil, err
			}
			env.AddCleanup(func() { _ = c.Close() })
			return testkit.LoggingFactory(func(_ context.Context, opts loki.BatchOptions) (testkit.LoggingTarget, error) {
				lc, err := c.NewLogClient(loki.LogClientOptions{
					Batch:    opts,
					Resource: map[string]loki.AttrValue{"service.name": env.ServiceName, "deployment.environment": "conformance"},
				})
				if err != nil {
					return testkit.LoggingTarget{}, err
				}
				return testkit.LoggingTarget{Client: lc, Query: c.LogQuery()}, nil
			})(ctx, env)
		},
	}
}
