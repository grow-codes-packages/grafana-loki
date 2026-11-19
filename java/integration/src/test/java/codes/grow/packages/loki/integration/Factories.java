package codes.grow.packages.loki.integration;

import codes.grow.packages.loki.LokiClient;
import codes.grow.packages.loki.LokiLogClientOptions;
import codes.grow.packages.loki.core.EnvReader;
import codes.grow.packages.loki.logging.BatchOptions;
import codes.grow.packages.loki.logging.BatchingLogClient;
import codes.grow.packages.loki.testing.ConformanceRunner;
import codes.grow.packages.loki.testing.logging.LoggingTarget;
import java.util.Map;

/**
 * Conformance targets of this service (owned by the service repository).
 *
 * <p>{@code core} is the client against the single-tenant Loki ({@code GROW_GRAFANA_LOKI_URL});
 * {@code logging} is a batching {@code LogClient} over JSON push plus the Loki {@code
 * LogQueryClient}. Loki serves every logging capability: query ({@code query_range}), tail
 * (WebSocket), labels and streams ({@code series}), and the server version ({@code buildinfo}).
 */
final class Factories {
  /** Capabilities the deployment under test offers (see the scenario {@code requires}). */
  static final String[] CAPABILITIES = {"version", "query", "tail", "labels", "streams"};

  /** Service name used as the {@code service.name} resource of conformance records. */
  static final String SERVICE_NAME = "grow-conformance-java";

  private Factories() {}

  static void register(ConformanceRunner.Builder runner, EnvReader env) {
    runner.serviceName(SERVICE_NAME);
    runner.target("core", scenario -> LokiClient.builder().fromEnv(env).build());
    runner.target(
        "logging",
        scenario -> {
          LokiClient client = LokiClient.builder().fromEnv(env).build();
          BatchingLogClient logs =
              client.logClient(
                  LokiLogClientOptions.builder()
                      .batchOptions(LoggingTarget.batchOptions(scenario, BatchOptions.defaults()))
                      .resource(
                          Map.of(
                              "service.name", SERVICE_NAME,
                              "deployment.environment", "conformance"))
                      .build());
          return LoggingTarget.of(logs, client.logQueryClient());
        });
  }
}
