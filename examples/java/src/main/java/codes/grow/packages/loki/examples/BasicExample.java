// Created by sdk-kit 0.1.0; owned by this repository.
package codes.grow.packages.loki.examples;

import codes.grow.packages.loki.LogQL;
import codes.grow.packages.loki.LokiClient;
import codes.grow.packages.loki.LokiEntry;
import codes.grow.packages.loki.LokiLogClientOptions;
import codes.grow.packages.loki.LokiStream;
import codes.grow.packages.loki.LokiTail;
import codes.grow.packages.loki.QueryRangeOptions;
import codes.grow.packages.loki.QueryResult;
import codes.grow.packages.loki.StreamResult;
import codes.grow.packages.loki.core.HealthStatus;
import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.logging.BatchingLogClient;
import codes.grow.packages.loki.logging.LogRecord;
import codes.grow.packages.loki.logging.WaitForLogOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A tour of the Loki client: status, push with structured metadata, LogQL builder, range and metric
 * queries, labels, live tail and the logging category (batching log client + search).
 *
 * <p>Configuration comes from {@code GROW_GRAFANA_LOKI_URL} / {@code LOKI_URL} (default {@code
 * http://127.0.0.1:3100}), {@code LOKI_TENANT_ID}, {@code LOKI_USERNAME}/{@code LOKI_PASSWORD} or
 * {@code LOKI_BEARER_TOKEN}.
 */
public final class BasicExample {
  private BasicExample() {}

  /**
   * Runs the example.
   *
   * @param args unused
   * @throws InterruptedException when interrupted while waiting for the tail
   */
  public static void main(String[] args) throws InterruptedException {
    String run = UUID.randomUUID().toString().substring(0, 8);
    try (LokiClient client = LokiClient.fromEnv()) {
      HealthStatus health = client.health(); // never throws for an unreachable service
      System.out.println("health: " + health);
      if (!health.healthy()) {
        return;
      }
      System.out.println("server version: " + client.serverVersion());

      // Live tail: only entries pushed from now on.
      LinkedBlockingQueue<String> tailed = new LinkedBlockingQueue<>();
      LokiTail tail =
          client.tail(
              "{service_name=\"example\"}",
              frame -> frame.streams().forEach(s -> s.entries().forEach(e -> tailed.add(e.line()))));

      // Push: "run" is not in the cardinality guard allowlist, so it is stored as structured
      // metadata instead of an index label.
      client.push(
          List.of(
              LokiStream.of(
                  Map.of("service_name", "example", "level", "info", "run", run),
                  LokiEntry.of(Instant.now(), "hello " + run, Map.of("request_id", "r-1")),
                  LokiEntry.of(Instant.now(), "{\"msg\":\"slow\",\"status\":503}"))));
      System.out.println("tail: " + tailed.poll(10, TimeUnit.SECONDS));
      tail.stop();

      // LogQL builder + range query
      String logql =
          LogQL.selector(Map.of("service_name", "example"))
              .where("run", "=", run)
              .json()
              .where("status", ">=", 500)
              .build();
      QueryResult result = client.queryRange(logql, QueryRangeOptions.builder().limit(10).build());
      for (StreamResult s : result.streams()) {
        s.entries().forEach(e -> System.out.println(s.labels() + " " + e.line() + " " + e.parsed()));
      }
      // Metric query
      QueryResult count =
          client.query("sum(count_over_time({service_name=\"example\"} | run=\"" + run + "\" [5m]))");
      System.out.println("count: " + count.vector());
      System.out.println("labels: " + client.labels());
      System.out.println("levels: " + client.labelValues("level"));

      // Logging category: batching log client with redaction, then search it back
      BatchingLogClient logs =
          client.logClient(
              LokiLogClientOptions.builder().resource(Map.of("service.name", "example")).build());
      logs.logger("checkout", Map.of("run", run)).error("payment failed", Map.of("password", "x"));
      logs.flush(Duration.ofSeconds(5));
      LogRecord found =
          client
              .logQueryClient()
              .waitForLog("payment failed", WaitForLogOptions.of(Duration.ofSeconds(10), Duration.ofMillis(250)));
      System.out.println("found: " + found.body() + " " + found.attributes());
    } catch (LokiException e) {
      // messages never contain credentials
      System.err.println(e.code() + ": " + e.getMessage());
    }
  }
}
