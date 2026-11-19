# Java examples

Install the package into your local Maven repository, point it at a running Grafana Loki and run an
example:

```bash
make -C ../../java pack            # builds and installs codes.grow.packages:grafana-loki
export GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100
export GROW_GRAFANA_LOKI_TENANT_ID=...   # multi-tenant Loki (X-Scope-OrgID)
mvn -q compile exec:java                                                  # BasicExample
mvn -q compile exec:java -Dexec.mainClass=codes.grow.packages.loki.examples.AsyncExample
```

Against the published package, set `-Dsdk.version=<version>` (or edit `pom.xml`) instead of
installing it locally.

| Class | Shows |
| - | - |
| `BasicExample` | Loki tour: health, version, push with structured metadata, tail, LogQL builder, range and metric queries, labels, log client + search |
| `AsyncExample` | `…Async` methods, combining futures, cancellation |
| `HooksAndLoggingExample` | request hooks, a logger, retries and timeouts |
