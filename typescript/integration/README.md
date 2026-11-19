# TypeScript integration tests

A consumer project: it installs the packed `@grow-codes/grafana-loki` tarball (never the source tree)
and runs the integration and conformance tests with `node --test` against a real Grafana Loki.

```sh
cd typescript && make pack            # writes dist/grow-codes-grafana-loki-<version>.tgz
cd integration && npm run install-package
GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100 npm test   # plus the /bootstrap/env values for tenants and the gateway
```

In CI and locally with Docker this runs inside `docker/Dockerfile.typescript`
(`make test-docker LANGS=typescript` from the repository root), which reads `/bootstrap/env`.
