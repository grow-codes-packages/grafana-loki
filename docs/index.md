# Grafana Loki SDK

[![Grow Codes](https://img.shields.io/badge/Grow%20Codes-grow.codes-2ea44f)](https://grow.codes)
[![CI](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml/badge.svg)](https://github.com/grow-codes-packages/grafana-loki/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](license.md)

One API for **Grafana Loki** in five languages: TypeScript, Rust, Go, Python and Java. Every package
has the same functions, options, defaults, error codes and behaviour, tested against a real
`grafana/loki:3.7.10` in Docker.

| Language | Package | Coverage |
| - | - | - |
| TypeScript | [`@grow-codes/grafana-loki`](https://www.npmjs.com/package/@grow-codes/grafana-loki) | ![TypeScript coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-typescript.json) |
| Rust | [`grow-grafana-loki`](https://crates.io/crates/grow-grafana-loki) | ![Rust coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-rust.json) |
| Go | [`github.com/grow-codes-packages/grafana-loki/go`](https://pkg.go.dev/github.com/grow-codes-packages/grafana-loki/go) | ![Go coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-go.json) |
| Python | [`grow-grafana-loki`](https://pypi.org/project/grow-grafana-loki/) | ![Python coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-python.json) |
| Java | [`codes.grow.packages:grafana-loki`](https://central.sonatype.com/artifact/codes.grow.packages/grafana-loki) | ![Java coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/grafana-loki/badges/coverage-java.json) |

## What you get

- **Client** with health, authenticated ping and server version ([Client](client.md)).
- **Configuration** from code or `GROW_GRAFANA_LOKI_*` / vendor environment variables, immutable
  and validated ([Configuration](configuration.md)).
- **Retries** with full-jitter backoff and `Retry-After`, connect/request/total timeouts and
  cancellation in every language.
- **Typed errors** with one set of codes across languages ([Error handling](errors.md)).
- **Redaction everywhere**: credentials never appear in errors, logs, hooks or `toString`
  ([Redaction](redaction.md)).
- **Logging**: batching log client, logger facade, redaction processor, framework adapters and
  log search ([Logging](logging.md)).
- **Service functions** for the Grafana Loki API ([Functions](functions.md)).

Start with [Install](install.md) and the [Quickstart](quickstart.md).

!!! note "Independent project"
    This SDK is made by [Grow Codes](https://grow.codes). Grafana Loki is a trademark of
    its owner (Grafana Labs); this project is not affiliated with or endorsed by it. See
    [License & disclaimer](license.md).
