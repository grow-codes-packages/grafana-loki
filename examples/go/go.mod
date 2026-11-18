module github.com/grow-codes-packages/grafana-loki/examples/go

go 1.26.0

require github.com/grow-codes-packages/grafana-loki/go v0.0.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coder/websocket v1.8.14 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
)

replace github.com/grow-codes-packages/grafana-loki/go => ../../go
