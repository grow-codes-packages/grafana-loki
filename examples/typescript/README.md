# TypeScript example

```sh
(cd ../../typescript && npm install && npm run build)
npm install
GROW_GRAFANA_LOKI_URL=http://127.0.0.1:3100 npm start
```

It checks health and the server version, pushes a stream, reads it back with the LogQL builder,
runs a metric query, logs through the batching log client and watches a live tail.
For a multi-tenant Loki set `GROW_GRAFANA_LOKI_TENANT_ID`; for a gateway or Grafana Cloud set
`GROW_GRAFANA_LOKI_USERNAME` + `GROW_GRAFANA_LOKI_PASSWORD` or `GROW_GRAFANA_LOKI_BEARER_TOKEN`.

Replace the `file:` dependency in `package.json` with a version (`npm install @grow-codes/grafana-loki`)
to run it against the published package.
