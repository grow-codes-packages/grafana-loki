# TypeScript example

```sh
(cd ../../typescript && npm install && npm run build)
npm install
GROW_GRAFANA_LOKI_URL=http://127.0.0.1:<port> GROW_GRAFANA_LOKI_TOKEN=... npm start
```

Replace the `file:` dependency in `package.json` with a version (`npm install @grow-codes/grafana-loki`)
to run it against the published package.
