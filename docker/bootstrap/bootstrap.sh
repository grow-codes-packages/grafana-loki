#!/bin/sh
# Loki fixture bootstrap (GRO-1374). Loki has no users or tokens; this script only proves both instances
# serve requests, checks the version and tenant enforcement, and writes the connection facts to
# /bootstrap/env. Idempotent: every run re-checks and atomically rewrites the same file.
set -eu

URL="${LOKI_INTERNAL_URL:-http://grafana-loki:3100}"
MT_URL="${LOKI_MT_INTERNAL_URL:-http://grafana-loki-mt:3100}"
WANT="${LOKI_EXPECTED_VERSION:-3.7.10}"
TA="${LOKI_TENANT_A:-sdk-tenant-a}"
TB="${LOKI_TENANT_B:-sdk-tenant-b}"
AUTH_URL="${LOKI_AUTH_PROXY_URL:-http://grafana-loki-auth:8080}"
: "${LOKI_PROXY_USERNAME:?}" "${LOKI_PROXY_PASSWORD:?}" "${LOKI_PROXY_BEARER_TOKEN:?}"
OUT=/bootstrap/env

log() { echo "bootstrap: $*" >&2; }

wait_ready() { # $1 = base URL; /ready answers 503 "Ingester not ready" for ~15 s after start
  i=0
  until [ "$(curl -s -o /dev/null -w '%{http_code}' "$1/ready")" = 200 ]; do
    i=$((i + 1)); [ "$i" -le 90 ] || { log "$1/ready not 200 after 90 s"; exit 1; }
    sleep 1
  done
}

wait_ready "$URL"
wait_ready "$MT_URL"

ver=$(curl -sf "$URL/loki/api/v1/status/buildinfo" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
[ "$ver" = "$WANT" ] || { log "unexpected Loki version '$ver' (want $WANT)"; exit 1; }

# Data APIs must answer 200 on the single-tenant instance (no tenant header needed) ...
code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/loki/api/v1/labels")
[ "$code" = 200 ] || { log "single-tenant labels returned $code"; exit 1; }
# ... and 401 without / 200 with X-Scope-OrgID on the multi-tenant one.
code=$(curl -s -o /dev/null -w '%{http_code}' "$MT_URL/loki/api/v1/labels")
[ "$code" = 401 ] || { log "multi-tenant labels without tenant returned $code (want 401)"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-Scope-OrgID: $TA" "$MT_URL/loki/api/v1/labels")
[ "$code" = 200 ] || { log "multi-tenant labels with tenant returned $code"; exit 1; }

# Test gateway: 401 without credentials, 200 with Basic and with Bearer (credentials never printed).
code=$(curl -s -o /dev/null -w '%{http_code}' "$AUTH_URL/loki/api/v1/labels")
[ "$code" = 401 ] || { log "auth gateway without credentials returned $code (want 401)"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -u "$LOKI_PROXY_USERNAME:$LOKI_PROXY_PASSWORD" "$AUTH_URL/loki/api/v1/labels")
[ "$code" = 200 ] || { log "auth gateway with Basic returned $code (check LOKI_PROXY_BASIC_B64)"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $LOKI_PROXY_BEARER_TOKEN" "$AUTH_URL/loki/api/v1/labels")
[ "$code" = 200 ] || { log "auth gateway with Bearer returned $code"; exit 1; }

mkdir -p /bootstrap
tmp="$OUT.tmp.$$"
cat > "$tmp" <<EOF
GROW_GRAFANA_LOKI_URL=$URL
GROW_GRAFANA_LOKI_MULTI_TENANT_URL=$MT_URL
GROW_GRAFANA_LOKI_TENANT_A=$TA
GROW_GRAFANA_LOKI_TENANT_B=$TB
GROW_GRAFANA_LOKI_EXPECTED_VERSION=$ver
GROW_GRAFANA_LOKI_AUTH_PROXY_URL=$AUTH_URL
GROW_GRAFANA_LOKI_AUTH_PROXY_USERNAME=$LOKI_PROXY_USERNAME
GROW_GRAFANA_LOKI_AUTH_PROXY_PASSWORD=$LOKI_PROXY_PASSWORD
GROW_GRAFANA_LOKI_AUTH_PROXY_BEARER_TOKEN=$LOKI_PROXY_BEARER_TOKEN
EOF
chmod 0644 "$tmp"           # test-only gateway credentials; the test containers run as non-root
mv "$tmp" "$OUT"
log "ok (Loki $ver, single-tenant $URL, multi-tenant $MT_URL, auth gateway $AUTH_URL)"
