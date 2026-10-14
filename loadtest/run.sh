#!/usr/bin/env bash
#
# Load-test harness for the rota proxy gateway.
#
# Brings up a local TLS destination (nginx) and an upstream proxy (gost) on the
# rota docker network, then runs k6 twice with the SAME script:
#   - baseline: k6 -> gost -> nginx
#   - via rota: k6 -> rota -> gost -> nginx
# The delta between the two isolates rota's overhead.
#
# noConnectionReuse in k6.js forces a fresh HTTP CONNECT tunnel per iteration,
# so `iteration_duration` reflects tunnel-establishment cost and
# `http_req_duration` reflects the steady-state (reused-tunnel) data path.
#
# Prereqs:
#   1. The rota stack is running (`docker compose up -d`).
#   2. A proxy-user is wired to a pool containing the gost upstream below.
#      See README.md for the one-time API wiring (uses address rota-lt-gost:8080).
#
# Tunables: VUS, DURATION, ROTA_PROXY.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
NET="$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' rota-core)"
VUS="${VUS:-20}"
DURATION="${DURATION:-15s}"
ROTA_PROXY="${ROTA_PROXY:-http://smoke:smoke123@rota-core:8000}"
GOST_PROXY="http://rota-lt-gost:8080"

echo "network: $NET  vus: $VUS  duration: $DURATION"

# 1. Self-signed cert for the TLS destination (generated once, git-ignored).
if [ ! -f "$DIR/certs/server.crt" ]; then
  mkdir -p "$DIR/certs"
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$DIR/certs/server.key" -out "$DIR/certs/server.crt" \
    -days 365 -subj "/CN=rota-lt-nginx"
fi

# 2. Destination (nginx+TLS) and upstream proxy (gost) on the rota network.
docker rm -f rota-lt-nginx rota-lt-gost >/dev/null 2>&1 || true
docker run -d --name rota-lt-nginx --network "$NET" \
  -v "$DIR/nginx.conf:/etc/nginx/nginx.conf:ro" \
  -v "$DIR/certs:/etc/nginx/certs:ro" nginx:alpine >/dev/null
docker run -d --name rota-lt-gost --network "$NET" \
  ginuerzh/gost -L=http://:8080 >/dev/null
sleep 2

STATS='--summary-trend-stats=avg,min,med,p(95),p(99),max'
k6run() {
  docker run --rm --network "$NET" \
    -e HTTPS_PROXY="$1" -e VUS="$VUS" -e DURATION="$DURATION" \
    -v "$DIR:/scripts" grafana/k6 run --quiet $STATS /scripts/k6.js
}

echo; echo "### BASELINE (k6 -> gost) ###"
k6run "$GOST_PROXY" | tee "$DIR/out_baseline.txt" | grep -E 'http_req_duration|iteration_duration|http_reqs'

echo; echo "### VIA ROTA (k6 -> rota -> gost) ###"
k6run "$ROTA_PROXY" | tee "$DIR/out_rota.txt" | grep -E 'http_req_duration|iteration_duration|http_reqs'

echo; echo "Overhead = (via-rota) - (baseline). Compare iteration_duration (cold tunnel) and http_req_duration (reused)."
echo "Cleanup: docker rm -f rota-lt-nginx rota-lt-gost"
