#!/usr/bin/env bash
# Held-concurrency run (on the box). VUS ≈ concurrent held tunnels.
set -u
cd /root/go/src/rota

VUS="${VUS:-3000}"; DUR="${DUR:-40s}"; SLEEP="${SLEEP:-1}"
docker rm -f k6hold >/dev/null 2>&1 || true

docker run --rm --name k6hold --network rota_rota-network \
  --ulimit nofile=1048576 \
  --sysctl net.ipv4.ip_local_port_range="1024 65535" \
  -e HTTPS_PROXY=http://smoke:smoke123@rota-core:8000 \
  -e VUS="$VUS" -e DURATION="$DUR" -e SLEEP="$SLEEP" \
  -v /root/go/src/rota/loadtest:/scripts grafana/k6 run --quiet \
  --summary-trend-stats="avg,med,p(95),p(99),max" /scripts/k6_hold.js > /tmp/k6hold.out 2>&1 &
K6PID=$!

echo "=== rota-core во время прогона (cpu / mem / pids / open-fds) ==="
for i in 1 2 3 4 5; do
  sleep 8
  LINE=$(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}} {{.PIDs}}' rota-core 2>/dev/null)
  PID=$(docker inspect -f '{{.State.Pid}}' rota-core 2>/dev/null)
  FDS=$(ls "/proc/$PID/fd" 2>/dev/null | wc -l)
  echo "  t+${i}x8s: cpu/mem/pids=[$LINE] open_fds=$FDS"
done

wait $K6PID
echo "=== k6 summary ==="
grep -E 'status 200|http_req_failed|http_req_duration|http_reqs|iteration_duration|vus' /tmp/k6hold.out
