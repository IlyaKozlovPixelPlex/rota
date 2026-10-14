# Load test — rota proxy gateway

Measures rota's per-request and per-tunnel overhead by running [k6](https://k6.io)
through a local, isolated chain so the result reflects rota — not internet noise.

```
k6  ──►  [ gost ]            ──►  nginx+TLS     (baseline)
k6  ──►  rota ──► gost       ──►  nginx+TLS     (via rota)
```

Everything runs on the rota docker network. `k6.js` uses `noConnectionReuse`, so
every iteration opens a fresh CONNECT tunnel:

- `iteration_duration` — full cost incl. tunnel establishment (the `Connection: close` case)
- `http_req_duration`  — steady-state data path only (the reused-tunnel case)

Rota overhead = (via-rota) − (baseline).

## Why local gost, not production proxies

Do **not** point the load test at the production proxy pool: it burns provider
quota, risks bans/rate-limits on real upstreams, and real 50–200 ms latency
hides rota's own overhead. Use local gost for overhead/ceiling numbers; use a
few real proxies only for a separate, low-volume realism check.

## Run

Prereqs: the stack is up (`docker compose up -d`) and rota has a proxy-user
pointing at a pool that contains the gost upstream.

One-time rota wiring (replace `$ADMINPW` with the admin password from the core logs):

```bash
BASE=http://localhost/api/v1
TOKEN=$(curl -s "$BASE/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$ADMINPW\"}" | jq -r .token)
A=(-H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json')

PID=$(curl -s "$BASE/proxies" "${A[@]}" -d '{"address":"rota-lt-gost:8080","protocol":"http","tags":["lt"]}' | jq -r .id)
POOL=$(curl -s "$BASE/pools" "${A[@]}" -d '{"name":"lt","rotation_method":"roundrobin","stick_count":1,"sync_mode":"manual","enabled":true}' | jq -r .id)
curl -s "$BASE/pools/$POOL/proxies" "${A[@]}" -d "{\"proxy_ids\":[$PID]}"
curl -s "$BASE/proxy-users" "${A[@]}" -d "{\"username\":\"smoke\",\"password\":\"smoke123\",\"enabled\":true,\"main_pool_id\":$POOL,\"fallback_pool_ids\":[],\"max_retries\":5,\"requests_per_minute\":0}"
```

Then:

```bash
VUS=20 DURATION=15s ./run.sh
```

## Per-core ceiling

To find the CONNECT/s ceiling on one core, restart the core with `GOMAXPROCS=1`
(add it to the `rota-core` environment in `docker-compose.yml`, `docker compose up -d rota-core`)
and re-run with a higher `VUS` until RPS plateaus.

## Files

| File | Purpose |
|------|---------|
| `run.sh` | Brings up nginx+gost, runs both k6 scenarios |
| `k6.js` | Load script (CONNECT per iteration via `noConnectionReuse`) |
| `nginx.conf` | Minimal TLS destination (tiny body) |
| `RESULTS.md` | Recorded results + interpretation |
| `certs/`, `out_*.txt` | Generated at runtime (git-ignored) |
