import http from 'k6/http';
import { check, sleep } from 'k6';

// Held-concurrency test: each VU opens a tunnel and KEEPS it open (connection
// reuse, with a sleep between requests), so VUS ≈ number of concurrent tunnels
// held at once — matching the webshare profile (many long-lived connections,
// modest per-connection RPS). Measures whether rota holds N concurrent tunnels
// with flat latency, not establishment throughput.
//
//   VUS=3000 SLEEP=1  → ~3000 concurrent held tunnels, ~1 req/s each

const TARGET = __ENV.TARGET || 'https://rota-lt-nginx/';

export const options = {
  insecureSkipTLSVerify: true,
  // NOTE: no noConnectionReuse — we WANT the tunnel held open per VU.
  scenarios: {
    hold: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 3000),
      duration: __ENV.DURATION || '40s',
      gracefulStop: '5s',
    },
  },
  thresholds: { http_req_failed: ['rate<0.05'] },
};

export default function () {
  const res = http.get(TARGET);
  check(res, { 'status 200': (r) => r.status === 200 });
  sleep(Number(__ENV.SLEEP || 1)); // hold the tunnel open between requests
}
