import http from 'k6/http';
import { check } from 'k6';

// Load test for the rota proxy gateway.
//
// Measures tunnel-establishment overhead by forcing a brand-new connection
// (and thus a new HTTP CONNECT tunnel) on every iteration via noConnectionReuse.
// The proxy is selected by the HTTPS_PROXY env var, so the SAME script runs
// both the baseline (straight to gost) and the through-rota case — only the
// env var changes:
//
//   baseline:  HTTPS_PROXY=http://rota-smoke-gost:8080
//   via rota:  HTTPS_PROXY=http://smoke:smoke123@rota-core:8000
//
// Target is the local nginx+TLS. insecureSkipTLSVerify accepts its self-signed cert.
//
// Tunables (env): TARGET (url), VUS, DURATION.

const TARGET = __ENV.TARGET || 'https://rota-lt-nginx/';

export const options = {
  noConnectionReuse: true,        // new tunnel per iteration → measures CONNECT/s
  insecureSkipTLSVerify: true,    // self-signed destination
  scenarios: {
    load: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 20),
      duration: __ENV.DURATION || '20s',
    },
  },
  thresholds: {
    // informational; we read p50/p95/p99 from the summary
    http_req_failed: ['rate<0.05'],
  },
};

export default function () {
  const res = http.get(TARGET);
  check(res, { 'status 200': (r) => r.status === 200 });
}
