package proxy

import (
	"crypto/subtle"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// proxyMetrics holds the Prometheus instruments for the proxy layer.
// A dedicated registry is used so these metrics don't conflict with any
// default Go runtime metrics registered elsewhere.
type proxyMetrics struct {
	registry *prometheus.Registry

	// requestsTotal counts every completed proxy request (HTTP + CONNECT).
	// Labels: upstream = proxy address, user = proxy-user name (or "" for
	// legacy/unauthenticated), result = "success" | "fail".
	requestsTotal *prometheus.CounterVec

	// connectLatency observes the time from receiving a CONNECT request to
	// the moment the upstream tunnel is established (before data copy starts).
	// Labels: upstream = proxy address.
	connectLatency *prometheus.HistogramVec

	// upstreamFailTotal counts upstream connection / request failures.
	// Labels: upstream = proxy address.
	upstreamFailTotal *prometheus.CounterVec

	// activeTunnels is the current number of live CONNECT tunnels.
	activeTunnels prometheus.Gauge
}

// pmetrics is the package-level singleton, initialised once at startup.
var pmetrics = newProxyMetrics()

func newProxyMetrics() *proxyMetrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	return &proxyMetrics{
		registry: reg,
		requestsTotal: f.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rota_requests_total",
				Help: "Total proxy requests by upstream address, proxy user, and result.",
			},
			[]string{"upstream", "user", "result"},
		),
		connectLatency: f.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "rota_connect_latency_seconds",
				Help:    "Upstream CONNECT tunnel establishment time in seconds.",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
			},
			[]string{"upstream"},
		),
		upstreamFailTotal: f.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rota_upstream_fail_total",
				Help: "Total upstream failures by proxy address.",
			},
			[]string{"upstream"},
		),
		activeTunnels: f.NewGauge(
			prometheus.GaugeOpts{
				Name: "rota_active_tunnels",
				Help: "Number of currently active CONNECT tunnels.",
			},
		),
	}
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics.
// Mount it on the admin/API server (port 8001), not the proxy data-plane (8000).
//
// Access control:
//   - If METRICS_BASIC_USER and METRICS_BASIC_PASSWORD are both set, the handler
//     enforces HTTP basic auth (constant-time comparison).
//   - If either variable is unset or empty, every request gets 403 (closed by default).
//
// Called once at startup; env vars are read here and captured in the closure.
func MetricsHandler() http.Handler {
	user := os.Getenv("METRICS_BASIC_USER")
	pass := os.Getenv("METRICS_BASIC_PASSWORD")

	if user == "" || pass == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "metrics endpoint disabled: set METRICS_BASIC_USER and METRICS_BASIC_PASSWORD", http.StatusForbidden)
		})
	}

	inner := promhttp.HandlerFor(pmetrics.registry, promhttp.HandlerOpts{})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Rota Metrics"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	})
}
