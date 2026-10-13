package proxy

import (
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

// makeSelector builds a PoolSelector with a pre-loaded proxy list (no DB needed).
func makeSelector(method string, proxies ...*models.Proxy) *PoolSelector {
	ps := &PoolSelector{
		method:     method,
		proxies:    proxies,
		failCounts: make(map[int]int),
		banCounts:  make(map[int]int),
		quarantine: make(map[int]time.Time),
	}
	return ps
}

func proxy(id int) *models.Proxy { return &models.Proxy{ID: id, Address: "127.0.0.1:0"} }

// TestReportFailure_Threshold verifies that a proxy enters quarantine only after
// chainFailureThreshold consecutive failures, not before.
func TestReportFailure_Threshold(t *testing.T) {
	ps := makeSelector("roundrobin", proxy(1))

	for i := 0; i < chainFailureThreshold-1; i++ {
		ps.ReportFailure(1)
		if _, ok := ps.quarantine[1]; ok {
			t.Fatalf("quarantined after %d failures, want %d", i+1, chainFailureThreshold)
		}
	}

	ps.ReportFailure(1)
	until, ok := ps.quarantine[1]
	if !ok {
		t.Fatal("proxy not quarantined after threshold")
	}
	if !until.After(time.Now()) {
		t.Fatal("quarantine deadline is in the past")
	}
}

// TestReportSuccess_ClearsQuarantine verifies that a success lifts an active quarantine.
func TestReportSuccess_ClearsQuarantine(t *testing.T) {
	ps := makeSelector("roundrobin", proxy(1))

	for i := 0; i < chainFailureThreshold; i++ {
		ps.ReportFailure(1)
	}
	if _, ok := ps.quarantine[1]; !ok {
		t.Fatal("expected quarantine after threshold failures")
	}

	ps.ReportSuccess(1)
	if _, ok := ps.quarantine[1]; ok {
		t.Fatal("quarantine not cleared after success")
	}
	if ps.failCounts[1] != 0 {
		t.Fatal("failCount not reset after success")
	}
}

// TestExponentialBackoff verifies the quarantine duration doubles on each ban.
func TestExponentialBackoff(t *testing.T) {
	ps := makeSelector("roundrobin", proxy(1))

	triggerBan := func() time.Duration {
		// reset failures first so we start a fresh run
		ps.failCounts[1] = 0
		before := time.Now()
		for i := 0; i < chainFailureThreshold; i++ {
			ps.ReportFailure(1)
		}
		until := ps.quarantine[1]
		return until.Sub(before)
	}

	d0 := triggerBan()
	if d0 < quarantineBase || d0 > quarantineBase+time.Second {
		t.Fatalf("first ban duration %v, want ~%v", d0, quarantineBase)
	}

	d1 := triggerBan()
	if d1 < 2*quarantineBase || d1 > 2*quarantineBase+time.Second {
		t.Fatalf("second ban duration %v, want ~%v", d1, 2*quarantineBase)
	}

	d2 := triggerBan()
	if d2 < 4*quarantineBase || d2 > 4*quarantineBase+time.Second {
		t.Fatalf("third ban duration %v, want ~%v", d2, 4*quarantineBase)
	}
}

// TestSelect_SkipsQuarantine verifies that Select skips quarantined proxies and
// returns the next available one.
func TestSelect_SkipsQuarantine(t *testing.T) {
	p1, p2 := proxy(1), proxy(2)
	ps := makeSelector("roundrobin", p1, p2)

	// Quarantine proxy 1 far into the future.
	ps.quarantine[1] = time.Now().Add(time.Hour)

	got, err := ps.Select(nil)
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	if got.ID != 2 {
		t.Fatalf("expected proxy 2, got %d", got.ID)
	}
}

// TestSelect_AllQuarantined verifies Select returns an error when all proxies
// are in quarantine.
func TestSelect_AllQuarantined(t *testing.T) {
	ps := makeSelector("roundrobin", proxy(1), proxy(2))
	ps.quarantine[1] = time.Now().Add(time.Hour)
	ps.quarantine[2] = time.Now().Add(time.Hour)

	_, err := ps.Select(nil)
	if err == nil {
		t.Fatal("expected error when all proxies quarantined")
	}
}

// TestSelect_QuarantineExpiry verifies that a proxy becomes available again once
// its quarantine deadline passes.
func TestSelect_QuarantineExpiry(t *testing.T) {
	p1 := proxy(1)
	ps := makeSelector("roundrobin", p1)

	// Set a quarantine that has already expired.
	ps.quarantine[1] = time.Now().Add(-time.Millisecond)

	got, err := ps.Select(nil)
	if err != nil {
		t.Fatalf("Select error after expiry: %v", err)
	}
	if got.ID != 1 {
		t.Fatalf("expected proxy 1 after expiry, got %d", got.ID)
	}
}

// TestProxyQuarantine_LegacyPath verifies the handler-level quarantine tracker
// used by the legacy selection path.
func TestProxyQuarantine_LegacyPath(t *testing.T) {
	q := newProxyQuarantine()

	for i := 0; i < chainFailureThreshold-1; i++ {
		q.reportFailure(42)
		if q.isQuarantined(42) {
			t.Fatalf("quarantined after %d failures", i+1)
		}
	}

	q.reportFailure(42)
	if !q.isQuarantined(42) {
		t.Fatal("not quarantined after threshold")
	}

	q.reportSuccess(42)
	if q.isQuarantined(42) {
		t.Fatal("still quarantined after success")
	}
}
