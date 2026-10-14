package proxy

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

// mockProxy starts a TCP listener that responds to every HTTP CONNECT with 200.
// Returns the address and a cleanup function.
func mockProxy(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				r.ReadString('\n') // CONNECT line
				for {
					hdr, _ := r.ReadString('\n')
					if strings.TrimSpace(hdr) == "" {
						break
					}
				}
				c.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
				io.Copy(io.Discard, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// proxyFor builds a minimal *models.Proxy pointing at addr.
func proxyFor(addr string) *models.Proxy {
	return &models.Proxy{Address: addr, Protocol: "http"}
}

// fillPool injects n pipe connections into dp's idle list for p, bypassing the
// warmer so tests don't need a running upstream.
func fillPool(t *testing.T, dp *dialPool, p *models.Proxy, n int) (partners []net.Conn) {
	t.Helper()
	key := transportCacheKey(p)
	dp.mu.Lock()
	if dp.pools[key] == nil {
		dp.pools[key] = &upstreamPool{}
	}
	for i := 0; i < n; i++ {
		c, partner := net.Pipe()
		dp.pools[key].idle = append(dp.pools[key].idle, poolEntry{conn: c, dialedAt: time.Now()})
		dp.totalIdle++
		partners = append(partners, partner)
	}
	dp.mu.Unlock()
	return partners
}

// Test 1 — disabled pool: get returns nil, all signal methods are no-ops.
func TestDialPool_Disabled(t *testing.T) {
	dp := newDialPoolWith(0, 10*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")

	if got := dp.get(p); got != nil {
		got.Close()
		t.Fatal("disabled pool: get() should return nil")
	}
	// These must not panic or allocate any state.
	dp.reportRequest(p)
	dp.reportDead(p)
	dp.reportWarmSuccess(p)
	dp.maybeWarm(p)

	dp.mu.RLock()
	pools := len(dp.pools)
	dp.mu.RUnlock()
	if pools != 0 {
		t.Fatalf("disabled pool: unexpected pool entries: %d", pools)
	}
}

// Test 1b — disabled pool: connectViaHTTPStandalone behaviour is identical to
// the pre-pool baseline (no extra dial, no error).
func TestDialPool_Disabled_ConnectIdentical(t *testing.T) {
	addr, cleanup := mockProxy(t)
	defer cleanup()

	old := upstreamDialPool
	defer func() { upstreamDialPool = old }()
	upstreamDialPool = newDialPoolWith(0, 10*time.Second, 512)

	conn, err := connectViaHTTPStandalone(proxyFor(addr), "example.com:443")
	if err != nil {
		t.Fatalf("disabled pool path: %v", err)
	}
	conn.Close()
}

// Test 2 — warm hit: get() returns the pre-filled conn.
func TestDialPool_Get_WarmHit(t *testing.T) {
	dp := newDialPoolWith(2, 10*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")

	partners := fillPool(t, dp, p, 1)
	defer partners[0].Close()

	key := transportCacheKey(p)
	var original net.Conn
	dp.mu.Lock()
	original = dp.pools[key].idle[0].conn
	dp.mu.Unlock()

	got := dp.get(p)
	if got == nil {
		t.Fatal("expected warm conn, got nil")
	}
	if got != original {
		got.Close()
		t.Fatal("get() returned a different conn than what was in the pool")
	}
	got.Close()

	dp.mu.Lock()
	remaining := len(dp.pools[key].idle)
	dp.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("idle len = %d after get, want 0", remaining)
	}
	if dp.totalIdle != 0 {
		t.Fatalf("totalIdle = %d after get, want 0", dp.totalIdle)
	}
}

// Test 3 — stale eviction: entries older than maxIdle are closed and skipped.
func TestDialPool_Get_StaleEviction(t *testing.T) {
	dp := newDialPoolWith(2, 5*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")
	key := transportCacheKey(p)

	c, partner := net.Pipe()
	defer partner.Close()

	dp.mu.Lock()
	dp.pools[key] = &upstreamPool{
		idle: []poolEntry{{conn: c, dialedAt: time.Now().Add(-10 * time.Second)}},
	}
	dp.stats[key] = &poolStats{}
	dp.totalIdle = 1
	dp.mu.Unlock()

	got := dp.get(p)
	if got != nil {
		got.Close()
		t.Fatal("expected nil for stale conn")
	}

	// The stale conn should be closed; partner gets EOF.
	partner.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1)
	_, err := partner.Read(buf)
	if err == nil {
		t.Fatal("expected partner to see EOF/error after stale conn closed")
	}

	dp.mu.RLock()
	evicts := dp.stats[key].unusedEvicts
	dp.mu.RUnlock()
	if evicts != 1 {
		t.Fatalf("unusedEvicts = %d, want 1", evicts)
	}
	if dp.totalIdle != 0 {
		t.Fatalf("totalIdle = %d after stale evict, want 0", dp.totalIdle)
	}
}

// Test 4 — dead-conn fallback: a closed pool conn causes a fresh dial and
// reportDead is called.
func TestDialPool_DeadConn_Fallback(t *testing.T) {
	addr, cleanup := mockProxy(t)
	defer cleanup()

	p := proxyFor(addr)
	key := transportCacheKey(p)

	dead, deadPartner := net.Pipe()
	deadPartner.Close()
	dead.Close()

	dp := newDialPoolWith(1, 10*time.Second, 512)
	dp.mu.Lock()
	dp.pools[key] = &upstreamPool{
		idle: []poolEntry{{conn: dead, dialedAt: time.Now()}},
	}
	dp.stats[key] = &poolStats{warmScore: 3}
	dp.totalIdle = 1
	dp.mu.Unlock()

	old := upstreamDialPool
	defer func() { upstreamDialPool = old }()
	upstreamDialPool = dp

	conn, err := connectViaHTTPStandalone(p, "example.com:443")
	if err != nil {
		t.Fatalf("fallback dial should succeed, got: %v", err)
	}
	conn.Close()

	dp.mu.RLock()
	streak := dp.stats[key].deadStreak
	dp.mu.RUnlock()
	if streak != 1 {
		t.Fatalf("deadStreak = %d after dead-conn fallback, want 1", streak)
	}
}

// Test 5 — teardown: invalidate closes all idle conns and removes the pool entry.
func TestDialPool_Invalidate(t *testing.T) {
	dp := newDialPoolWith(3, 10*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")

	partners := fillPool(t, dp, p, 3)

	dp.invalidate(p)

	if dp.totalIdle != 0 {
		t.Fatalf("totalIdle = %d after invalidate, want 0", dp.totalIdle)
	}
	dp.mu.RLock()
	_, exists := dp.pools[transportCacheKey(p)]
	dp.mu.RUnlock()
	if exists {
		t.Fatal("pool entry still present after invalidate")
	}

	// All partner ends should see EOF (our side was closed).
	for i, partner := range partners {
		partner.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 1)
		_, err := partner.Read(buf)
		if err == nil {
			partner.Close()
			t.Fatalf("partner %d: expected EOF after invalidate", i)
		}
		partner.Close()
	}
}

// Test 6a — self-tuning: warmScore < 2 → maybeWarm does not dial.
func TestDialPool_SelfTuning_LowScore_NoWarm(t *testing.T) {
	dp := newDialPoolWith(2, 10*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")
	key := transportCacheKey(p)

	// warmScore = 1 (below threshold)
	dp.mu.Lock()
	dp.stats[key] = &poolStats{warmScore: 1, lastUsed: time.Now()}
	dp.mu.Unlock()

	dp.maybeWarm(p)
	dp.waitForWarmers()

	dp.mu.RLock()
	pool := dp.pools[key]
	dp.mu.RUnlock()
	if pool != nil && len(pool.idle) > 0 {
		t.Fatalf("expected no warm conns with warmScore<2, got %d", len(pool.idle))
	}
}

// Test 6b — self-tuning: after 2× reportRequest warmScore reaches 2 and
// maybeWarm dials up to warmTarget.
func TestDialPool_SelfTuning_Warms_AfterTwoRequests(t *testing.T) {
	addr, cleanup := mockProxy(t)
	defer cleanup()

	dp := newDialPoolWith(2, 10*time.Second, 512)
	p := proxyFor(addr)
	key := transportCacheKey(p)

	dp.reportRequest(p) // warmScore = 1
	dp.reportRequest(p) // warmScore = 2
	dp.maybeWarm(p)
	dp.waitForWarmers()

	dp.mu.RLock()
	idle := 0
	if pool := dp.pools[key]; pool != nil {
		idle = len(pool.idle)
	}
	dp.mu.RUnlock()

	if idle != 2 {
		t.Fatalf("expected %d warm conns after warming, got %d", 2, idle)
	}

	// Drain to avoid leaking conns.
	dp.invalidate(p)
}

// Test 6c — self-tuning: deadStreak ≥ 3 → warmDisabledUntil is set in the future.
func TestDialPool_SelfTuning_DeadStreak_Disables(t *testing.T) {
	dp := newDialPoolWith(2, 10*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")
	key := transportCacheKey(p)

	for i := 0; i < 3; i++ {
		dp.reportDead(p)
	}

	dp.mu.RLock()
	st := dp.stats[key]
	var until time.Time
	if st != nil {
		until = st.warmDisabledUntil
	}
	dp.mu.RUnlock()

	if !until.After(time.Now()) {
		t.Fatalf("warmDisabledUntil = %v, expected a future time after 3 reportDead calls", until)
	}
}

// Test 7 — no FD leak: conns injected into the pool are all closed by invalidateAll.
func TestDialPool_NoFDLeak_InvalidateAll(t *testing.T) {
	dp := newDialPoolWith(2, 10*time.Second, 512)

	const upstreams = 4
	const connsEach = 2

	var closed atomic.Int64
	var allPartners []net.Conn

	for u := 0; u < upstreams; u++ {
		p := &models.Proxy{
			Address:  "10.0.0." + string(rune('1'+u)) + ":3128",
			Protocol: "http",
		}
		key := transportCacheKey(p)
		dp.mu.Lock()
		if dp.pools[key] == nil {
			dp.pools[key] = &upstreamPool{}
		}
		for i := 0; i < connsEach; i++ {
			c, partner := net.Pipe()
			closed2 := &closed
			tracked := &trackingConn{Conn: c, onClose: func() { closed2.Add(1) }}
			dp.pools[key].idle = append(dp.pools[key].idle, poolEntry{conn: tracked, dialedAt: time.Now()})
			dp.totalIdle++
			allPartners = append(allPartners, partner)
		}
		dp.mu.Unlock()
	}

	total := int64(upstreams * connsEach)
	dp.invalidateAll()

	if got := closed.Load(); got != total {
		t.Fatalf("closed = %d, want %d (FD leak)", got, total)
	}
	if dp.totalIdle != 0 {
		t.Fatalf("totalIdle = %d after invalidateAll, want 0", dp.totalIdle)
	}

	for _, p := range allPartners {
		p.Close()
	}
}

// Test 8 — stats GC: sweep removes stats entry when a pool cools down entirely,
// preventing unbounded map growth on long-running servers with many rotated proxies.
func TestDialPool_Sweep_GCsStatsOnCooldown(t *testing.T) {
	dp := newDialPoolWith(2, 5*time.Second, 512)
	p := proxyFor("1.2.3.4:3128")
	key := transportCacheKey(p)

	// Inject a stale idle conn so sweep has something to evict.
	c, partner := net.Pipe()
	defer partner.Close()

	dp.mu.Lock()
	dp.pools[key] = &upstreamPool{
		idle: []poolEntry{{conn: c, dialedAt: time.Now().Add(-10 * time.Second)}},
	}
	// warmScore = 0 — proxy has cooled; sweep should GC both pool and stats.
	dp.stats[key] = &poolStats{warmScore: 0}
	dp.totalIdle = 1
	dp.mu.Unlock()

	dp.sweep()

	dp.mu.RLock()
	_, poolExists := dp.pools[key]
	_, statsExists := dp.stats[key]
	dp.mu.RUnlock()

	if poolExists {
		t.Fatal("sweep should have removed the empty cold pool")
	}
	if statsExists {
		t.Fatal("sweep should have removed stats for the cold pool (F1: stats leak)")
	}
}

// trackingConn calls onClose when Close is called.
type trackingConn struct {
	net.Conn
	onClose func()
	once    atomic.Bool
}

func (tc *trackingConn) Close() error {
	if tc.once.CompareAndSwap(false, true) {
		tc.onClose()
	}
	return tc.Conn.Close()
}
