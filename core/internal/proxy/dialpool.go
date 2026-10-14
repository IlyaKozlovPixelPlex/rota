package proxy

import (
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
)

// upstreamDialPool is the package-level warm connection pool.
// Tests replace it with newDialPoolWith to avoid reading env or spawning goroutines.
var upstreamDialPool = newDialPool()

// poolEntry is a single idle TCP connection with its creation timestamp.
type poolEntry struct {
	conn     net.Conn
	dialedAt time.Time
}

// upstreamPool holds the idle connection queue for one upstream proxy.
type upstreamPool struct {
	idle []poolEntry
}

// poolStats tracks per-upstream warming signals.
type poolStats struct {
	warmScore         int
	lastUsed          time.Time
	deadStreak        int
	warmDisabledUntil time.Time
	unusedEvicts      int
}

// dialPool is a flag-gated warm TCP connection pool for upstream HTTP proxies.
// When enabled (UPSTREAM_WARM_CONNS > 0) it pre-establishes TCP connections so
// that the CONNECT handshake can start without a dial RTT on the hot path.
//
// Self-tuning: warming only kicks in when warmScore ≥ 2 (proxy seen at least
// twice recently). In a round-robin pool of N proxies each proxy is seen at most
// once per rotation, so warming never fires for large pools — which is the desired
// behaviour (warming thousands of upstreams would be wasteful).
type dialPool struct {
	enabled    bool
	warmTarget int           // target idle conns per upstream
	maxIdle    time.Duration // evict entries older than this
	globalCap  int           // total idle conns across all upstreams

	mu        sync.RWMutex
	pools     map[string]*upstreamPool // key (transportCacheKey) → idle queue
	stats     map[string]*poolStats
	totalIdle int

	// sem limits the number of concurrent warmOne goroutines (anti-storm).
	sem chan struct{}
}

// newDialPool is the production constructor: reads UPSTREAM_WARM_CONNS from env
// and starts the background sweep goroutine when warming is enabled.
func newDialPool() *dialPool {
	n, _ := strconv.Atoi(os.Getenv("UPSTREAM_WARM_CONNS"))
	dp := newDialPoolWith(n, 10*time.Second, 512)
	if dp.enabled {
		go dp.sweepLoop()
	}
	return dp
}

// newDialPoolWith constructs a dialPool with explicit parameters.
// It does NOT start the sweep goroutine — use in tests to avoid background goroutines.
func newDialPoolWith(warm int, maxIdle time.Duration, cap int) *dialPool {
	return &dialPool{
		enabled:    warm > 0,
		warmTarget: warm,
		maxIdle:    maxIdle,
		globalCap:  cap,
		pools:      make(map[string]*upstreamPool),
		stats:      make(map[string]*poolStats),
		sem:        make(chan struct{}, 16),
	}
}

// get pops the freshest idle connection for p (LIFO).
// Stale entries (older than maxIdle) are evicted and closed.
// Returns nil when the pool is disabled or empty.
func (dp *dialPool) get(p *models.Proxy) net.Conn {
	if !dp.enabled {
		return nil
	}
	key := transportCacheKey(p)

	dp.mu.Lock()
	pool := dp.pools[key]
	if pool == nil || len(pool.idle) == 0 {
		dp.mu.Unlock()
		return nil
	}

	now := time.Now()
	st := dp.stats[key]
	var stale []net.Conn
	var result net.Conn

	for len(pool.idle) > 0 {
		last := len(pool.idle) - 1
		entry := pool.idle[last]
		pool.idle = pool.idle[:last]
		dp.totalIdle--

		if now.Sub(entry.dialedAt) > dp.maxIdle {
			stale = append(stale, entry.conn)
			if st != nil {
				st.unusedEvicts++ // counted inside the lock
			}
			continue
		}
		result = entry.conn
		break
	}
	dp.mu.Unlock()

	// Close stale conns outside the lock (syscall without holding the mutex).
	for _, c := range stale {
		c.Close()
	}
	return result
}

// reportRequest records a successful proxy request and nudges the warm score up.
// Called on every successful request through this upstream.
func (dp *dialPool) reportRequest(p *models.Proxy) {
	if !dp.enabled {
		return
	}
	key := transportCacheKey(p)
	dp.mu.Lock()
	st := dp.getOrCreateStats(key)
	if st.warmScore < 5 {
		st.warmScore++
	}
	st.lastUsed = time.Now()
	dp.mu.Unlock()
}

// reportDead records that a pooled connection failed the CONNECT handshake.
// After 3 consecutive dead-pool hits warming is suppressed for 60 s.
func (dp *dialPool) reportDead(p *models.Proxy) {
	if !dp.enabled {
		return
	}
	key := transportCacheKey(p)
	dp.mu.Lock()
	st := dp.getOrCreateStats(key)
	st.deadStreak++
	if st.deadStreak >= 3 {
		st.warmDisabledUntil = time.Now().Add(60 * time.Second)
	}
	dp.mu.Unlock()
}

// reportWarmSuccess clears the dead-streak and unused-evict counters after a
// pool conn was successfully used. Called only when fromPool && ok.
func (dp *dialPool) reportWarmSuccess(p *models.Proxy) {
	if !dp.enabled {
		return
	}
	key := transportCacheKey(p)
	dp.mu.Lock()
	if st := dp.stats[key]; st != nil {
		st.deadStreak = 0
		st.unusedEvicts = 0
	}
	dp.mu.Unlock()
}

// maybeWarm tops up the idle pool toward warmTarget when the upstream is "hot"
// (warmScore ≥ 2) and warming is not suppressed. It spawns at most `need`
// warmOne goroutines; if the semaphore is full it returns immediately (anti-storm).
func (dp *dialPool) maybeWarm(p *models.Proxy) {
	if !dp.enabled {
		return
	}
	key := transportCacheKey(p)

	dp.mu.RLock()
	st := dp.stats[key]
	pool := dp.pools[key]
	idle := 0
	if pool != nil {
		idle = len(pool.idle)
	}
	now := time.Now()
	hot := st != nil && st.warmScore >= 2 && now.After(st.warmDisabledUntil)
	need := dp.warmTarget - idle
	total := dp.totalIdle
	dp.mu.RUnlock()

	if !hot || need <= 0 || total >= dp.globalCap {
		return
	}

	for i := 0; i < need; i++ {
		select {
		case dp.sem <- struct{}{}:
			go dp.warmOne(key, p)
		default:
			return // semaphore full — anti-storm
		}
	}
}

// warmOne dials p.Address and pushes the connection into the idle pool.
// Runs as a goroutine; always releases the semaphore slot on exit.
func (dp *dialPool) warmOne(key string, p *models.Proxy) {
	defer func() { <-dp.sem }()

	conn, err := net.DialTimeout("tcp", p.Address, dialTimeout)
	if err != nil {
		return // dial failure doesn't touch deadStreak (that's for CONNECT failures)
	}

	dp.mu.Lock()
	if dp.totalIdle >= dp.globalCap {
		dp.mu.Unlock()
		conn.Close()
		return
	}
	pool := dp.pools[key]
	if pool == nil {
		pool = &upstreamPool{}
		dp.pools[key] = pool
	}
	if len(pool.idle) >= dp.warmTarget {
		dp.mu.Unlock()
		conn.Close()
		return
	}
	pool.idle = append(pool.idle, poolEntry{conn: conn, dialedAt: time.Now()})
	dp.totalIdle++
	dp.mu.Unlock()
}

// sweepLoop ticks at maxIdle/2 and calls sweep each time.
func (dp *dialPool) sweepLoop() {
	ticker := time.NewTicker(dp.maxIdle / 2)
	defer ticker.Stop()
	for range ticker.C {
		dp.sweep()
	}
}

// sweep evicts stale entries, decays warmScore for idle upstreams, disables
// warming when too many evictions happen, and GC's empty cold pools.
func (dp *dialPool) sweep() {
	now := time.Now()

	// Snapshot keys under RLock so we don't hold the lock during per-key work.
	dp.mu.RLock()
	keys := make([]string, 0, len(dp.pools))
	for k := range dp.pools {
		keys = append(keys, k)
	}
	dp.mu.RUnlock()

	for _, key := range keys {
		var stale []net.Conn

		dp.mu.Lock()
		pool := dp.pools[key]
		st := dp.stats[key]

		if pool != nil {
			fresh := pool.idle[:0]
			for _, entry := range pool.idle {
				if now.Sub(entry.dialedAt) > dp.maxIdle {
					stale = append(stale, entry.conn)
					dp.totalIdle--
					if st != nil {
						st.unusedEvicts++
					}
				} else {
					fresh = append(fresh, entry)
				}
			}
			pool.idle = fresh

			// GC empty pools that are no longer hot.
			if len(pool.idle) == 0 && (st == nil || st.warmScore == 0) {
				delete(dp.pools, key)
				delete(dp.stats, key) // prevent unbounded growth for rotated-out proxies
			}
		}

		if st != nil {
			// Decay warmScore when the upstream hasn't been used recently.
			if st.warmScore > 0 && now.Sub(st.lastUsed) > dp.maxIdle*2 {
				st.warmScore--
			}
			// Too many stale evictions → suppress warming for 60 s.
			if st.unusedEvicts >= 3 {
				st.warmDisabledUntil = now.Add(60 * time.Second)
				st.unusedEvicts = 0
			}
		}
		dp.mu.Unlock()

		// Close stale conns outside the lock.
		for _, c := range stale {
			c.Close()
		}
	}
}

// invalidate closes all idle connections for p and removes its pool entry.
// Called by InvalidateTransport and alongside quarantine eviction.
func (dp *dialPool) invalidate(p *models.Proxy) {
	if !dp.enabled {
		return
	}
	key := transportCacheKey(p)

	dp.mu.Lock()
	pool := dp.pools[key]
	delete(dp.pools, key)
	delete(dp.stats, key)
	if pool != nil {
		dp.totalIdle -= len(pool.idle)
		if dp.totalIdle < 0 {
			dp.totalIdle = 0
		}
	}
	dp.mu.Unlock()

	if pool != nil {
		for _, e := range pool.idle {
			e.conn.Close()
		}
	}
}

// invalidateAll closes every idle connection and resets the pool entirely.
// Called by ClearTransportCache.
func (dp *dialPool) invalidateAll() {
	if !dp.enabled {
		return
	}

	dp.mu.Lock()
	pools := dp.pools
	dp.pools = make(map[string]*upstreamPool)
	dp.stats = make(map[string]*poolStats)
	dp.totalIdle = 0
	dp.mu.Unlock()

	for _, pool := range pools {
		for _, e := range pool.idle {
			e.conn.Close()
		}
	}
}

// getOrCreateStats returns stats for key, creating them if absent.
// Must be called with dp.mu held for writing.
func (dp *dialPool) getOrCreateStats(key string) *poolStats {
	if st := dp.stats[key]; st != nil {
		return st
	}
	st := &poolStats{}
	dp.stats[key] = st
	return st
}

// waitForWarmers blocks until all in-flight warmOne goroutines have finished.
// Implemented by filling the semaphore to capacity (each running warmOne holds
// one slot; filling the rest of the slots forces a wait until they release theirs).
func (dp *dialPool) waitForWarmers() {
	n := cap(dp.sem)
	for i := 0; i < n; i++ {
		dp.sem <- struct{}{}
	}
	for i := 0; i < n; i++ {
		<-dp.sem
	}
}
