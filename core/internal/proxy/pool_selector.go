package proxy

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
)

const (
	// chainFailureThreshold is the number of consecutive failures before a proxy
	// enters quarantine (was in pool_chain.go, now owned by the selector layer).
	chainFailureThreshold = 3
	// quarantineBase is the cooldown duration for the first quarantine epoch.
	// Each subsequent ban doubles it: 30s → 60s → 120s … up to quarantineMax.
	quarantineBase = 30 * time.Second
	// quarantineMax caps the exponential backoff.
	quarantineMax = time.Hour
)

// PoolSelector selects a proxy from a specific pool using the pool's rotation strategy.
// It keeps in-memory state (round-robin index, stick counters, quarantine) per pool instance.
type PoolSelector struct {
	db     *database.DB
	poolID int
	method string // roundrobin | random | stick
	stick  int    // stick_count

	mu          sync.Mutex
	proxies     []*models.Proxy
	rrIdx       int
	stickIdx    int
	stickServed int

	// Quarantine state: proxies that exceed chainFailureThreshold consecutive
	// failures are placed in a time-based cooldown with exponential backoff.
	failCounts map[int]int       // consecutive failures since last success
	banCounts  map[int]int       // total quarantine epochs (drives backoff exponent)
	quarantine map[int]time.Time // quarantineUntil per proxy ID
}

// NewPoolSelector creates a PoolSelector for the given pool.
func NewPoolSelector(db *database.DB, pool models.ProxyPool) *PoolSelector {
	return &PoolSelector{
		db:         db,
		poolID:     pool.ID,
		method:     pool.RotationMethod,
		stick:      pool.StickCount,
		failCounts: make(map[int]int),
		banCounts:  make(map[int]int),
		quarantine: make(map[int]time.Time),
	}
}

// Refresh reloads only active/idle proxies that belong to this pool.
func (ps *PoolSelector) Refresh(ctx context.Context) error {
	rows, err := ps.db.Pool.Query(ctx, `
		SELECT p.id, p.address, p.protocol, p.username, p.password,
		       p.status, p.requests, p.successful_requests, p.failed_requests,
		       p.avg_response_time, p.last_check, p.last_error, p.created_at, p.updated_at
		FROM proxies p
		JOIN pool_proxies pp ON pp.proxy_id = p.id
		WHERE pp.pool_id = $1
		  AND p.status IN ('active', 'idle')
		ORDER BY p.id
	`, ps.poolID)
	if err != nil {
		return fmt.Errorf("pool selector refresh: %w", err)
	}
	defer rows.Close()

	var proxies []*models.Proxy
	for rows.Next() {
		var p models.Proxy
		err := rows.Scan(
			&p.ID, &p.Address, &p.Protocol, &p.Username, &p.Password,
			&p.Status, &p.Requests, &p.SuccessfulRequests, &p.FailedRequests,
			&p.AvgResponseTime, &p.LastCheck, &p.LastError, &p.CreatedAt, &p.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("pool selector scan: %w", err)
		}
		proxies = append(proxies, &p)
	}

	ps.mu.Lock()
	ps.proxies = proxies
	// fix out-of-bounds indices after refresh
	if ps.rrIdx >= len(proxies) {
		ps.rrIdx = 0
	}
	if ps.stickIdx >= len(proxies) {
		ps.stickIdx = 0
		ps.stickServed = 0
	}
	// prune quarantine state for proxies no longer in the pool
	live := make(map[int]struct{}, len(proxies))
	for _, p := range proxies {
		live[p.ID] = struct{}{}
	}
	for id := range ps.quarantine {
		if _, ok := live[id]; !ok {
			delete(ps.quarantine, id)
			delete(ps.failCounts, id)
			delete(ps.banCounts, id)
		}
	}
	ps.mu.Unlock()
	return nil
}

// HasActive returns true if the pool currently has at least one active/idle proxy.
func (ps *PoolSelector) HasActive() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.proxies) > 0
}

// Select picks the next proxy according to the pool's rotation method,
// skipping any proxy whose quarantine period has not yet expired.
func (ps *PoolSelector) Select(_ context.Context) (*models.Proxy, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if len(ps.proxies) == 0 {
		return nil, fmt.Errorf("pool %d has no active proxies", ps.poolID)
	}

	now := time.Now()
	n := len(ps.proxies)

	switch ps.method {
	case "random":
		// Build a list of non-quarantined proxies and pick at random.
		available := make([]*models.Proxy, 0, n)
		for _, p := range ps.proxies {
			if until, ok := ps.quarantine[p.ID]; !ok || now.After(until) {
				available = append(available, p)
			}
		}
		if len(available) == 0 {
			return nil, fmt.Errorf("pool %d: all %d proxies quarantined", ps.poolID, n)
		}
		return available[rand.IntN(len(available))], nil

	case "stick":
		if ps.stick <= 0 {
			ps.stick = 10
		}
		start := ps.stickIdx
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			p := ps.proxies[idx]
			if until, ok := ps.quarantine[p.ID]; ok && now.Before(until) {
				continue
			}
			// If we jumped past the intended stickIdx, reset the served count.
			if idx != ps.stickIdx {
				ps.stickIdx = idx
				ps.stickServed = 0
			}
			ps.stickServed++
			if ps.stickServed >= ps.stick {
				ps.stickIdx = (idx + 1) % n
				ps.stickServed = 0
			}
			return p, nil
		}
		return nil, fmt.Errorf("pool %d: all %d proxies quarantined", ps.poolID, n)

	default: // roundrobin
		start := ps.rrIdx
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			p := ps.proxies[idx]
			if until, ok := ps.quarantine[p.ID]; ok && now.Before(until) {
				continue
			}
			ps.rrIdx = (idx + 1) % n
			return p, nil
		}
		return nil, fmt.Errorf("pool %d: all %d proxies quarantined", ps.poolID, n)
	}
}

// ReportFailure records a connection failure for the given proxy.
// After chainFailureThreshold consecutive failures the proxy enters quarantine
// with an exponential backoff: 30s → 60s → 120s … capped at quarantineMax.
// The proxy remains in ps.proxies so it recovers automatically once the cooldown expires.
func (ps *PoolSelector) ReportFailure(proxyID int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.failCounts[proxyID]++
	if ps.failCounts[proxyID] < chainFailureThreshold {
		return
	}
	ps.failCounts[proxyID] = 0
	bans := ps.banCounts[proxyID]
	ps.banCounts[proxyID]++
	backoff := quarantineBase << uint(bans)
	if backoff > quarantineMax {
		backoff = quarantineMax
	}
	ps.quarantine[proxyID] = time.Now().Add(backoff)
}

// ReportSuccess clears failure state and any active quarantine for the proxy.
// banCounts is also reset so a recovered proxy starts fresh on the next failure.
func (ps *PoolSelector) ReportSuccess(proxyID int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.failCounts, proxyID)
	delete(ps.quarantine, proxyID)
	delete(ps.banCounts, proxyID)
}
