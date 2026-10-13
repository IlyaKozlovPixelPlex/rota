package proxy

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alpkeskin/rota/core/internal/database"
	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

// userChainKey is the context key that carries the resolved *PoolChain.
type userChainKey struct{}

// UserChainContextKey is exported for use in the handler.
var UserChainContextKey = userChainKey{}

// userNameKey is the context key that carries the authenticated proxy username.
type userNameKey struct{}

// UserNameContextKey is exported for use in the handler (e.g. Prometheus labels).
var UserNameContextKey = userNameKey{}

// userEntry caches a resolved PoolChain and a fast password verifier for a user.
type userEntry struct {
	chain     *PoolChain
	expiresAt time.Time
	// verifiedHMAC is HMAC-SHA256(plaintext_password, per-process secret) stored on
	// a successful slow-path auth. Fast-path checks compare the incoming password's
	// HMAC against this value via constant-time comparison — bcrypt never runs on hits.
	verifiedHMAC string
}

// UserAuthMiddleware resolves Proxy-Authorization credentials against proxy_users.
// When a matching enabled user is found it attaches a *PoolChain to the request context.
// If user-based auth is not configured (no proxy_users) and the legacy single-user
// auth is enabled, it falls through to the original AuthMiddleware behaviour.
type UserAuthMiddleware struct {
	userRepo    *repository.UserRepository
	poolRepo    *repository.PoolRepository
	db          *database.DB
	logger      *logger.Logger
	rotSettings *models.RotationSettings

	// legacy fallback (original single-user auth)
	legacy *AuthMiddleware

	// secret is a random per-process key used to compute HMAC verifiers in cache entries.
	// It is generated once at startup; restarting the process invalidates all cached HMACs.
	secret []byte

	// cache: username -> userEntry (TTL 60s)
	mu    sync.RWMutex
	cache map[string]userEntry

	// usersConfigured caches whether any proxy_users exist (TTL 30s). Used by the
	// no-credentials path to decide whether an unauthenticated request may pass.
	usersConfigured   bool
	usersCheckedUntil time.Time
}

// NewUserAuthMiddleware creates the middleware.
func NewUserAuthMiddleware(
	userRepo *repository.UserRepository,
	poolRepo *repository.PoolRepository,
	db *database.DB,
	legacy *AuthMiddleware,
	rotSettings *models.RotationSettings,
	log *logger.Logger,
) *UserAuthMiddleware {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic("rota: failed to generate HMAC secret: " + err.Error())
	}
	m := &UserAuthMiddleware{
		userRepo:    userRepo,
		poolRepo:    poolRepo,
		db:          db,
		legacy:      legacy,
		rotSettings: rotSettings,
		logger:      log,
		secret:      secret,
		cache:       make(map[string]userEntry),
	}
	// background goroutine: refresh all cached chains every 30s
	go m.refreshLoop()
	return m
}

// HandleRequest is called for every HTTP proxy request.
// It reads Proxy-Authorization, looks up the user, builds a PoolChain and stores
// it in the request context so the handler can use it.
func (m *UserAuthMiddleware) HandleRequest(req *http.Request) (*http.Request, *http.Response) {
	username, password, ok := parseProxyAuth(req)
	if !ok {
		// No credentials provided. Delegate to legacy single-user auth if enabled.
		if m.legacy != nil && m.legacy.IsEnabled() {
			if _, resp := m.legacy.HandleRequest(req); resp != nil {
				// Legacy is enabled and demands authentication → 407.
				return req, resp
			}
		}
		// Legacy is disabled (or absent). If proxy_users are configured, an
		// unauthenticated request must NOT be allowed through — otherwise this
		// would be an auth bypass (AUD-1). Only a fully-unconfigured proxy (no
		// legacy auth, no proxy_users) stays open.
		if m.hasProxyUsers(req.Context()) {
			return req, unauthorized()
		}
		return req, nil
	}

	chain, err := m.resolve(req.Context(), username, password)
	if err != nil {
		m.logger.Warn("user auth failed", "username", username, "err", err)
		// Credentials were supplied but didn't match a proxy_user. Fall back to
		// legacy single-user auth ONLY when it is actually enforcing (AUD-1):
		// if legacy is disabled we must reject, never allow through.
		if m.legacy != nil && m.legacy.IsEnabled() {
			// Legacy enabled: it authoritatively accepts (nil) or rejects (407).
			if _, resp := m.legacy.HandleRequest(req); resp != nil {
				return req, resp
			}
			// legacy auth passed with these same credentials → allow without pool chain
			return req, nil
		}
		// No valid auth path for these credentials → reject.
		return req, unauthorized()
	}

	// Attach chain and username to context, then strip the auth header.
	newCtx := context.WithValue(req.Context(), UserChainContextKey, chain)
	newCtx = context.WithValue(newCtx, UserNameContextKey, username)
	req = req.WithContext(newCtx)
	req.Header.Del("Proxy-Authorization")
	return req, nil
}

// HandleConnect is the same but for HTTPS CONNECT.
func (m *UserAuthMiddleware) HandleConnect(req *http.Request) (*http.Request, *http.Response) {
	return m.HandleRequest(req)
}

// computeHMAC returns hex(HMAC-SHA256(password, m.secret)).
func (m *UserAuthMiddleware) computeHMAC(password string) string {
	mac := hmac.New(sha256.New, m.secret)
	mac.Write([]byte(password))
	return hex.EncodeToString(mac.Sum(nil))
}

// resolve authenticates the user and returns a warm PoolChain.
// bcrypt runs only on a cache miss or after the 60-second TTL expires.
// On cache hits the incoming password is verified by comparing its HMAC-SHA256
// against the HMAC stored at slow-path time — constant-time, no bcrypt cost.
func (m *UserAuthMiddleware) resolve(ctx context.Context, username, password string) (*PoolChain, error) {
	now := time.Now()

	// ── Fast path: cache hit within TTL ──────────────────────────────────
	m.mu.RLock()
	entry, hit := m.cache[username]
	m.mu.RUnlock()

	if hit && now.Before(entry.expiresAt) {
		incoming := m.computeHMAC(password)
		if subtle.ConstantTimeCompare([]byte(incoming), []byte(entry.verifiedHMAC)) != 1 {
			return nil, fmt.Errorf("invalid credentials")
		}
		return entry.chain, nil
	}

	// ── Slow path: full DB lookup + bcrypt (runs at most once per 60s per user) ──
	user, err := m.userRepo.Authenticate(ctx, username, password)
	if err != nil {
		return nil, err
	}

	chain, err := m.buildChain(ctx, user)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.cache[username] = userEntry{
		chain:        chain,
		expiresAt:    now.Add(60 * time.Second),
		verifiedHMAC: m.computeHMAC(password),
	}
	m.mu.Unlock()

	return chain, nil
}

// buildChain constructs an ordered PoolChain for a user: [mainPool, ...fallbackPools].
func (m *UserAuthMiddleware) buildChain(ctx context.Context, user *models.ProxyUser) (*PoolChain, error) {
	var pools []models.ProxyPool

	// Main pool
	if user.MainPoolID != nil {
		p, err := m.poolRepo.GetByID(ctx, *user.MainPoolID)
		if err != nil {
			return nil, err
		}
		if p != nil {
			pools = append(pools, *p)
		}
	}

	// Fallback pools in order
	for _, fbID := range user.FallbackPoolIDs {
		p, err := m.poolRepo.GetByID(ctx, fbID)
		if err != nil || p == nil {
			continue
		}
		pools = append(pools, *p)
	}

	maxRetry := user.MaxRetries
	if maxRetry <= 0 {
		maxRetry = 5
	}

	chain := NewPoolChain(m.db, pools, maxRetry, m.logger)
	chain.Refresh(ctx)
	return chain, nil
}

// hasProxyUsers reports whether any proxy_users are configured, with a 30s TTL
// cache so the no-credentials hot path doesn't hit the DB on every request.
func (m *UserAuthMiddleware) hasProxyUsers(ctx context.Context) bool {
	m.mu.RLock()
	fresh := time.Now().Before(m.usersCheckedUntil)
	cached := m.usersConfigured
	m.mu.RUnlock()
	if fresh {
		return cached
	}

	users, err := m.userRepo.List(ctx)
	if err != nil {
		// On error keep the last known value (fail toward the previous decision).
		return cached
	}
	has := len(users) > 0

	m.mu.Lock()
	m.usersConfigured = has
	m.usersCheckedUntil = time.Now().Add(30 * time.Second)
	m.mu.Unlock()
	return has
}

// refreshLoop periodically refreshes cached chains that are still live and evicts
// entries whose TTL has expired so the cache can't grow unbounded (AUD-19).
func (m *UserAuthMiddleware) refreshLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		now := time.Now()

		// Snapshot live (non-expired) entries; collect expired usernames for eviction.
		m.mu.RLock()
		live := make([]userEntry, 0, len(m.cache))
		var expired []string
		for k, v := range m.cache {
			if now.After(v.expiresAt) {
				expired = append(expired, k)
				continue
			}
			live = append(live, v)
		}
		m.mu.RUnlock()

		// Evict expired entries.
		if len(expired) > 0 {
			m.mu.Lock()
			for _, k := range expired {
				// Re-check under the write lock in case it was refreshed since the snapshot.
				if e, ok := m.cache[k]; ok && now.After(e.expiresAt) {
					delete(m.cache, k)
				}
			}
			m.mu.Unlock()
		}

		// Refresh chains still in use so new proxies become available.
		for _, entry := range live {
			entry.chain.Refresh(ctx)
		}
		cancel()
	}
}

// InvalidateUser removes a user's cached chain (call after user is updated/deleted).
func (m *UserAuthMiddleware) InvalidateUser(username string) {
	m.mu.Lock()
	delete(m.cache, username)
	m.mu.Unlock()
}

// parseProxyAuth extracts username+password from the Proxy-Authorization header.
func parseProxyAuth(req *http.Request) (string, string, bool) {
	auth := req.Header.Get("Proxy-Authorization")
	if auth == "" {
		return "", "", false
	}
	if !strings.HasPrefix(auth, "Basic ") {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
	if err != nil {
		return "", "", false
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// unauthorized builds a 407 response (standalone, no receiver needed).
func unauthorized() *http.Response {
	resp := &http.Response{
		StatusCode: http.StatusProxyAuthRequired,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
	}
	resp.Header.Set("Proxy-Authenticate", `Basic realm="Rota Proxy"`)
	return resp
}
