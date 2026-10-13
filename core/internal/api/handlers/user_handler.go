package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/repository"
	"github.com/alpkeskin/rota/core/pkg/logger"
	"github.com/go-chi/chi/v5"
)

// UserHandler handles proxy user management endpoints
type UserHandler struct {
	userRepo *repository.UserRepository
	poolRepo *repository.PoolRepository
	logger   *logger.Logger
}

// NewUserHandler creates a new UserHandler
func NewUserHandler(
	userRepo *repository.UserRepository,
	poolRepo *repository.PoolRepository,
	log *logger.Logger,
) *UserHandler {
	return &UserHandler{userRepo: userRepo, poolRepo: poolRepo, logger: log}
}

// List returns all proxy users
func (h *UserHandler) List(w http.ResponseWriter, r *http.Request) {
	users, err := h.userRepo.List(r.Context())
	if err != nil {
		h.logger.Error("list users failed", "error", err)
		http.Error(w, `{"error":"failed to list users"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": users})
}

// Get returns a single user (no password)
func (h *UserHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	u, err := h.userRepo.GetByID(r.Context(), id)
	if err != nil || u == nil {
		http.Error(w, `{"error":"user not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, u)
}

// Create adds a new proxy user
func (h *UserHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req models.CreateProxyUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Username == "" || req.Password == "" {
		http.Error(w, `{"error":"username and password are required"}`, http.StatusBadRequest)
		return
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = 5
	}

	u, err := h.userRepo.Create(r.Context(), req)
	if err != nil {
		h.logger.Error("create user failed", "error", err)
		http.Error(w, `{"error":"failed to create user: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, u)
}

// Update modifies an existing user
func (h *UserHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	var req models.UpdateProxyUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	u, err := h.userRepo.Update(r.Context(), id, req)
	if err != nil || u == nil {
		http.Error(w, `{"error":"user not found or update failed"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, u)
}

// Delete removes a user
func (h *UserHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	if err := h.userRepo.Delete(r.Context(), id); err != nil {
		http.Error(w, `{"error":"failed to delete user"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ExportWorkingProxies exports working proxies for a specific pool using user credentials supplied via URL query.
// GET /api/v1/proxy-users/export-working-proxies?username=...&password=...&pool=...&count=...
func (h *UserHandler) ExportWorkingProxies(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		username = r.URL.Query().Get("user")
	}
	password := r.URL.Query().Get("password")
	if password == "" {
		password = r.URL.Query().Get("pass")
	}
	if username == "" || password == "" {
		http.Error(w, `{"error":"username and password query parameters are required"}`, http.StatusUnauthorized)
		return
	}

	user, err := h.userRepo.Authenticate(r.Context(), username, password)
	if err != nil || user == nil {
		http.Error(w, `{"error":"invalid user credentials"}`, http.StatusUnauthorized)
		return
	}

	if !user.AllowWorkingProxiesExport {
		http.Error(w, `{"error":"working proxies export is disabled for this user"}`, http.StatusForbidden)
		return
	}

	poolParam := r.URL.Query().Get("pool")
	if poolParam == "" {
		poolParam = r.URL.Query().Get("pool_id")
	}

	var poolID int
	if poolParam != "" {
		if id, err := strconv.Atoi(poolParam); err == nil && id > 0 {
			poolID = id
		} else {
			p, err := h.poolRepo.GetByName(r.Context(), poolParam)
			if err != nil || p == nil {
				http.Error(w, `{"error":"pool not found by name"}`, http.StatusNotFound)
				return
			}
			poolID = p.ID
		}
	} else {
		if user.MainPoolID == nil || *user.MainPoolID <= 0 {
			http.Error(w, `{"error":"no pool specified and user has no main pool assigned"}`, http.StatusBadRequest)
			return
		}
		poolID = *user.MainPoolID
	}

	countParam := r.URL.Query().Get("count")
	if countParam == "" {
		countParam = r.URL.Query().Get("limit")
	}
	limit := 0
	if countParam != "" {
		if l, err := strconv.Atoi(countParam); err == nil && l > 0 {
			limit = l
		}
	}

	statusFilter := r.URL.Query().Get("status")
	if statusFilter == "" {
		statusFilter = "active"
	}

	proxies, err := h.poolRepo.GetWorkingProxies(r.Context(), poolID, limit, statusFilter)
	if err != nil {
		h.logger.Error("failed to get working pool proxies", "error", err, "pool_id", poolID)
		http.Error(w, `{"error":"failed to fetch working pool proxies"}`, http.StatusInternalServerError)
		return
	}

	formatParam := r.URL.Query().Get("format")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	for _, p := range proxies {
		var line string
		if formatParam == "url" {
			if p.Username != nil && *p.Username != "" && p.Password != nil && *p.Password != "" {
				line = fmt.Sprintf("%s://%s:%s@%s", p.Protocol, *p.Username, *p.Password, p.Address)
			} else if p.Username != nil && *p.Username != "" {
				line = fmt.Sprintf("%s://%s@%s", p.Protocol, *p.Username, p.Address)
			} else {
				line = fmt.Sprintf("%s://%s", p.Protocol, p.Address)
			}
		} else {
			line = p.Address
			if p.Username != nil && *p.Username != "" {
				if p.Password != nil && *p.Password != "" {
					line = fmt.Sprintf("%s:%s:%s", line, *p.Username, *p.Password)
				} else {
					line = fmt.Sprintf("%s:%s", line, *p.Username)
				}
			}
		}
		fmt.Fprintln(w, line)
	}
}
