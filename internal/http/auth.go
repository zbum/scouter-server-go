package http

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/login"
)

// httpSession represents an HTTP API session.
type httpSession struct {
	ID        string
	UserID    string
	CreatedAt time.Time
}

// HTTPSessionStore manages HTTP API sessions (cookie-based).
type HTTPSessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*httpSession
	timeout  time.Duration
}

// NewHTTPSessionStore creates a new HTTP session store.
func NewHTTPSessionStore(timeout time.Duration) *HTTPSessionStore {
	store := &HTTPSessionStore{
		sessions: make(map[string]*httpSession),
		timeout:  timeout,
	}
	// Periodically clean expired sessions
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			store.cleanup()
		}
	}()
	return store
}

func (s *HTTPSessionStore) create(userID string) string {
	b := make([]byte, 32)
	rand.Read(b)
	id := hex.EncodeToString(b)

	s.mu.Lock()
	s.sessions[id] = &httpSession{
		ID:        id,
		UserID:    userID,
		CreatedAt: time.Now(),
	}
	s.mu.Unlock()
	return id
}

func (s *HTTPSessionStore) validate(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[id]
	if !ok {
		return false
	}
	return time.Since(sess.CreatedAt) < s.timeout
}

func (s *HTTPSessionStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sess := range s.sessions {
		if time.Since(sess.CreatedAt) >= s.timeout {
			delete(s.sessions, id)
		}
	}
}

// authMiddleware applies HTTP API authentication based on config settings.
// Checks are applied in order: IP auth, bearer token auth, session auth.
// /health is always exempt from authentication.
func authMiddleware(accountManager *login.AccountManager, sessionStore *HTTPSessionStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// /health is always exempt
			if r.URL.Path == "/health" {
				next.ServeHTTP(w, r)
				return
			}

			cfg := config.Get()
			if cfg == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Check net_http_api_enabled
			if !cfg.NetHTTPApiEnabled() {
				writeError(w, http.StatusForbidden, "HTTP API is disabled")
				return
			}

			// IP-based authentication
			if cfg.NetHTTPApiAuthIpEnabled() {
				if !checkIPAuth(r, cfg.NetHTTPApiAllowIps()) {
					slog.Debug("HTTP API: IP not allowed", "ip", r.RemoteAddr)
					writeError(w, http.StatusForbidden, "IP not allowed")
					return
				}
			}

			// Bearer token authentication
			if cfg.NetHTTPApiAuthBearerTokenEnabled() {
				authHeader := r.Header.Get("Authorization")
				if strings.HasPrefix(authHeader, "Bearer ") {
					token := strings.TrimPrefix(authHeader, "Bearer ")
					// Validate bearer token against account passwords
					if accountManager != nil && validateBearerToken(accountManager, token) {
						next.ServeHTTP(w, r)
						return
					}
					writeError(w, http.StatusUnauthorized, "Invalid bearer token")
					return
				}
			}

			// Session-based authentication
			if cfg.NetHTTPApiAuthSessionEnabled() && sessionStore != nil {
				// Login endpoint is exempt from session check
				if r.URL.Path == "/api/v1/login" && r.Method == http.MethodPost {
					handleHTTPLogin(w, r, accountManager, sessionStore)
					return
				}

				// Check session cookie
				cookie, err := r.Cookie("SCOUTER_SESSION")
				if err == nil && sessionStore.validate(cookie.Value) {
					next.ServeHTTP(w, r)
					return
				}
				writeError(w, http.StatusUnauthorized, "Not authenticated")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// checkIPAuth checks if the request IP is in the allowed list.
func checkIPAuth(r *http.Request, allowIPs string) bool {
	remoteIP := extractIP(r.RemoteAddr)
	allowed := strings.Split(allowIPs, ",")
	for _, ip := range allowed {
		ip = strings.TrimSpace(ip)
		if ip == "" {
			continue
		}
		if ip == remoteIP || ip == "localhost" && (remoteIP == "127.0.0.1" || remoteIP == "::1") {
			return true
		}
	}
	return false
}

// extractIP extracts the IP from a host:port address.
func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// validateBearerToken checks if the token matches any account's password hash.
func validateBearerToken(am *login.AccountManager, token string) bool {
	accounts := am.GetAccountList()
	for _, acct := range accounts {
		if acct.Password == token {
			return true
		}
	}
	return false
}

// handleHTTPLogin handles the /api/v1/login endpoint for session-based auth.
func handleHTTPLogin(w http.ResponseWriter, r *http.Request, am *login.AccountManager, store *HTTPSessionStore) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	id := r.FormValue("id")
	pass := r.FormValue("pass")

	if am == nil || !am.AuthorizeAccount(id, pass) {
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	sessionID := store.create(id)
	http.SetCookie(w, &http.Cookie{
		Name:     "SCOUTER_SESSION",
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
	})

	writeJSON(w, map[string]string{"status": "ok", "session": sessionID})
}
