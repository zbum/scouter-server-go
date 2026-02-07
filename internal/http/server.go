package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

var startTime = time.Now()

// Server is the HTTP REST API server for Scouter monitoring data.
type Server struct {
	port                 int
	corsAllowOrigin      string
	corsAllowCredentials string
	gzipEnabled          bool
	objectCache          *cache.ObjectCache
	counterCache         *cache.CounterCache
	xlogCache            *cache.XLogCache
	textCache            *cache.TextCache
	xlogRD               *xlog.XLogRD
	counterRD            *counter.CounterRD
	alertRD              *alert.AlertRD
	httpServer           *http.Server
}

// ServerConfig holds all dependencies required to construct a Server.
type ServerConfig struct {
	Port                 int
	CorsAllowOrigin      string
	CorsAllowCredentials string
	GzipEnabled          bool
	ObjectCache          *cache.ObjectCache
	CounterCache         *cache.CounterCache
	XLogCache            *cache.XLogCache
	TextCache            *cache.TextCache
	XLogRD               *xlog.XLogRD
	CounterRD            *counter.CounterRD
	AlertRD              *alert.AlertRD
}

// NewServer creates and configures a new HTTP API server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.CorsAllowOrigin == "" {
		cfg.CorsAllowOrigin = "*"
	}
	if cfg.CorsAllowCredentials == "" {
		cfg.CorsAllowCredentials = "true"
	}

	s := &Server{
		port:                 cfg.Port,
		corsAllowOrigin:      cfg.CorsAllowOrigin,
		corsAllowCredentials: cfg.CorsAllowCredentials,
		gzipEnabled:          cfg.GzipEnabled,
		objectCache:          cfg.ObjectCache,
		counterCache:         cfg.CounterCache,
		xlogCache:            cfg.XLogCache,
		textCache:            cfg.TextCache,
		xlogRD:               cfg.XLogRD,
		counterRD:            cfg.CounterRD,
		alertRD:              cfg.AlertRD,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/objects", s.handleObjects)
	mux.HandleFunc("/api/v1/counter/realtime", s.handleCounterRealtime)
	mux.HandleFunc("/api/v1/xlog/realtime", s.handleXLogRealtime)
	mux.HandleFunc("/api/v1/text", s.handleText)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/server/info", s.handleServerInfo)

	s.httpServer = &http.Server{
		Addr:    net.JoinHostPort("", strconv.Itoa(s.port)),
		Handler: s.corsMiddleware(mux),
	}
	return s
}

// corsMiddleware adds CORS headers to every HTTP response.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.corsAllowOrigin)
		w.Header().Set("Access-Control-Allow-Credentials", s.corsAllowCredentials)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Start begins listening for HTTP connections. It blocks until the server
// is shut down or an error occurs. The provided context controls graceful shutdown.
func (s *Server) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown error", "error", err)
		}
	}()

	slog.Info("HTTP API server starting", "port", s.port)
	err := s.httpServer.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// handleHealth returns a simple health check response.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleServerInfo returns basic server information.
func (s *Server) handleServerInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	uptimeMs := time.Since(startTime).Milliseconds()
	writeJSON(w, map[string]interface{}{
		"version":   "dev",
		"uptime_ms": uptimeMs,
	})
}

// objectResponse is the JSON representation of a single monitored object.
type objectResponse struct {
	ObjHash int32  `json:"objHash"`
	ObjName string `json:"objName"`
	ObjType string `json:"objType"`
	Address string `json:"address"`
	Alive   bool   `json:"alive"`
}

// handleObjects returns all registered monitoring objects.
func (s *Server) handleObjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	allObjects := s.objectCache.GetAll()
	objects := make([]objectResponse, 0, len(allObjects))
	for _, info := range allObjects {
		p := info.Pack
		objects = append(objects, objectResponse{
			ObjHash: p.ObjHash,
			ObjName: p.ObjName,
			ObjType: p.ObjType,
			Address: p.Address,
			Alive:   p.Alive,
		})
	}

	writeJSON(w, map[string]interface{}{
		"objects": objects,
	})
}

// handleCounterRealtime returns the real-time counter value for an object.
// Query params: objHash (required), counter (required).
func (s *Server) handleCounterRealtime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	objHashStr := r.URL.Query().Get("objHash")
	counterName := r.URL.Query().Get("counter")

	if objHashStr == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: objHash")
		return
	}
	if counterName == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: counter")
		return
	}

	objHash64, err := strconv.ParseInt(objHashStr, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid objHash: must be a 32-bit integer")
		return
	}
	objHash := int32(objHash64)

	key := cache.CounterKey{
		ObjHash: objHash,
		Counter: counterName,
	}
	val, ok := s.counterCache.Get(key)
	if !ok {
		writeError(w, http.StatusNotFound, "counter not found")
		return
	}

	writeJSON(w, map[string]interface{}{
		"objHash": objHash,
		"counter": counterName,
		"value":   valueToNumber(val),
	})
}

// xlogResponse is the JSON representation of a single XLog entry.
type xlogResponse struct {
	ObjHash int32 `json:"objHash"`
	Elapsed int32 `json:"elapsed"`
	Error   bool  `json:"error"`
}

// handleXLogRealtime returns recent XLog entries from the cache.
// Query params: limit (optional, default 100).
func (s *Server) handleXLogRealtime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit: must be a positive integer")
			return
		}
		limit = parsed
	}

	entries := s.xlogCache.GetRecent(limit)
	xlogs := make([]xlogResponse, 0, len(entries))
	for _, e := range entries {
		xlogs = append(xlogs, xlogResponse{
			ObjHash: e.ObjHash,
			Elapsed: e.Elapsed,
			Error:   e.IsError,
		})
	}

	writeJSON(w, map[string]interface{}{
		"xlogs": xlogs,
		"total": len(xlogs),
	})
}

// handleText returns the text value for a given type and hash.
// Query params: type (required), hash (required).
func (s *Server) handleText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	textType := r.URL.Query().Get("type")
	hashStr := r.URL.Query().Get("hash")

	if textType == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: type")
		return
	}
	if hashStr == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: hash")
		return
	}

	hash64, err := strconv.ParseInt(hashStr, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid hash: must be a 32-bit integer")
		return
	}
	hash := int32(hash64)

	text, ok := s.textCache.Get(textType, hash)
	if !ok {
		writeError(w, http.StatusNotFound, "text not found")
		return
	}

	writeJSON(w, map[string]interface{}{
		"type": textType,
		"hash": hash,
		"text": text,
	})
}

// writeJSON encodes data as JSON and writes it to the response.
func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// writeError writes a JSON error response with the given HTTP status code.
func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// valueToNumber converts a value.Value to a numeric representation for JSON output.
func valueToNumber(v value.Value) interface{} {
	switch tv := v.(type) {
	case *value.DecimalValue:
		return tv.Value
	case *value.FloatValue:
		return tv.Value
	case *value.DoubleValue:
		return tv.Value
	case *value.TextValue:
		return tv.Value
	case *value.BooleanValue:
		return tv.Value
	default:
		return 0
	}
}
