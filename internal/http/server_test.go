package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// newTestServer creates a Server populated with fresh caches for testing.
// The xlogRD, counterRD, and alertRD fields are left nil because the
// real-time endpoints only use in-memory caches.
func newTestServer() *Server {
	return NewServer(ServerConfig{
		Port:         0,
		ObjectCache:  cache.NewObjectCache(),
		CounterCache: cache.NewCounterCache(),
		XLogCache:    cache.NewXLogCache(1000),
		TextCache:    cache.NewTextCache(),
	})
}

func TestHealthEndpoint(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}
}

func TestHealthEndpointMethodNotAllowed(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestServerInfoEndpoint(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/server/info", nil)
	w := httptest.NewRecorder()
	s.handleServerInfo(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["version"] != "dev" {
		t.Fatalf("expected version=dev, got %v", body["version"])
	}
	if _, ok := body["uptime_ms"]; !ok {
		t.Fatal("expected uptime_ms field in response")
	}
}

func TestObjectsEndpoint(t *testing.T) {
	s := newTestServer()

	// Populate cache with two objects
	s.objectCache.Put(100, &pack.ObjectPack{
		ObjHash: 100,
		ObjName: "/app/host1",
		ObjType: "java",
		Address: "192.168.1.1",
		Alive:   true,
	})
	s.objectCache.Put(200, &pack.ObjectPack{
		ObjHash: 200,
		ObjName: "/app/host2",
		ObjType: "golang",
		Address: "192.168.1.2",
		Alive:   false,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	w := httptest.NewRecorder()
	s.handleObjects(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body struct {
		Objects []objectResponse `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(body.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(body.Objects))
	}

	// Build a lookup by objHash
	byHash := make(map[int32]objectResponse)
	for _, obj := range body.Objects {
		byHash[obj.ObjHash] = obj
	}

	obj1, ok := byHash[100]
	if !ok {
		t.Fatal("expected object with hash 100")
	}
	if obj1.ObjName != "/app/host1" {
		t.Fatalf("expected objName=/app/host1, got %q", obj1.ObjName)
	}
	if obj1.ObjType != "java" {
		t.Fatalf("expected objType=java, got %q", obj1.ObjType)
	}
	if obj1.Address != "192.168.1.1" {
		t.Fatalf("expected address=192.168.1.1, got %q", obj1.Address)
	}
	if !obj1.Alive {
		t.Fatal("expected alive=true for object 100")
	}

	obj2, ok := byHash[200]
	if !ok {
		t.Fatal("expected object with hash 200")
	}
	if obj2.Alive {
		t.Fatal("expected alive=false for object 200")
	}
}

func TestObjectsEndpointEmpty(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects", nil)
	w := httptest.NewRecorder()
	s.handleObjects(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body struct {
		Objects []objectResponse `json:"objects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(body.Objects) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(body.Objects))
	}
}

func TestCounterRealtimeEndpoint(t *testing.T) {
	s := newTestServer()

	// Populate counter cache
	key := cache.CounterKey{
		ObjHash: 123,
		Counter: "TPS",
	}
	s.counterCache.Put(key, value.NewDecimalValue(42))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/counter/realtime?objHash=123&counter=TPS", nil)
	w := httptest.NewRecorder()
	s.handleCounterRealtime(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if int32(body["objHash"].(float64)) != 123 {
		t.Fatalf("expected objHash=123, got %v", body["objHash"])
	}
	if body["counter"] != "TPS" {
		t.Fatalf("expected counter=TPS, got %v", body["counter"])
	}
	if int64(body["value"].(float64)) != 42 {
		t.Fatalf("expected value=42, got %v", body["value"])
	}
}

func TestCounterRealtimeEndpointFloatValue(t *testing.T) {
	s := newTestServer()

	key := cache.CounterKey{
		ObjHash: 456,
		Counter: "CPU",
	}
	s.counterCache.Put(key, &value.FloatValue{Value: 75.5})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/counter/realtime?objHash=456&counter=CPU", nil)
	w := httptest.NewRecorder()
	s.handleCounterRealtime(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	val := float32(body["value"].(float64))
	if val < 75.0 || val > 76.0 {
		t.Fatalf("expected value near 75.5, got %v", body["value"])
	}
}

func TestCounterRealtimeMissingParams(t *testing.T) {
	s := newTestServer()

	// Missing both params
	req := httptest.NewRequest(http.MethodGet, "/api/v1/counter/realtime", nil)
	w := httptest.NewRecorder()
	s.handleCounterRealtime(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing params, got %d", w.Result().StatusCode)
	}

	// Missing counter
	req = httptest.NewRequest(http.MethodGet, "/api/v1/counter/realtime?objHash=123", nil)
	w = httptest.NewRecorder()
	s.handleCounterRealtime(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing counter, got %d", w.Result().StatusCode)
	}

	// Missing objHash
	req = httptest.NewRequest(http.MethodGet, "/api/v1/counter/realtime?counter=TPS", nil)
	w = httptest.NewRecorder()
	s.handleCounterRealtime(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing objHash, got %d", w.Result().StatusCode)
	}
}

func TestCounterRealtimeNotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/counter/realtime?objHash=999&counter=MISSING", nil)
	w := httptest.NewRecorder()
	s.handleCounterRealtime(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestXLogRealtimeEndpoint(t *testing.T) {
	s := newTestServer()

	// Populate xlog cache
	s.xlogCache.Put(100, 250, false, []byte("data1"))
	s.xlogCache.Put(200, 500, true, []byte("data2"))
	s.xlogCache.Put(300, 100, false, []byte("data3"))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/xlog/realtime?limit=10", nil)
	w := httptest.NewRecorder()
	s.handleXLogRealtime(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body struct {
		XLogs []xlogResponse `json:"xlogs"`
		Total int            `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if body.Total != 3 {
		t.Fatalf("expected total=3, got %d", body.Total)
	}
	if len(body.XLogs) != 3 {
		t.Fatalf("expected 3 xlogs, got %d", len(body.XLogs))
	}

	// Verify ordering: oldest first (ring buffer returns in insertion order)
	if body.XLogs[0].ObjHash != 100 {
		t.Fatalf("expected first xlog objHash=100, got %d", body.XLogs[0].ObjHash)
	}
	if body.XLogs[0].Elapsed != 250 {
		t.Fatalf("expected first xlog elapsed=250, got %d", body.XLogs[0].Elapsed)
	}
	if body.XLogs[0].Error != false {
		t.Fatal("expected first xlog error=false")
	}

	if body.XLogs[1].ObjHash != 200 {
		t.Fatalf("expected second xlog objHash=200, got %d", body.XLogs[1].ObjHash)
	}
	if body.XLogs[1].Error != true {
		t.Fatal("expected second xlog error=true")
	}
}

func TestXLogRealtimeDefaultLimit(t *testing.T) {
	s := newTestServer()

	// Insert 5 entries
	for i := 0; i < 5; i++ {
		s.xlogCache.Put(int32(i), int32(i*100), false, nil)
	}

	// No limit parameter - should default to 100
	req := httptest.NewRequest(http.MethodGet, "/api/v1/xlog/realtime", nil)
	w := httptest.NewRecorder()
	s.handleXLogRealtime(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body struct {
		XLogs []xlogResponse `json:"xlogs"`
		Total int            `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if body.Total != 5 {
		t.Fatalf("expected total=5, got %d", body.Total)
	}
}

func TestXLogRealtimeWithLimit(t *testing.T) {
	s := newTestServer()

	// Insert 10 entries
	for i := 0; i < 10; i++ {
		s.xlogCache.Put(int32(i), int32(i*100), false, nil)
	}

	// Request only 3
	req := httptest.NewRequest(http.MethodGet, "/api/v1/xlog/realtime?limit=3", nil)
	w := httptest.NewRecorder()
	s.handleXLogRealtime(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body struct {
		XLogs []xlogResponse `json:"xlogs"`
		Total int            `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if body.Total != 3 {
		t.Fatalf("expected total=3, got %d", body.Total)
	}
	if len(body.XLogs) != 3 {
		t.Fatalf("expected 3 xlogs, got %d", len(body.XLogs))
	}
}

func TestXLogRealtimeInvalidLimit(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/xlog/realtime?limit=abc", nil)
	w := httptest.NewRecorder()
	s.handleXLogRealtime(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid limit, got %d", w.Result().StatusCode)
	}
}

func TestTextEndpoint(t *testing.T) {
	s := newTestServer()

	// Populate text cache
	s.textCache.Put("service", 12345, "/api/users")
	s.textCache.Put("sql", 67890, "SELECT * FROM users")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/text?type=service&hash=12345", nil)
	w := httptest.NewRecorder()
	s.handleText(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if body["type"] != "service" {
		t.Fatalf("expected type=service, got %v", body["type"])
	}
	if int32(body["hash"].(float64)) != 12345 {
		t.Fatalf("expected hash=12345, got %v", body["hash"])
	}
	if body["text"] != "/api/users" {
		t.Fatalf("expected text=/api/users, got %v", body["text"])
	}
}

func TestTextEndpointNotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/text?type=service&hash=99999", nil)
	w := httptest.NewRecorder()
	s.handleText(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestTextEndpointMissingParams(t *testing.T) {
	s := newTestServer()

	// Missing both
	req := httptest.NewRequest(http.MethodGet, "/api/v1/text", nil)
	w := httptest.NewRecorder()
	s.handleText(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}

	// Missing hash
	req = httptest.NewRequest(http.MethodGet, "/api/v1/text?type=service", nil)
	w = httptest.NewRecorder()
	s.handleText(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}

	// Missing type
	req = httptest.NewRequest(http.MethodGet, "/api/v1/text?hash=12345", nil)
	w = httptest.NewRecorder()
	s.handleText(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}

func TestTextEndpointInvalidHash(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/text?type=service&hash=notanumber", nil)
	w := httptest.NewRecorder()
	s.handleText(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}
