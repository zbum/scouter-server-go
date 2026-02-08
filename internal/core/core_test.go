package core

import (
	"net"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// --- Dispatcher tests ---

func TestDispatcher_Register_Dispatch(t *testing.T) {
	d := NewDispatcher()
	var received pack.Pack
	d.Register(pack.PackTypeXLog, func(p pack.Pack, addr *net.UDPAddr) {
		received = p
	})

	xp := &pack.XLogPack{Txid: 123}
	d.Dispatch(xp, nil)

	if received == nil {
		t.Fatal("handler not called")
	}
	if received.(*pack.XLogPack).Txid != 123 {
		t.Fatal("wrong pack dispatched")
	}
}

func TestDispatcher_NilPack(t *testing.T) {
	d := NewDispatcher()
	d.Dispatch(nil, nil) // should not panic
}

func TestDispatcher_UnregisteredType(t *testing.T) {
	d := NewDispatcher()
	xp := &pack.XLogPack{}
	d.Dispatch(xp, nil) // should not panic
}

// --- TextCore tests ---

func TestTextCore_Handler(t *testing.T) {
	tc := cache.NewTextCache()
	core := NewTextCore(tc, nil)
	handler := core.Handler()

	tp := &pack.TextPack{XType: "service", Hash: 100, Text: "/api/users"}
	handler(tp, nil)

	// Text should be cached immediately (before queue processing)
	got, ok := tc.Get("service", 100)
	if !ok || got != "/api/users" {
		t.Fatalf("expected /api/users in cache, got %q ok=%v", got, ok)
	}
}

func TestTextCore_Handler_WrongPackType(t *testing.T) {
	tc := cache.NewTextCache()
	core := NewTextCore(tc, nil)
	handler := core.Handler()

	// Passing wrong pack type should be silently ignored
	xp := &pack.XLogPack{}
	handler(xp, nil)

	if tc.Size() != 0 {
		t.Fatal("expected no cache entries for wrong pack type")
	}
}

// --- XLogCore tests ---

func TestXLogCore_Handler(t *testing.T) {
	xc := cache.NewXLogCache(100)
	core := NewXLogCore(xc, nil, nil, nil)
	handler := core.Handler()

	xp := &pack.XLogPack{
		ObjHash: 42,
		Elapsed: 150,
		Txid:    12345,
		EndTime: 1000,
	}
	handler(xp, nil)

	// Allow processing goroutine to run
	time.Sleep(50 * time.Millisecond)

	entries := xc.GetRecent(10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry in xlog cache, got %d", len(entries))
	}
	if entries[0].ObjHash != 42 || entries[0].Elapsed != 150 {
		t.Fatalf("unexpected entry: %+v", entries[0])
	}
}

func TestXLogCore_Handler_SetsEndTime(t *testing.T) {
	xc := cache.NewXLogCache(100)
	core := NewXLogCore(xc, nil, nil, nil)
	handler := core.Handler()

	before := time.Now().UnixMilli()
	xp := &pack.XLogPack{ObjHash: 1, EndTime: 0}
	handler(xp, nil)

	if xp.EndTime < before {
		t.Fatalf("expected EndTime to be set, got %d", xp.EndTime)
	}
}

func TestXLogCore_Handler_ErrorFlag(t *testing.T) {
	xc := cache.NewXLogCache(100)
	core := NewXLogCore(xc, nil, nil, nil)
	handler := core.Handler()

	xp := &pack.XLogPack{ObjHash: 1, Error: 42, EndTime: 1000}
	handler(xp, nil)
	time.Sleep(50 * time.Millisecond)

	entries := xc.GetRecent(10)
	if len(entries) != 1 || !entries[0].IsError {
		t.Fatal("expected error flag set in cached entry")
	}
}

// --- PerfCountCore tests ---

func TestPerfCountCore_Handler(t *testing.T) {
	cc := cache.NewCounterCache()
	core := NewPerfCountCore(cc, nil)
	handler := core.Handler()

	data := value.NewMapValue()
	data.Put("TPS", value.NewDecimalValue(100))
	data.Put("CPU", &value.FloatValue{Value: 45.5})

	cp := &pack.PerfCounterPack{
		ObjName:  "/test/agent",
		TimeType: cache.TimeTypeRealtime,
		Data:     data,
	}
	handler(cp, nil)

	// Allow processing goroutine to run
	time.Sleep(50 * time.Millisecond)

	objHash := util.HashString("/test/agent")
	tpsKey := cache.CounterKey{ObjHash: objHash, Counter: "TPS", TimeType: cache.TimeTypeRealtime}
	v, ok := cc.Get(tpsKey)
	if !ok {
		t.Fatal("TPS not in cache")
	}
	dv, ok := v.(*value.DecimalValue)
	if !ok || dv.Value != 100 {
		t.Fatalf("expected TPS=100, got %v", v)
	}

	cpuKey := cache.CounterKey{ObjHash: objHash, Counter: "CPU", TimeType: cache.TimeTypeRealtime}
	v, ok = cc.Get(cpuKey)
	if !ok {
		t.Fatal("CPU not in cache")
	}
	fv, ok := v.(*value.FloatValue)
	if !ok || fv.Value != 45.5 {
		t.Fatalf("expected CPU=45.5, got %v", v)
	}
}

func TestPerfCountCore_Handler_WrongPackType(t *testing.T) {
	cc := cache.NewCounterCache()
	core := NewPerfCountCore(cc, nil)
	handler := core.Handler()

	handler(&pack.XLogPack{}, nil) // should not panic
}

// --- AgentManager tests ---

func TestAgentManager_Handler(t *testing.T) {
	oc := cache.NewObjectCache()
	am := NewAgentManager(oc, 30*time.Second, nil, nil, nil, nil)
	handler := am.Handler()

	op := &pack.ObjectPack{
		ObjName: "/test/agent",
		ObjType: "java",
	}
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6100}
	handler(op, addr)

	expectedHash := util.HashString("/test/agent")
	if op.ObjHash != expectedHash {
		t.Fatalf("expected ObjHash=%d, got %d", expectedHash, op.ObjHash)
	}
	if op.Address != "10.0.0.1" {
		t.Fatalf("expected Address=10.0.0.1, got %s", op.Address)
	}
	if !op.Alive {
		t.Fatal("expected Alive=true")
	}

	info, ok := oc.Get(expectedHash)
	if !ok {
		t.Fatal("object not in cache")
	}
	if info.Pack.ObjName != "/test/agent" {
		t.Fatalf("unexpected objName: %s", info.Pack.ObjName)
	}
}

func TestAgentManager_Handler_PresetHashAndAddr(t *testing.T) {
	oc := cache.NewObjectCache()
	am := NewAgentManager(oc, 30*time.Second, nil, nil, nil, nil)
	handler := am.Handler()

	op := &pack.ObjectPack{
		ObjHash: 999,
		ObjName: "/test/agent",
		Address: "192.168.1.1",
	}
	handler(op, &net.UDPAddr{IP: net.ParseIP("10.0.0.1")})

	// Should keep existing hash and address
	if op.ObjHash != 999 {
		t.Fatalf("expected ObjHash=999, got %d", op.ObjHash)
	}
	if op.Address != "192.168.1.1" {
		t.Fatalf("expected Address=192.168.1.1, got %s", op.Address)
	}
}

func TestAgentManager_Handler_NilAddr(t *testing.T) {
	oc := cache.NewObjectCache()
	am := NewAgentManager(oc, 30*time.Second, nil, nil, nil, nil)
	handler := am.Handler()

	op := &pack.ObjectPack{ObjName: "/test"}
	handler(op, nil) // should not panic
	if op.Address != "" {
		t.Fatalf("expected empty address, got %s", op.Address)
	}
}

// --- AlertCore tests ---

func TestAlertCore_Handler(t *testing.T) {
	ac := NewAlertCore(nil, nil)
	handler := ac.Handler()

	ap := &pack.AlertPack{
		ObjHash: 42,
		ObjType: "java",
		Level:   2,
		Title:   "CPU High",
		Time:    1000,
	}
	handler(ap, nil) // should not panic

	if ap.Time != 1000 {
		t.Fatalf("Time should stay 1000, got %d", ap.Time)
	}
}

func TestAlertCore_Handler_SetsTime(t *testing.T) {
	ac := NewAlertCore(nil, nil)
	handler := ac.Handler()

	before := time.Now().UnixMilli()
	ap := &pack.AlertPack{ObjHash: 1, Time: 0}
	handler(ap, nil)

	if ap.Time < before {
		t.Fatalf("expected Time to be set, got %d", ap.Time)
	}
}

func TestAlertCore_Handler_WrongPackType(t *testing.T) {
	ac := NewAlertCore(nil, nil)
	handler := ac.Handler()
	handler(&pack.XLogPack{}, nil) // should not panic
}

// --- Integration: Dispatcher + Core wiring ---

func TestDispatcherCoreIntegration(t *testing.T) {
	// Create caches
	textCache := cache.NewTextCache()
	xlogCache := cache.NewXLogCache(100)
	counterCache := cache.NewCounterCache()
	objectCache := cache.NewObjectCache()

	// Create cores
	textCore := NewTextCore(textCache, nil)
	xlogCore := NewXLogCore(xlogCache, nil, nil, nil)
	perfCountCore := NewPerfCountCore(counterCache, nil)
	agentManager := NewAgentManager(objectCache, 30*time.Second, nil, nil, nil, nil)
	alertCore := NewAlertCore(nil, nil)

	// Wire dispatcher
	d := NewDispatcher()
	d.Register(pack.PackTypeText, textCore.Handler())
	d.Register(pack.PackTypeXLog, xlogCore.Handler())
	d.Register(pack.PackTypePerfCounter, perfCountCore.Handler())
	d.Register(pack.PackTypeObject, agentManager.Handler())
	d.Register(pack.PackTypeAlert, alertCore.Handler())

	// Dispatch a TextPack
	d.Dispatch(&pack.TextPack{XType: "service", Hash: 1, Text: "hello"}, nil)
	got, ok := textCache.Get("service", 1)
	if !ok || got != "hello" {
		t.Fatalf("text dispatch failed: %q %v", got, ok)
	}

	// Dispatch an ObjectPack
	d.Dispatch(&pack.ObjectPack{ObjName: "/app1", ObjType: "java"}, nil)
	if objectCache.Size() != 1 {
		t.Fatalf("object dispatch failed: size=%d", objectCache.Size())
	}

	// Dispatch an XLogPack
	d.Dispatch(&pack.XLogPack{ObjHash: 10, Elapsed: 50, Txid: 111, EndTime: 1000}, nil)
	time.Sleep(50 * time.Millisecond)
	entries := xlogCache.GetRecent(10)
	if len(entries) != 1 {
		t.Fatalf("xlog dispatch failed: count=%d", len(entries))
	}

	// Dispatch a PerfCounterPack
	data := value.NewMapValue()
	data.Put("TPS", value.NewDecimalValue(99))
	d.Dispatch(&pack.PerfCounterPack{ObjName: "/app1", Data: data}, nil)
	time.Sleep(50 * time.Millisecond)
	objHash := util.HashString("/app1")
	counters := counterCache.GetByObjHash(objHash)
	if len(counters) != 1 {
		t.Fatalf("counter dispatch failed: count=%d", len(counters))
	}
}
