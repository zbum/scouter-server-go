package cache

import (
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// --- TextCache tests ---

func TestTextCache_PutGet(t *testing.T) {
	c := NewTextCache()
	c.Put("service", 100, "/api/users")
	c.Put("sql", 200, "SELECT * FROM t")

	got, ok := c.Get("service", 100)
	if !ok || got != "/api/users" {
		t.Fatalf("expected /api/users, got %q ok=%v", got, ok)
	}
	got, ok = c.Get("sql", 200)
	if !ok || got != "SELECT * FROM t" {
		t.Fatalf("expected SELECT * FROM t, got %q ok=%v", got, ok)
	}
}

func TestTextCache_Miss(t *testing.T) {
	c := NewTextCache()
	_, ok := c.Get("service", 999)
	if ok {
		t.Fatal("expected miss")
	}
}

func TestTextCache_Overwrite(t *testing.T) {
	c := NewTextCache()
	c.Put("service", 100, "old")
	c.Put("service", 100, "new")
	got, _ := c.Get("service", 100)
	if got != "new" {
		t.Fatalf("expected new, got %q", got)
	}
}

func TestTextCache_Size(t *testing.T) {
	c := NewTextCache()
	if c.Size() != 0 {
		t.Fatalf("expected 0, got %d", c.Size())
	}
	c.Put("a", 1, "x")
	c.Put("b", 2, "y")
	if c.Size() != 2 {
		t.Fatalf("expected 2, got %d", c.Size())
	}
}

func TestTextCache_SameDivDifferentHash(t *testing.T) {
	c := NewTextCache()
	c.Put("service", 1, "a")
	c.Put("service", 2, "b")
	if c.Size() != 2 {
		t.Fatalf("expected 2, got %d", c.Size())
	}
}

// --- CounterCache tests ---

func TestCounterCache_PutGet(t *testing.T) {
	c := NewCounterCache()
	key := CounterKey{ObjHash: 100, Counter: "TPS", TimeType: 0}
	v := value.NewDecimalValue(42)
	c.Put(key, v)

	got, ok := c.Get(key)
	if !ok {
		t.Fatal("expected hit")
	}
	dv, ok := got.(*value.DecimalValue)
	if !ok || dv.Value != 42 {
		t.Fatalf("expected 42, got %v", got)
	}
}

func TestCounterCache_GetByObjHash(t *testing.T) {
	c := NewCounterCache()
	c.Put(CounterKey{ObjHash: 1, Counter: "TPS", TimeType: 0}, value.NewDecimalValue(10))
	c.Put(CounterKey{ObjHash: 1, Counter: "CPU", TimeType: 0}, value.NewDecimalValue(50))
	c.Put(CounterKey{ObjHash: 2, Counter: "TPS", TimeType: 0}, value.NewDecimalValue(20))

	result := c.GetByObjHash(1)
	if len(result) != 2 {
		t.Fatalf("expected 2 counters, got %d", len(result))
	}
	if _, ok := result["TPS"]; !ok {
		t.Fatal("missing TPS")
	}
	if _, ok := result["CPU"]; !ok {
		t.Fatal("missing CPU")
	}
}

func TestCounterCache_GetByObjHash_Empty(t *testing.T) {
	c := NewCounterCache()
	result := c.GetByObjHash(999)
	if len(result) != 0 {
		t.Fatalf("expected 0, got %d", len(result))
	}
}

// --- XLogCache tests ---

func TestXLogCache_PutAndGetRecent(t *testing.T) {
	c := NewXLogCache(10)
	c.Put(1, 100, false, []byte{1})
	c.Put(2, 200, true, []byte{2})
	c.Put(3, 300, false, []byte{3})

	entries := c.GetRecent(10)
	if len(entries) != 3 {
		t.Fatalf("expected 3, got %d", len(entries))
	}
	// Entries should be in order: oldest first
	if entries[0].ObjHash != 1 || entries[1].ObjHash != 2 || entries[2].ObjHash != 3 {
		t.Fatalf("unexpected order: %v", entries)
	}
}

func TestXLogCache_RingOverflow(t *testing.T) {
	c := NewXLogCache(3)
	c.Put(1, 100, false, []byte{1})
	c.Put(2, 200, false, []byte{2})
	c.Put(3, 300, false, []byte{3})
	c.Put(4, 400, false, []byte{4}) // overwrites entry 1

	entries := c.GetRecent(10)
	if len(entries) != 3 {
		t.Fatalf("expected 3, got %d", len(entries))
	}
	// Should have entries 2, 3, 4
	if entries[0].ObjHash != 2 || entries[1].ObjHash != 3 || entries[2].ObjHash != 4 {
		t.Fatalf("unexpected entries after overflow: %v", entries)
	}
}

func TestXLogCache_GetRecentMaxCount(t *testing.T) {
	c := NewXLogCache(10)
	for i := int32(0); i < 5; i++ {
		c.Put(i, i*100, false, []byte{byte(i)})
	}

	entries := c.GetRecent(2)
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
	// Should return the 2 most recent
	if entries[0].ObjHash != 3 || entries[1].ObjHash != 4 {
		t.Fatalf("unexpected entries: %v", entries)
	}
}

func TestXLogCache_GetRecentByObjHash(t *testing.T) {
	c := NewXLogCache(10)
	c.Put(1, 100, false, []byte{1})
	c.Put(2, 200, false, []byte{2})
	c.Put(1, 300, true, []byte{3})
	c.Put(3, 400, false, []byte{4})

	entries := c.GetRecentByObjHash(1, 10)
	if len(entries) != 2 {
		t.Fatalf("expected 2, got %d", len(entries))
	}
	if entries[0].Elapsed != 100 || entries[1].Elapsed != 300 {
		t.Fatalf("unexpected entries: %v", entries)
	}
}

func TestXLogCache_Count(t *testing.T) {
	c := NewXLogCache(10)
	if c.Count() != 0 {
		t.Fatalf("expected 0, got %d", c.Count())
	}
	c.Put(1, 100, false, []byte{1})
	c.Put(2, 200, false, []byte{2})
	if c.Count() != 2 {
		t.Fatalf("expected 2, got %d", c.Count())
	}
}

func TestXLogCache_Empty(t *testing.T) {
	c := NewXLogCache(10)
	entries := c.GetRecent(10)
	if len(entries) != 0 {
		t.Fatalf("expected 0, got %d", len(entries))
	}
}

// --- ObjectCache tests ---

func TestObjectCache_PutGet(t *testing.T) {
	c := NewObjectCache()
	op := &pack.ObjectPack{ObjName: "/test/agent", ObjHash: 123, ObjType: "java", Alive: true}
	c.Put(123, op)

	info, ok := c.Get(123)
	if !ok {
		t.Fatal("expected hit")
	}
	if info.Pack.ObjName != "/test/agent" {
		t.Fatalf("expected /test/agent, got %s", info.Pack.ObjName)
	}
}

func TestObjectCache_GetAll(t *testing.T) {
	c := NewObjectCache()
	c.Put(1, &pack.ObjectPack{ObjHash: 1, ObjName: "a"})
	c.Put(2, &pack.ObjectPack{ObjHash: 2, ObjName: "b"})

	all := c.GetAll()
	if len(all) != 2 {
		t.Fatalf("expected 2, got %d", len(all))
	}
}

func TestObjectCache_GetLive(t *testing.T) {
	c := NewObjectCache()
	c.Put(1, &pack.ObjectPack{ObjHash: 1, ObjName: "a"})
	c.Put(2, &pack.ObjectPack{ObjHash: 2, ObjName: "b"})

	live := c.GetLive(10 * time.Second)
	if len(live) != 2 {
		t.Fatalf("expected 2 live, got %d", len(live))
	}
}

func TestObjectCache_MarkDead(t *testing.T) {
	c := NewObjectCache()
	op := &pack.ObjectPack{ObjHash: 1, ObjName: "old", Alive: true}
	c.Put(1, op)

	// Manually backdate LastSeen
	c.mu.Lock()
	c.store[1].LastSeen = time.Now().Add(-1 * time.Minute)
	c.mu.Unlock()

	dead := c.MarkDead(30 * time.Second)
	if len(dead) != 1 {
		t.Fatalf("expected 1 dead, got %d", len(dead))
	}
	if dead[0].Pack.Alive {
		t.Fatal("expected Alive=false")
	}

	// Second call should return empty (already dead)
	dead = c.MarkDead(30 * time.Second)
	if len(dead) != 0 {
		t.Fatalf("expected 0 dead on second call, got %d", len(dead))
	}
}

func TestObjectCache_Size(t *testing.T) {
	c := NewObjectCache()
	if c.Size() != 0 {
		t.Fatalf("expected 0, got %d", c.Size())
	}
	c.Put(1, &pack.ObjectPack{ObjHash: 1})
	if c.Size() != 1 {
		t.Fatalf("expected 1, got %d", c.Size())
	}
}
