package cache

import (
	"fmt"
	"testing"
)

func TestTextCacheLRU_Eviction(t *testing.T) {
	c := NewTextCacheWithSize(3)

	c.Put("s", 1, "one")
	c.Put("s", 2, "two")
	c.Put("s", 3, "three")

	if c.Size() != 3 {
		t.Fatalf("expected size 3, got %d", c.Size())
	}

	// Adding a 4th entry should evict the oldest (key 1)
	c.Put("s", 4, "four")

	if c.Size() != 3 {
		t.Fatalf("expected size 3 after eviction, got %d", c.Size())
	}

	_, ok := c.Get("s", 1)
	if ok {
		t.Fatal("key 1 should have been evicted")
	}

	val, ok := c.Get("s", 2)
	if !ok || val != "two" {
		t.Fatalf("key 2 should still exist, got %q (ok=%v)", val, ok)
	}
}

func TestTextCacheLRU_AccessRefreshes(t *testing.T) {
	c := NewTextCacheWithSize(3)

	c.Put("s", 1, "one")
	c.Put("s", 2, "two")
	c.Put("s", 3, "three")

	// Access key 1 to refresh it (move to front)
	c.Get("s", 1)

	// Now add key 4 -- key 2 should be evicted (it's the oldest)
	c.Put("s", 4, "four")

	_, ok := c.Get("s", 2)
	if ok {
		t.Fatal("key 2 should have been evicted (key 1 was refreshed)")
	}

	val, ok := c.Get("s", 1)
	if !ok || val != "one" {
		t.Fatalf("key 1 should still exist after refresh, got %q (ok=%v)", val, ok)
	}
}

func TestTextCacheLRU_UpdateExisting(t *testing.T) {
	c := NewTextCacheWithSize(3)

	c.Put("s", 1, "old")
	c.Put("s", 1, "new")

	if c.Size() != 1 {
		t.Fatalf("expected size 1, got %d", c.Size())
	}

	val, ok := c.Get("s", 1)
	if !ok || val != "new" {
		t.Fatalf("expected new, got %q", val)
	}
}

func TestTextCacheLRU_DefaultMaxSize(t *testing.T) {
	c := NewTextCache()
	if c.maxSize != defaultTextCacheMaxSize {
		t.Fatalf("expected default max size %d, got %d", defaultTextCacheMaxSize, c.maxSize)
	}
}

func TestTextCacheLRU_EvictionOrder(t *testing.T) {
	c := NewTextCacheWithSize(5)

	for i := 0; i < 5; i++ {
		c.Put("s", int32(i), fmt.Sprintf("v%d", i))
	}

	// Access 0, 1 to refresh them
	c.Get("s", 0)
	c.Get("s", 1)

	// Add 3 more -- should evict 2, 3, 4 (oldest not-recently-accessed)
	for i := 5; i < 8; i++ {
		c.Put("s", int32(i), fmt.Sprintf("v%d", i))
	}

	// 0, 1 should survive (they were refreshed)
	for _, id := range []int32{0, 1} {
		_, ok := c.Get("s", id)
		if !ok {
			t.Fatalf("key %d should have survived eviction", id)
		}
	}

	// 2, 3, 4 should be evicted
	for _, id := range []int32{2, 3, 4} {
		_, ok := c.Get("s", id)
		if ok {
			t.Fatalf("key %d should have been evicted", id)
		}
	}
}
