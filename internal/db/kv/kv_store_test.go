package kv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestKVStore_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// Test Set and Get
	store.Set("key1", "value1")
	val, ok := store.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Get failed: got (%v, %v), want (value1, true)", val, ok)
	}

	// Test non-existent key
	val, ok = store.Get("nonexistent")
	if ok {
		t.Errorf("Get of nonexistent key should return false, got (%v, %v)", val, ok)
	}
}

func TestKVStore_TTL(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// Set with short TTL
	store.SetTTL("expiring", "value", 100) // 100ms

	// Should be available immediately
	val, ok := store.Get("expiring")
	if !ok || val != "value" {
		t.Errorf("Get before expiry failed: got (%v, %v), want (value, true)", val, ok)
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Should be expired
	val, ok = store.Get("expiring")
	if ok {
		t.Errorf("Get after expiry should return false, got (%v, %v)", val, ok)
	}
}

func TestKVStore_BulkOperations(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// Set bulk
	pairs := map[string]string{
		"bulk1": "value1",
		"bulk2": "value2",
		"bulk3": "value3",
	}
	store.SetBulk(pairs)

	// Get bulk
	keys := []string{"bulk1", "bulk2", "bulk3", "nonexistent"}
	result := store.GetBulk(keys)

	if len(result) != 3 {
		t.Errorf("GetBulk returned %d entries, want 3", len(result))
	}

	for k, expectedVal := range pairs {
		if gotVal, ok := result[k]; !ok || gotVal != expectedVal {
			t.Errorf("GetBulk[%s] = (%v, %v), want (%v, true)", k, gotVal, ok, expectedVal)
		}
	}

	if _, ok := result["nonexistent"]; ok {
		t.Errorf("GetBulk should not return nonexistent keys")
	}
}

func TestKVStore_BulkWithExpiry(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// Set one with TTL, one without
	store.Set("persistent", "persists")
	store.SetTTL("expiring", "expires", 100) // 100ms

	// Both should be available
	result := store.GetBulk([]string{"persistent", "expiring"})
	if len(result) != 2 {
		t.Errorf("GetBulk before expiry returned %d entries, want 2", len(result))
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Only persistent should remain
	result = store.GetBulk([]string{"persistent", "expiring"})
	if len(result) != 1 {
		t.Errorf("GetBulk after expiry returned %d entries, want 1", len(result))
	}
	if val, ok := result["persistent"]; !ok || val != "persists" {
		t.Errorf("GetBulk should still return persistent key")
	}
}

func TestKVStore_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store and add data
	store1 := NewKVStore(tmpDir, "persist.json")
	store1.Set("key1", "value1")
	store1.Set("key2", "value2")
	store1.Close()

	// Wait a bit to ensure file is written
	time.Sleep(50 * time.Millisecond)

	// Verify file exists
	path := filepath.Join(tmpDir, "kv", "persist.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Persistence file not created: %v", err)
	}

	// Create new store with same path
	store2 := NewKVStore(tmpDir, "persist.json")
	defer store2.Close()

	// Should load previous data
	val, ok := store2.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Loaded store Get(key1) = (%v, %v), want (value1, true)", val, ok)
	}

	val, ok = store2.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("Loaded store Get(key2) = (%v, %v), want (value2, true)", val, ok)
	}
}

func TestKVStore_CleanupExpired(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// Add expired and non-expired entries
	store.Set("persistent", "value")
	store.SetTTL("expired", "value", 50) // 50ms

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Trigger cleanup
	store.cleanupExpired()

	// Check that expired entry is gone
	store.mu.RLock()
	_, hasExpired := store.data["expired"]
	_, hasPersistent := store.data["persistent"]
	store.mu.RUnlock()

	if hasExpired {
		t.Errorf("Expired entry should have been cleaned up")
	}
	if !hasPersistent {
		t.Errorf("Persistent entry should still exist")
	}
}

func TestKVStore_BackgroundTasks(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.Start(ctx)

	// Add data
	store.Set("key1", "value1")

	// Wait a bit for background save
	time.Sleep(100 * time.Millisecond)

	store.Close()

	// Verify file exists
	path := filepath.Join(tmpDir, "kv", "test.json")
	if _, err := os.Stat(path); err != nil {
		t.Errorf("Background save should have created file: %v", err)
	}
}

func TestKVStore_OverwriteKey(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// Set initial value
	store.Set("key", "value1")

	val, ok := store.Get("key")
	if !ok || val != "value1" {
		t.Errorf("Initial Get failed: got (%v, %v), want (value1, true)", val, ok)
	}

	// Overwrite
	store.Set("key", "value2")

	val, ok = store.Get("key")
	if !ok || val != "value2" {
		t.Errorf("Get after overwrite failed: got (%v, %v), want (value2, true)", val, ok)
	}
}

func TestKVStore_SetTTLZero(t *testing.T) {
	tmpDir := t.TempDir()
	store := NewKVStore(tmpDir, "test.json")
	defer store.Close()

	// SetTTL with 0 should mean no expiry
	store.SetTTL("key", "value", 0)

	// Should be available
	val, ok := store.Get("key")
	if !ok || val != "value" {
		t.Errorf("Get failed: got (%v, %v), want (value, true)", val, ok)
	}

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Should still be available
	val, ok = store.Get("key")
	if !ok || val != "value" {
		t.Errorf("Get after wait failed: got (%v, %v), want (value, true)", val, ok)
	}
}
