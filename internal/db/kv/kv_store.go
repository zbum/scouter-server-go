package kv

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// KVStore provides in-memory key-value storage with file persistence.
type KVStore struct {
	mu       sync.RWMutex
	data     map[string]kvEntry
	baseDir  string
	filename string
	dirty    bool // tracks if data has changed since last save
}

type kvEntry struct {
	Value     string `json:"value"`
	ExpiresAt int64  `json:"expires_at"` // 0 means no expiry
}

// persistedData is the structure saved to disk.
type persistedData struct {
	Entries map[string]kvEntry `json:"entries"`
}

// NewKVStore creates a new KV store with the given base directory and filename.
func NewKVStore(baseDir, filename string) *KVStore {
	s := &KVStore{
		data:     make(map[string]kvEntry),
		baseDir:  baseDir,
		filename: filename,
	}
	s.load()
	return s
}

// Get retrieves a value by key. Returns the value and true if found and not expired.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return "", false
	}

	// Check expiry
	if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
		return "", false
	}

	return entry.Value, true
}

// Set stores a key-value pair with no expiry.
func (s *KVStore) Set(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = kvEntry{
		Value:     value,
		ExpiresAt: 0,
	}
	s.dirty = true
}

// SetTTL stores a key-value pair with a TTL in milliseconds.
func (s *KVStore) SetTTL(key string, value string, ttlMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiresAt := int64(0)
	if ttlMs > 0 {
		expiresAt = time.Now().UnixMilli() + ttlMs
	}

	s.data[key] = kvEntry{
		Value:     value,
		ExpiresAt: expiresAt,
	}
	s.dirty = true
}

// GetBulk retrieves multiple values by their keys.
// Returns a map containing only the found and non-expired keys.
func (s *KVStore) GetBulk(keys []string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string)
	now := time.Now().UnixMilli()

	for _, key := range keys {
		if entry, ok := s.data[key]; ok {
			// Check expiry
			if entry.ExpiresAt == 0 || now <= entry.ExpiresAt {
				result[key] = entry.Value
			}
		}
	}

	return result
}

// SetBulk stores multiple key-value pairs with no expiry.
func (s *KVStore) SetBulk(pairs map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, value := range pairs {
		s.data[key] = kvEntry{
			Value:     value,
			ExpiresAt: 0,
		}
	}
	s.dirty = true
}

// Start begins background tasks: cleanup of expired entries and periodic save.
func (s *KVStore) Start(ctx context.Context) {
	go s.backgroundTasks(ctx)
}

// backgroundTasks runs periodic cleanup and save operations.
func (s *KVStore) backgroundTasks(ctx context.Context) {
	cleanupTicker := time.NewTicker(60 * time.Second)
	saveTicker := time.NewTicker(30 * time.Second)
	defer cleanupTicker.Stop()
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			s.cleanupExpired()
		case <-saveTicker.C:
			s.save()
		}
	}
}

// cleanupExpired removes expired entries from the store.
func (s *KVStore) cleanupExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	removed := 0

	for key, entry := range s.data {
		if entry.ExpiresAt > 0 && now > entry.ExpiresAt {
			delete(s.data, key)
			removed++
		}
	}

	if removed > 0 {
		s.dirty = true
		slog.Debug("KV store cleanup", "file", s.filename, "removed", removed)
	}
}

// load reads the store from disk.
func (s *KVStore) load() {
	path := s.getFilePath()

	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("KV store load error", "file", s.filename, "error", err)
		}
		return
	}

	var pd persistedData
	if err := json.Unmarshal(data, &pd); err != nil {
		slog.Warn("KV store unmarshal error", "file", s.filename, "error", err)
		return
	}

	s.mu.Lock()
	s.data = pd.Entries
	if s.data == nil {
		s.data = make(map[string]kvEntry)
	}
	s.mu.Unlock()

	slog.Info("KV store loaded", "file", s.filename, "entries", len(s.data))
}

// save writes the store to disk.
func (s *KVStore) save() {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}

	pd := persistedData{
		Entries: s.data,
	}
	s.dirty = false
	s.mu.Unlock()

	data, err := json.MarshalIndent(pd, "", "  ")
	if err != nil {
		slog.Error("KV store marshal error", "file", s.filename, "error", err)
		return
	}

	path := s.getFilePath()
	dir := filepath.Dir(path)

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("KV store mkdir error", "file", s.filename, "error", err)
		return
	}

	// Write atomically using temp file + rename
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		slog.Error("KV store write error", "file", s.filename, "error", err)
		return
	}

	if err := os.Rename(tmpPath, path); err != nil {
		slog.Error("KV store rename error", "file", s.filename, "error", err)
		return
	}

	slog.Debug("KV store saved", "file", s.filename, "entries", len(pd.Entries))
}

// getFilePath returns the full path to the persistence file.
func (s *KVStore) getFilePath() string {
	return filepath.Join(s.baseDir, "kv", s.filename)
}

// Close saves the store and releases resources.
func (s *KVStore) Close() {
	s.save()
	slog.Info("KV store closed", "file", s.filename)
}
