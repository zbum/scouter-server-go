package text

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zbum/scouter-server-go/internal/util"
)

func TestTextTable_SetGet(t *testing.T) {
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "20260207")

	table, err := NewTextTable(dir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	// Test Set and Get
	div := "service"
	text := "UserService.getUser"
	hash := util.HashString(text)

	err = table.Set(div, hash, text)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, found, err := table.Get(div, hash)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected text to be found")
	}
	if retrieved != text {
		t.Errorf("Expected %q, got %q", text, retrieved)
	}
}

func TestTextTable_MultipleTexts(t *testing.T) {
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "20260207")

	table, err := NewTextTable(dir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	// Store multiple texts with different divs
	testCases := []struct {
		div  string
		text string
	}{
		{"service", "UserService.login"},
		{"service", "OrderService.create"},
		{"sql", "SELECT * FROM users WHERE id = ?"},
		{"sql", "INSERT INTO orders VALUES (?, ?, ?)"},
		{"error", "NullPointerException at line 42"},
	}

	for _, tc := range testCases {
		hash := util.HashString(tc.text)
		err := table.Set(tc.div, hash, tc.text)
		if err != nil {
			t.Fatalf("Set failed for %s: %v", tc.text, err)
		}
	}

	// Retrieve all texts
	for _, tc := range testCases {
		hash := util.HashString(tc.text)
		retrieved, found, err := table.Get(tc.div, hash)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", tc.text, err)
		}
		if !found {
			t.Fatalf("Text not found: %s", tc.text)
		}
		if retrieved != tc.text {
			t.Errorf("Expected %q, got %q", tc.text, retrieved)
		}
	}
}

func TestTextTable_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "20260207")

	table, err := NewTextTable(dir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	// Try to get non-existent text
	retrieved, found, err := table.Get("service", 12345)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Errorf("Expected not found, but got: %q", retrieved)
	}
	if retrieved != "" {
		t.Errorf("Expected empty string, got: %q", retrieved)
	}
}

func TestTextWR_AsyncWrite(t *testing.T) {
	tmpDir := t.TempDir()

	wr := NewTextWR(tmpDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr.Start(ctx)

	// Add some texts
	date := "20260207"
	texts := []struct {
		div  string
		text string
	}{
		{"service", "UserService.login"},
		{"service", "OrderService.create"},
		{"sql", "SELECT * FROM users"},
	}

	for _, tc := range texts {
		hash := util.HashString(tc.text)
		wr.Add(date, tc.div, hash, tc.text)
	}

	// Wait for async processing and close writer
	wr.Flush()
	wr.Close()

	// Verify writes
	dir := filepath.Join(tmpDir, date)
	table, err := NewTextTable(dir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	for _, tc := range texts {
		hash := util.HashString(tc.text)
		retrieved, found, err := table.Get(tc.div, hash)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", tc.text, err)
		}
		if !found {
			t.Fatalf("Text not found: %s", tc.text)
		}
		if retrieved != tc.text {
			t.Errorf("Expected %q, got %q", tc.text, retrieved)
		}
	}
}

func TestTextWR_Deduplication(t *testing.T) {
	tmpDir := t.TempDir()

	wr := NewTextWR(tmpDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr.Start(ctx)

	// Add the same text twice
	date := "20260207"
	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	wr.Add(date, div, hash, text)
	wr.Add(date, div, hash, text)

	// Wait for async processing
	wr.Flush()

	// Check dedup cache before closing
	wr.mu.Lock()
	key := dupKey{Date: date, Div: div, Hash: hash}
	_, exists := wr.dupCheck[key]
	wr.mu.Unlock()

	if !exists {
		t.Error("Expected text to be in dedup cache")
	}

	// Close writer before verification
	wr.Close()

	// Verify only one write occurred
	dir := filepath.Join(tmpDir, date)
	table, err := NewTextTable(dir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	retrieved, found, err := table.Get(div, hash)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Text not found")
	}
	if retrieved != text {
		t.Errorf("Expected %q, got %q", text, retrieved)
	}
}

func TestTextRD_Read(t *testing.T) {
	tmpDir := t.TempDir()

	// First write some data
	wr := NewTextWR(tmpDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr.Start(ctx)

	date := "20260207"
	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	wr.Add(date, div, hash, text)
	wr.Flush()
	wr.Close()

	// Now read it back
	rd := NewTextRD(tmpDir)
	defer rd.Close()

	retrieved, err := rd.GetString(date, div, hash)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved != text {
		t.Errorf("Expected %q, got %q", text, retrieved)
	}
}

func TestTextRD_Cache(t *testing.T) {
	tmpDir := t.TempDir()

	// First write some data
	wr := NewTextWR(tmpDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr.Start(ctx)

	date := "20260207"
	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	wr.Add(date, div, hash, text)
	wr.Flush()
	wr.Close()

	// Read twice to test cache
	rd := NewTextRD(tmpDir)
	defer rd.Close()

	// First read (from table)
	retrieved1, err := rd.GetString(date, div, hash)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved1 != text {
		t.Errorf("Expected %q, got %q", text, retrieved1)
	}

	// Second read (from cache)
	retrieved2, err := rd.GetString(date, div, hash)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved2 != text {
		t.Errorf("Expected %q, got %q", text, retrieved2)
	}

	// Verify cache hit
	rd.mu.Lock()
	key := cacheKey{Date: date, Div: div, Hash: hash}
	cached, inCache := rd.cache[key]
	rd.mu.Unlock()

	if !inCache {
		t.Error("Expected text to be in cache")
	}
	if cached != text {
		t.Errorf("Expected cached %q, got %q", text, cached)
	}
}

func TestTextRD_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	rd := NewTextRD(tmpDir)
	defer rd.Close()

	// Try to read non-existent text
	retrieved, err := rd.GetString("20260207", "service", 12345)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved != "" {
		t.Errorf("Expected empty string, got: %q", retrieved)
	}
}

func TestMultipleDates(t *testing.T) {
	tmpDir := t.TempDir()

	wr := NewTextWR(tmpDir)
	defer wr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr.Start(ctx)

	// Write to different dates
	dates := []string{"20260207", "20260208", "20260209"}
	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	for _, date := range dates {
		wr.Add(date, div, hash, text)
	}

	wr.Flush()
	wr.Close()

	// Verify each date has its own directory and data
	for _, date := range dates {
		dir := filepath.Join(tmpDir, date)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to exist", dir)
		}

		table, err := NewTextTable(dir)
		if err != nil {
			t.Fatalf("NewTextTable failed for %s: %v", date, err)
		}

		retrieved, found, err := table.Get(div, hash)
		table.Close()

		if err != nil {
			t.Fatalf("Get failed for %s: %v", date, err)
		}
		if !found {
			t.Fatalf("Text not found for date %s", date)
		}
		if retrieved != text {
			t.Errorf("Expected %q, got %q for date %s", text, retrieved, date)
		}
	}
}

func TestTextTable_UnicodeText(t *testing.T) {
	tmpDir := t.TempDir()
	dir := filepath.Join(tmpDir, "20260207")

	table, err := NewTextTable(dir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	// Test with various unicode strings
	texts := []string{
		"사용자 서비스",
		"ユーザーサービス",
		"用户服务",
		"SELECT * FROM users WHERE name = '한글'",
		"Error: Не удалось подключиться к базе данных",
	}

	for _, text := range texts {
		hash := util.HashString(text)
		div := "unicode"

		err := table.Set(div, hash, text)
		if err != nil {
			t.Fatalf("Set failed for %q: %v", text, err)
		}

		retrieved, found, err := table.Get(div, hash)
		if err != nil {
			t.Fatalf("Get failed for %q: %v", text, err)
		}
		if !found {
			t.Fatalf("Text not found: %q", text)
		}
		if retrieved != text {
			t.Errorf("Expected %q, got %q", text, retrieved)
		}
	}
}
