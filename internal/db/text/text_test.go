package text

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/zbum/scouter-server-go/internal/util"
)

func TestTextPermTable_SetGet(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextPermTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
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

func TestTextPermTable_MultipleTexts(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextPermTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
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

func TestTextPermTable_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextPermTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
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

func TestTextPermTable_Dedup(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextPermTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
	}
	defer table.Close()

	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	// Set twice — second should be no-op
	err = table.Set(div, hash, text)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	err = table.Set(div, hash, text)
	if err != nil {
		t.Fatalf("Set (2nd) failed: %v", err)
	}

	// HasKey
	exists, err := table.HasKey(div, hash)
	if err != nil {
		t.Fatalf("HasKey failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected key to exist")
	}

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

func TestTextTable_DailySetGet(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	// Test Set and Get with composite key
	div := "error"
	text := "NullPointerException at line 42"
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

func TestTextTable_DailyMultipleDivs(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextTable failed: %v", err)
	}
	defer table.Close()

	// Same hash with different divs should be different entries
	text1 := "UserService.login"
	text2 := "OrderService.login"
	hash1 := util.HashString(text1)
	hash2 := util.HashString(text2)

	table.Set("service", hash1, text1)
	table.Set("error", hash2, text2)

	r1, found1, _ := table.Get("service", hash1)
	r2, found2, _ := table.Get("error", hash2)

	if !found1 || r1 != text1 {
		t.Errorf("service text: expected %q, got %q (found=%v)", text1, r1, found1)
	}
	if !found2 || r2 != text2 {
		t.Errorf("error text: expected %q, got %q (found=%v)", text2, r2, found2)
	}
}

func TestTextWR_AsyncWrite(t *testing.T) {
	tmpDir := t.TempDir()

	wr := NewTextWR(tmpDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr.Start(ctx)

	// Add some texts
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
		wr.Add(tc.div, hash, tc.text)
	}

	// Wait for async processing and close writer
	wr.Flush()
	wr.Close()

	// Verify writes — permanent text stored in 00000000/text directory using TextPermTable
	dir := filepath.Join(tmpDir, textDirName, "text")
	table, err := NewTextPermTable(dir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
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
	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	wr.Add(div, hash, text)
	wr.Add(div, hash, text)

	// Wait for async processing
	wr.Flush()

	// Check dedup cache before closing
	wr.mu.Lock()
	key := dupKey{Div: div, Hash: hash}
	_, exists := wr.dupCheck[key]
	wr.mu.Unlock()

	if !exists {
		t.Error("Expected text to be in dedup cache")
	}

	// Close writer before verification
	wr.Close()

	// Verify only one write occurred using TextPermTable
	dir := filepath.Join(tmpDir, textDirName, "text")
	table, err := NewTextPermTable(dir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
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

	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	wr.Add(div, hash, text)
	wr.Flush()
	wr.Close()

	// Now read it back
	rd := NewTextRD(tmpDir)
	defer rd.Close()

	retrieved, err := rd.GetString(div, hash)
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

	div := "service"
	text := "UserService.login"
	hash := util.HashString(text)

	wr.Add(div, hash, text)
	wr.Flush()
	wr.Close()

	// Read twice to test cache
	rd := NewTextRD(tmpDir)
	defer rd.Close()

	// First read (from table)
	retrieved1, err := rd.GetString(div, hash)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved1 != text {
		t.Errorf("Expected %q, got %q", text, retrieved1)
	}

	// Second read (from cache)
	retrieved2, err := rd.GetString(div, hash)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved2 != text {
		t.Errorf("Expected %q, got %q", text, retrieved2)
	}

	// Verify cache hit
	rd.mu.Lock()
	key := cacheKey{Div: div, Hash: hash}
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
	retrieved, err := rd.GetString("service", 12345)
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if retrieved != "" {
		t.Errorf("Expected empty string, got: %q", retrieved)
	}
}

func TestTextPermTable_UnicodeText(t *testing.T) {
	tmpDir := t.TempDir()

	table, err := NewTextPermTable(tmpDir)
	if err != nil {
		t.Fatalf("NewTextPermTable failed: %v", err)
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

func TestTextWR_DailyText(t *testing.T) {
	tmpDir := t.TempDir()

	wr := NewTextWR(tmpDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wr.Start(ctx)

	div := "error"
	text := "NullPointerException"
	hash := util.HashString(text)
	date := "20260215"

	wr.AddDaily(date, div, hash, text)
	wr.Close()

	// Verify daily text
	retrieved, err := wr.GetDailyString(date, div, hash)
	// Writer is closed, so we need to read from a fresh reader
	rd := NewTextRD(tmpDir)
	defer rd.Close()

	retrieved, err = rd.GetDailyString(date, div, hash)
	if err != nil {
		t.Fatalf("GetDailyString failed: %v", err)
	}
	if retrieved != text {
		t.Errorf("Expected %q, got %q", text, retrieved)
	}
}
