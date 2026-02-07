package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAutoDeleteScheduler_Cleanup(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create date directories spanning different time periods
	oldDate1 := "20240101" // Very old
	oldDate2 := "20240615" // Old
	recentDate := time.Now().AddDate(0, 0, -15).Format("20060102") // 15 days ago
	todayDate := time.Now().Format("20060102")

	dirs := []string{oldDate1, oldDate2, recentDate, todayDate}
	for _, d := range dirs {
		path := filepath.Join(tempDir, d)
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatalf("Failed to create test dir %s: %v", path, err)
		}
		// Create a dummy file in each directory
		testFile := filepath.Join(path, "test.dat")
		if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	// Create scheduler with keepDays=30
	scheduler := NewAutoDeleteScheduler(tempDir, 30)
	scheduler.cleanup()

	// Check which directories still exist
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read temp dir: %v", err)
	}

	remaining := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			remaining[entry.Name()] = true
		}
	}

	// Old dates should be removed
	if remaining[oldDate1] {
		t.Errorf("Expected %s to be removed, but it still exists", oldDate1)
	}
	if remaining[oldDate2] {
		t.Errorf("Expected %s to be removed, but it still exists", oldDate2)
	}

	// Recent dates should remain
	if !remaining[recentDate] {
		t.Errorf("Expected %s to remain, but it was removed", recentDate)
	}
	if !remaining[todayDate] {
		t.Errorf("Expected %s to remain, but it was removed", todayDate)
	}
}

func TestAutoDeleteScheduler_KeepsRecent(t *testing.T) {
	tempDir := t.TempDir()

	// Create only recent directories
	today := time.Now().Format("20060102")
	yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")
	lastWeek := time.Now().AddDate(0, 0, -7).Format("20060102")

	dirs := []string{today, yesterday, lastWeek}
	for _, d := range dirs {
		path := filepath.Join(tempDir, d)
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatalf("Failed to create test dir %s: %v", path, err)
		}
	}

	// Create scheduler with keepDays=30
	scheduler := NewAutoDeleteScheduler(tempDir, 30)
	scheduler.cleanup()

	// All directories should still exist
	for _, d := range dirs {
		path := filepath.Join(tempDir, d)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected recent directory %s to remain, but it was removed", d)
		}
	}
}

func TestAutoDeleteScheduler_IgnoresNonDateDirs(t *testing.T) {
	tempDir := t.TempDir()

	// Create non-date directories
	nonDateDirs := []string{"config", "logs", "temp", "backup"}
	for _, d := range nonDateDirs {
		path := filepath.Join(tempDir, d)
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatalf("Failed to create test dir %s: %v", path, err)
		}
	}

	// Create an old date directory
	oldDate := "20200101"
	oldPath := filepath.Join(tempDir, oldDate)
	if err := os.Mkdir(oldPath, 0755); err != nil {
		t.Fatalf("Failed to create old date dir: %v", err)
	}

	scheduler := NewAutoDeleteScheduler(tempDir, 30)
	scheduler.cleanup()

	// Non-date directories should still exist
	for _, d := range nonDateDirs {
		path := filepath.Join(tempDir, d)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected non-date directory %s to remain, but it was removed", d)
		}
	}

	// Old date directory should be removed
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Errorf("Expected old date directory %s to be removed, but it still exists", oldDate)
	}
}

func TestIsDateDir(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid date 1", "20240101", true},
		{"valid date 2", "20250615", true},
		{"valid date 3", "20261231", true},
		{"invalid month", "20241301", false},
		{"invalid day", "20240132", false},
		{"too short", "2024010", false},
		{"too long", "202401011", false},
		{"contains letters", "2024010a", false},
		{"contains special chars", "2024-01-01", false},
		{"not a date", "config", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDateDir(tt.input)
			if result != tt.expected {
				t.Errorf("isDateDir(%s) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetDateDirs(t *testing.T) {
	tempDir := t.TempDir()

	// Create mixed directories
	dateDirs := []string{"20240101", "20250615", "20240315", "20260101"}
	nonDateDirs := []string{"config", "logs", "12345678"} // 12345678 is not a valid date

	for _, d := range dateDirs {
		path := filepath.Join(tempDir, d)
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatalf("Failed to create date dir %s: %v", path, err)
		}
	}

	for _, d := range nonDateDirs {
		path := filepath.Join(tempDir, d)
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatalf("Failed to create non-date dir %s: %v", path, err)
		}
	}

	// Create a file (should be ignored)
	testFile := filepath.Join(tempDir, "20240101.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	result, err := GetDateDirs(tempDir)
	if err != nil {
		t.Fatalf("GetDateDirs failed: %v", err)
	}

	// Should return sorted date directories only
	expected := []string{"20240101", "20240315", "20250615", "20260101"}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d date dirs, got %d: %v", len(expected), len(result), result)
	}

	for i, exp := range expected {
		if result[i] != exp {
			t.Errorf("Expected result[%d] = %s, got %s", i, exp, result[i])
		}
	}
}

func TestAutoDeleteScheduler_Start(t *testing.T) {
	tempDir := t.TempDir()

	// Create an old date directory
	oldDate := "20200101"
	oldPath := filepath.Join(tempDir, oldDate)
	if err := os.Mkdir(oldPath, 0755); err != nil {
		t.Fatalf("Failed to create old date dir: %v", err)
	}

	scheduler := NewAutoDeleteScheduler(tempDir, 30)
	scheduler.checkInterval = 100 * time.Millisecond // Speed up for testing

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler.Start(ctx)

	// Give it a moment to run the initial cleanup
	time.Sleep(50 * time.Millisecond)

	// Old directory should be removed
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Errorf("Expected old date directory to be removed after Start()")
	}

	// Wait for at least one more tick
	time.Sleep(150 * time.Millisecond)

	// Cancel context
	cancel()

	// Give goroutine time to exit
	time.Sleep(50 * time.Millisecond)
}

func TestAutoDeleteScheduler_NonExistentBaseDir(t *testing.T) {
	// Use a directory that doesn't exist
	nonExistentDir := filepath.Join(t.TempDir(), "does-not-exist")

	scheduler := NewAutoDeleteScheduler(nonExistentDir, 30)

	// Should not panic or error
	scheduler.cleanup()
}
