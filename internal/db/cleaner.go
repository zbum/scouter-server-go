package db

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// AutoDeleteScheduler periodically removes old date directories.
type AutoDeleteScheduler struct {
	baseDir       string
	keepDays      int
	checkInterval time.Duration
}

// NewAutoDeleteScheduler creates a new scheduler.
// keepDays: number of days to keep data (e.g., 30).
func NewAutoDeleteScheduler(baseDir string, keepDays int) *AutoDeleteScheduler {
	return &AutoDeleteScheduler{
		baseDir:       baseDir,
		keepDays:      keepDays,
		checkInterval: 1 * time.Hour,
	}
}

// Start begins the periodic cleanup goroutine.
func (s *AutoDeleteScheduler) Start(ctx context.Context) {
	// Run once immediately
	s.cleanup()

	go func() {
		ticker := time.NewTicker(s.checkInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cleanup()
			}
		}
	}()
}

// cleanup scans the base directory for date directories and removes old ones.
func (s *AutoDeleteScheduler) cleanup() {
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		slog.Error("AutoDelete: scan dir error", "error", err)
		return
	}

	cutoff := time.Now().AddDate(0, 0, -s.keepDays).Format("20060102")

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Only process directories that look like dates (8 digits)
		if len(name) != 8 || !isDateDir(name) {
			continue
		}

		if name < cutoff {
			dir := filepath.Join(s.baseDir, name)
			slog.Info("AutoDelete: removing old data", "date", name, "dir", dir)
			if err := os.RemoveAll(dir); err != nil {
				slog.Error("AutoDelete: remove error", "dir", dir, "error", err)
			}
		}
	}
}

// isDateDir checks if a string looks like YYYYMMDD.
func isDateDir(s string) bool {
	if len(s) != 8 {
		return false
	}
	// "00000000" is the permanent text storage directory — never treat as a date dir.
	if s == "00000000" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	// Basic validation: month 01-12, day 01-31
	_, err := time.Parse("20060102", s)
	return err == nil
}

// GetDateDirs returns sorted list of date directory names in the base directory.
func GetDateDirs(baseDir string) ([]string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	var dates []string
	for _, entry := range entries {
		if entry.IsDir() && isDateDir(entry.Name()) {
			dates = append(dates, entry.Name())
		}
	}
	sort.Strings(dates)
	return dates, nil
}
