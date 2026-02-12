package db

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zbum/scouter-server-go/internal/util"
)

// DataPurgeScheduler implements per-type data purging with different retention
// periods, matching Java's AutoDeleteScheduler behavior.
//
// Purge order (shortest retention first):
//  1. Profile files within xlog/ (mgr_purge_profile_keep_days, default 10)
//  2. XLog directory (mgr_purge_xlog_keep_days, default 30)
//  3. Summary directory (mgr_purge_sum_data_days, default 60)
//  4. Entire date directory (mgr_purge_counter_keep_days, default 70)
type DataPurgeScheduler struct {
	baseDir string

	profileKeepDays         int
	xlogKeepDays            int
	sumKeepDays             int
	counterKeepDays         int
	realtimeCounterKeepDays int
	dailyTextKeepDays       int
	diskUsagePct            int
}

// NewDataPurgeScheduler creates a new per-type data purge scheduler.
func NewDataPurgeScheduler(baseDir string, profileKeepDays, xlogKeepDays, sumKeepDays, counterKeepDays, realtimeCounterKeepDays, dailyTextKeepDays, diskUsagePct int) *DataPurgeScheduler {
	return &DataPurgeScheduler{
		baseDir:                 baseDir,
		profileKeepDays:         profileKeepDays,
		xlogKeepDays:            xlogKeepDays,
		sumKeepDays:             sumKeepDays,
		counterKeepDays:         counterKeepDays,
		realtimeCounterKeepDays: realtimeCounterKeepDays,
		dailyTextKeepDays:       dailyTextKeepDays,
		diskUsagePct:            diskUsagePct,
	}
}

// Start begins the periodic purge goroutine (checks every minute, matching Java).
func (s *DataPurgeScheduler) Start(ctx context.Context) {
	// Run once immediately
	s.purgeAll()

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.purgeAll()
			}
		}
	}()
}

func (s *DataPurgeScheduler) purgeAll() {
	today := time.Now().Format("20060102")

	s.purgeByType(today, s.profileKeepDays, "profile", s.deleteProfile)
	s.purgeByType(today, s.xlogKeepDays, "xlog", s.deleteXLog)
	s.purgeByType(today, s.sumKeepDays, "summary", s.deleteSummary)
	s.purgeByType(today, s.realtimeCounterKeepDays, "realtime_counter", s.deleteRealtimeCounter)
	s.purgeByType(today, s.dailyTextKeepDays, "daily_text", s.deleteDailyText)
	s.purgeByType(today, s.counterKeepDays, "all", s.deleteAll)

	// Disk usage based purge: delete oldest date directories until under threshold
	s.purgeDiskUsage(today)
}

// purgeByType iterates over date directories and deletes data older than keepDays.
func (s *DataPurgeScheduler) purgeByType(today string, keepDays int, typeName string, deleteFn func(string) bool) {
	if keepDays <= 0 {
		return
	}

	cutoff := time.Now().AddDate(0, 0, -keepDays).Format("20060102")
	dates := s.listDateDirs()

	for _, date := range dates {
		if date >= cutoff || date == today {
			break // dates are sorted; remaining are all newer
		}
		if deleteFn(date) {
			slog.Info("DataPurge: purged", "type", typeName, "date", date, "keepDays", keepDays)
		}
	}
}

// listDateDirs returns sorted date directory names.
func (s *DataPurgeScheduler) listDateDirs() []string {
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil
	}

	var dates []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() && len(name) == 8 && isDateDir(name) {
			dates = append(dates, name)
		}
	}
	sort.Strings(dates)
	return dates
}

// deleteProfile removes profile-specific files from {date}/xlog/ directory.
// Returns true if any files were deleted.
func (s *DataPurgeScheduler) deleteProfile(date string) bool {
	xlogDir := filepath.Join(s.baseDir, date, "xlog")
	if _, err := os.Stat(xlogDir); os.IsNotExist(err) {
		return false
	}

	deleted := false
	profileFiles := []string{
		"xlog_prof.data",
		"xlog_prof.hfile",
		"xlog_prof.kfile",
	}
	for _, f := range profileFiles {
		path := filepath.Join(xlogDir, f)
		if err := os.Remove(path); err == nil {
			deleted = true
		}
	}
	return deleted
}

// deleteXLog removes the entire {date}/xlog/ directory.
func (s *DataPurgeScheduler) deleteXLog(date string) bool {
	dir := filepath.Join(s.baseDir, date, "xlog")
	return removeIfExists(dir)
}

// deleteSummary removes the entire {date}/summary/ directory.
func (s *DataPurgeScheduler) deleteSummary(date string) bool {
	dir := filepath.Join(s.baseDir, date, "summary")
	return removeIfExists(dir)
}

// deleteAll removes the entire {date}/ directory.
func (s *DataPurgeScheduler) deleteAll(date string) bool {
	dir := filepath.Join(s.baseDir, date)
	return removeIfExists(dir)
}

// deleteRealtimeCounter removes realtime counter files from {date}/counter/ directory.
func (s *DataPurgeScheduler) deleteRealtimeCounter(date string) bool {
	counterDir := filepath.Join(s.baseDir, date, "counter")
	if _, err := os.Stat(counterDir); os.IsNotExist(err) {
		return false
	}

	entries, err := os.ReadDir(counterDir)
	if err != nil {
		return false
	}

	deleted := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "real") {
			path := filepath.Join(counterDir, entry.Name())
			if err := os.Remove(path); err == nil {
				deleted = true
			}
		}
	}
	return deleted
}

// deleteDailyText removes the {date}/text/ directory.
func (s *DataPurgeScheduler) deleteDailyText(date string) bool {
	dir := filepath.Join(s.baseDir, date, "text")
	return removeIfExists(dir)
}

// purgeDiskUsage deletes oldest date directories when disk usage exceeds threshold.
func (s *DataPurgeScheduler) purgeDiskUsage(today string) {
	if s.diskUsagePct <= 0 {
		return
	}

	dates := s.listDateDirs()
	for _, date := range dates {
		if date == today {
			continue
		}
		usage := util.DiskUsagePct(s.baseDir)
		if usage <= s.diskUsagePct {
			break
		}
		dir := filepath.Join(s.baseDir, date)
		if removeIfExists(dir) {
			slog.Info("DataPurge: disk usage purge", "date", date, "usage%", usage, "threshold%", s.diskUsagePct)
		}
	}
}

// removeIfExists removes a file or directory if it exists.
func removeIfExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	if err := os.RemoveAll(path); err != nil {
		slog.Error("DataPurge: remove error", "path", path, "error", err)
		return false
	}
	return true
}
