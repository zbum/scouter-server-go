package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDataPurgeScheduler_PurgeProfile(t *testing.T) {
	dir := t.TempDir()

	// Create date directories with xlog subdirectories and profile files
	oldDate := time.Now().AddDate(0, 0, -15).Format("20060102") // 15 days ago
	newDate := time.Now().AddDate(0, 0, -5).Format("20060102")  // 5 days ago

	for _, date := range []string{oldDate, newDate} {
		xlogDir := filepath.Join(dir, date, "xlog")
		os.MkdirAll(xlogDir, 0755)
		// Create profile files
		os.WriteFile(filepath.Join(xlogDir, "xlog_prof.data"), []byte("data"), 0644)
		os.WriteFile(filepath.Join(xlogDir, "xlog_prof.hfile"), []byte("hash"), 0644)
		os.WriteFile(filepath.Join(xlogDir, "xlog_prof.kfile"), []byte("key"), 0644)
		// Create xlog files (should NOT be deleted by profile purge)
		os.WriteFile(filepath.Join(xlogDir, "xlog.data"), []byte("xlog"), 0644)
		os.WriteFile(filepath.Join(xlogDir, "xlog_tim.hfile"), []byte("time"), 0644)
	}

	// Profile keep 10 days: oldDate (15 days) should be purged, newDate (5 days) should remain
	scheduler := NewDataPurgeScheduler(dir, 10, 0, 0, 0)
	scheduler.purgeAll()

	// Old date: profile files should be deleted, xlog files should remain
	if _, err := os.Stat(filepath.Join(dir, oldDate, "xlog", "xlog_prof.data")); !os.IsNotExist(err) {
		t.Error("old profile data should be deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, oldDate, "xlog", "xlog.data")); os.IsNotExist(err) {
		t.Error("old xlog data should NOT be deleted by profile purge")
	}

	// New date: all files should remain
	if _, err := os.Stat(filepath.Join(dir, newDate, "xlog", "xlog_prof.data")); os.IsNotExist(err) {
		t.Error("new profile data should remain")
	}
}

func TestDataPurgeScheduler_PurgeXLog(t *testing.T) {
	dir := t.TempDir()

	oldDate := time.Now().AddDate(0, 0, -35).Format("20060102")
	newDate := time.Now().AddDate(0, 0, -5).Format("20060102")

	for _, date := range []string{oldDate, newDate} {
		xlogDir := filepath.Join(dir, date, "xlog")
		os.MkdirAll(xlogDir, 0755)
		os.WriteFile(filepath.Join(xlogDir, "xlog.data"), []byte("data"), 0644)
		// Also create counter dir (should NOT be deleted by xlog purge)
		counterDir := filepath.Join(dir, date, "counter")
		os.MkdirAll(counterDir, 0755)
		os.WriteFile(filepath.Join(counterDir, "counter.data"), []byte("cnt"), 0644)
	}

	// XLog keep 30 days: oldDate (35 days) should have xlog dir deleted
	scheduler := NewDataPurgeScheduler(dir, 0, 30, 0, 0)
	scheduler.purgeAll()

	// Old date: xlog dir should be gone, counter should remain
	if _, err := os.Stat(filepath.Join(dir, oldDate, "xlog")); !os.IsNotExist(err) {
		t.Error("old xlog dir should be deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, oldDate, "counter", "counter.data")); os.IsNotExist(err) {
		t.Error("old counter data should NOT be deleted by xlog purge")
	}

	// New date: everything should remain
	if _, err := os.Stat(filepath.Join(dir, newDate, "xlog", "xlog.data")); os.IsNotExist(err) {
		t.Error("new xlog data should remain")
	}
}

func TestDataPurgeScheduler_PurgeSummary(t *testing.T) {
	dir := t.TempDir()

	oldDate := time.Now().AddDate(0, 0, -65).Format("20060102")

	sumDir := filepath.Join(dir, oldDate, "summary")
	os.MkdirAll(sumDir, 0755)
	os.WriteFile(filepath.Join(sumDir, "sum.data"), []byte("sum"), 0644)

	// Summary keep 60 days
	scheduler := NewDataPurgeScheduler(dir, 0, 0, 60, 0)
	scheduler.purgeAll()

	if _, err := os.Stat(filepath.Join(dir, oldDate, "summary")); !os.IsNotExist(err) {
		t.Error("old summary dir should be deleted")
	}
}

func TestDataPurgeScheduler_PurgeAll(t *testing.T) {
	dir := t.TempDir()

	oldDate := time.Now().AddDate(0, 0, -75).Format("20060102")

	dateDir := filepath.Join(dir, oldDate)
	os.MkdirAll(filepath.Join(dateDir, "xlog"), 0755)
	os.MkdirAll(filepath.Join(dateDir, "counter"), 0755)
	os.MkdirAll(filepath.Join(dateDir, "summary"), 0755)
	os.MkdirAll(filepath.Join(dateDir, "alert"), 0755)

	// Counter keep 70 days (triggers full directory deletion)
	scheduler := NewDataPurgeScheduler(dir, 0, 0, 0, 70)
	scheduler.purgeAll()

	if _, err := os.Stat(dateDir); !os.IsNotExist(err) {
		t.Error("old date dir should be completely deleted")
	}
}

func TestDataPurgeScheduler_DoNotDeleteToday(t *testing.T) {
	dir := t.TempDir()

	today := time.Now().Format("20060102")
	xlogDir := filepath.Join(dir, today, "xlog")
	os.MkdirAll(xlogDir, 0755)
	os.WriteFile(filepath.Join(xlogDir, "xlog.data"), []byte("data"), 0644)

	// Even with keepDays=0, today should not be deleted (purge skips keepDays <= 0)
	scheduler := NewDataPurgeScheduler(dir, 1, 1, 1, 1)
	scheduler.purgeAll()

	if _, err := os.Stat(filepath.Join(dir, today, "xlog", "xlog.data")); os.IsNotExist(err) {
		t.Error("today's data should NOT be deleted")
	}
}

func TestDataPurgeScheduler_GraduatedPurge(t *testing.T) {
	dir := t.TempDir()

	// Create a date that's 20 days old — profile should be purged, xlog should remain
	date := time.Now().AddDate(0, 0, -20).Format("20060102")
	xlogDir := filepath.Join(dir, date, "xlog")
	os.MkdirAll(xlogDir, 0755)
	os.WriteFile(filepath.Join(xlogDir, "xlog_prof.data"), []byte("prof"), 0644)
	os.WriteFile(filepath.Join(xlogDir, "xlog_prof.hfile"), []byte("hash"), 0644)
	os.WriteFile(filepath.Join(xlogDir, "xlog_prof.kfile"), []byte("key"), 0644)
	os.WriteFile(filepath.Join(xlogDir, "xlog.data"), []byte("xlog"), 0644)

	counterDir := filepath.Join(dir, date, "counter")
	os.MkdirAll(counterDir, 0755)
	os.WriteFile(filepath.Join(counterDir, "counter.data"), []byte("cnt"), 0644)

	sumDir := filepath.Join(dir, date, "summary")
	os.MkdirAll(sumDir, 0755)
	os.WriteFile(filepath.Join(sumDir, "sum.data"), []byte("sum"), 0644)

	// Profile=10, XLog=30, Sum=60, Counter=70
	scheduler := NewDataPurgeScheduler(dir, 10, 30, 60, 70)
	scheduler.purgeAll()

	// Profile files should be deleted (20 > 10)
	if _, err := os.Stat(filepath.Join(xlogDir, "xlog_prof.data")); !os.IsNotExist(err) {
		t.Error("profile data should be purged (20 > 10 days)")
	}
	// XLog files should remain (20 < 30)
	if _, err := os.Stat(filepath.Join(xlogDir, "xlog.data")); os.IsNotExist(err) {
		t.Error("xlog data should remain (20 < 30 days)")
	}
	// Counter should remain (20 < 70)
	if _, err := os.Stat(filepath.Join(counterDir, "counter.data")); os.IsNotExist(err) {
		t.Error("counter data should remain (20 < 70 days)")
	}
	// Summary should remain (20 < 60)
	if _, err := os.Stat(filepath.Join(sumDir, "sum.data")); os.IsNotExist(err) {
		t.Error("summary data should remain (20 < 60 days)")
	}
}
