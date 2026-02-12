package visitor

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// VisitorHourlyDB tracks hourly visitor counts using HyperLogLog.
type VisitorHourlyDB struct {
	mu      sync.Mutex
	baseDir string
	date    string
	hour    int

	// objHash -> HLL (per object per hour)
	objHLLs map[int32]*HLL
}

// NewVisitorHourlyDB creates a new hourly visitor database.
func NewVisitorHourlyDB(baseDir string) *VisitorHourlyDB {
	now := time.Now()
	return &VisitorHourlyDB{
		baseDir: baseDir,
		date:    now.Format("20060102"),
		hour:    now.Hour(),
		objHLLs: make(map[int32]*HLL),
	}
}

// Offer records a visitor for the current hour.
func (db *VisitorHourlyDB) Offer(objHash int32, userid int64) {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now()
	today := now.Format("20060102")
	currentHour := now.Hour()

	if today != db.date || currentHour != db.hour {
		db.flush()
		db.date = today
		db.hour = currentHour
		db.objHLLs = make(map[int32]*HLL)
	}

	if _, ok := db.objHLLs[objHash]; !ok {
		db.objHLLs[objHash] = db.loadHLL(db.date, objHash, db.hour)
	}
	db.objHLLs[objHash].Offer(userid)
}

// LoadHour loads the visitor count for a specific date, object, and hour.
func (db *VisitorHourlyDB) LoadHour(date string, objHash int32, hour int) int64 {
	hll := db.loadHLL(date, objHash, hour)
	return hll.Count()
}

// LoadAllHours loads visitor counts for all 24 hours of a date for a group of objects.
func (db *VisitorHourlyDB) LoadAllHours(date string, objHashes []int32) [24]int64 {
	var result [24]int64
	for hour := 0; hour < 24; hour++ {
		merged := NewHLL()
		for _, hash := range objHashes {
			hll := db.loadHLL(date, hash, hour)
			merged.Merge(hll)
		}
		result[hour] = merged.Count()
	}
	return result
}

// Flush writes all dirty HLLs to disk.
func (db *VisitorHourlyDB) Flush() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.flush()
}

func (db *VisitorHourlyDB) flush() {
	for hash, hll := range db.objHLLs {
		if hll.IsDirty() {
			db.saveHLL(db.date, hash, db.hour, hll)
		}
	}
}

// StartFlusher starts a background goroutine that flushes dirty data every 10 seconds.
func (db *VisitorHourlyDB) StartFlusher(done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				db.Flush()
				return
			case <-ticker.C:
				db.Flush()
			}
		}
	}()
}

func (db *VisitorHourlyDB) hourlyDir(date string) string {
	return filepath.Join(db.baseDir, date, "visit_hourly")
}

func (db *VisitorHourlyDB) loadHLL(date string, objHash int32, hour int) *HLL {
	hll := NewHLL()
	path := filepath.Join(db.hourlyDir(date), fmt.Sprintf("obj_%d_%02d.usr", objHash, hour))
	data, err := os.ReadFile(path)
	if err == nil && len(data) >= hllRegisters {
		hll.Deserialize(data)
	}
	return hll
}

func (db *VisitorHourlyDB) saveHLL(date string, objHash int32, hour int, hll *HLL) {
	dir := db.hourlyDir(date)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("VisitorHourlyDB: mkdir failed", "dir", dir, "error", err)
		return
	}
	path := filepath.Join(dir, fmt.Sprintf("obj_%d_%02d.usr", objHash, hour))
	data := hll.Serialize()
	if err := os.WriteFile(path, data, 0644); err != nil {
		slog.Error("VisitorHourlyDB: save failed", "path", path, "error", err)
	}
}
