package visitor

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// VisitorDB tracks daily visitor counts using HyperLogLog.
type VisitorDB struct {
	mu      sync.Mutex
	baseDir string
	date    string

	// objType -> HLL (total by type)
	typeHLLs map[string]*HLL
	// objHash -> HLL (per object)
	objHLLs map[int32]*HLL
}

// NewVisitorDB creates a new daily visitor database.
func NewVisitorDB(baseDir string) *VisitorDB {
	return &VisitorDB{
		baseDir:  baseDir,
		date:     time.Now().Format("20060102"),
		typeHLLs: make(map[string]*HLL),
		objHLLs:  make(map[int32]*HLL),
	}
}

// Offer records a visitor (userid) for the given object type and hash.
func (db *VisitorDB) Offer(objType string, objHash int32, userid int64) {
	db.mu.Lock()
	defer db.mu.Unlock()

	today := time.Now().Format("20060102")
	if today != db.date {
		db.flush()
		db.date = today
		db.typeHLLs = make(map[string]*HLL)
		db.objHLLs = make(map[int32]*HLL)
	}

	// Per-type HLL
	if _, ok := db.typeHLLs[objType]; !ok {
		db.typeHLLs[objType] = db.loadHLL(db.date, "type_"+objType)
	}
	db.typeHLLs[objType].Offer(userid)

	// Per-object HLL
	if _, ok := db.objHLLs[objHash]; !ok {
		db.objHLLs[objHash] = db.loadHLL(db.date, objHashKey(objHash))
	}
	db.objHLLs[objHash].Offer(userid)
}

// CountByType returns the visitor count for a given object type for today.
func (db *VisitorDB) CountByType(objType string) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if hll, ok := db.typeHLLs[objType]; ok {
		return hll.Count()
	}
	return 0
}

// CountByObj returns the visitor count for a given object hash for today.
func (db *VisitorDB) CountByObj(objHash int32) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if hll, ok := db.objHLLs[objHash]; ok {
		return hll.Count()
	}
	return 0
}

// CountByObjGroup returns the merged visitor count for a group of objects.
func (db *VisitorDB) CountByObjGroup(objHashes []int32) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	merged := NewHLL()
	for _, hash := range objHashes {
		if hll, ok := db.objHLLs[hash]; ok {
			merged.Merge(hll)
		}
	}
	return merged.Count()
}

// LoadDate loads historical visitor data for a specific date and object.
func (db *VisitorDB) LoadDate(date string, objHash int32) int64 {
	hll := db.loadHLL(date, objHashKey(objHash))
	return hll.Count()
}

// LoadDateTotal loads historical visitor data for a specific date and type.
func (db *VisitorDB) LoadDateTotal(date string, objType string) int64 {
	hll := db.loadHLL(date, "type_"+objType)
	return hll.Count()
}

// Flush writes all dirty HLLs to disk.
func (db *VisitorDB) Flush() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.flush()
}

func (db *VisitorDB) flush() {
	for name, hll := range db.typeHLLs {
		if hll.IsDirty() {
			db.saveHLL(db.date, "type_"+name, hll)
		}
	}
	for hash, hll := range db.objHLLs {
		if hll.IsDirty() {
			db.saveHLL(db.date, objHashKey(hash), hll)
		}
	}
}

// StartFlusher starts a background goroutine that flushes dirty data every 10 seconds.
func (db *VisitorDB) StartFlusher(done <-chan struct{}) {
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

func (db *VisitorDB) visitDir(date string) string {
	return filepath.Join(db.baseDir, date, "visit")
}

func (db *VisitorDB) loadHLL(date, name string) *HLL {
	hll := NewHLL()
	path := filepath.Join(db.visitDir(date), name+".usr")
	data, err := os.ReadFile(path)
	if err == nil && len(data) >= hllRegisters {
		hll.Deserialize(data)
	}
	return hll
}

func (db *VisitorDB) saveHLL(date, name string, hll *HLL) {
	dir := db.visitDir(date)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("VisitorDB: mkdir failed", "dir", dir, "error", err)
		return
	}
	path := filepath.Join(dir, name+".usr")
	data := hll.Serialize()
	if err := os.WriteFile(path, data, 0644); err != nil {
		slog.Error("VisitorDB: save failed", "path", path, "error", err)
	}
}

func objHashKey(hash int32) string {
	return "obj_" + itoa(int(hash))
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf) - 1
	for i > 0 {
		buf[pos] = byte('0' + i%10)
		pos--
		i /= 10
	}
	if neg {
		buf[pos] = '-'
		pos--
	}
	return string(buf[pos+1:])
}
