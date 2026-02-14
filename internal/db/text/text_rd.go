package text

import (
	"path/filepath"
	"sync"
)

// cacheKey uniquely identifies a cached text entry.
type cacheKey struct {
	Div  string
	Hash int32
}

// TextRD provides text reading with caching.
// Permanent text is read from "00000000/text/" using TextPermTable (per-div files with .data).
// Daily text is read from per-date directories using TextTable (single file with composite key).
type TextRD struct {
	mu          sync.RWMutex
	baseDir     string
	table       *TextPermTable
	dailyTables map[string]*TextTable // date → TextTable for daily text
	cache       map[cacheKey]string   // in-memory cache
}

// NewTextRD creates a new text reader.
func NewTextRD(baseDir string) *TextRD {
	return &TextRD{
		baseDir:     baseDir,
		dailyTables: make(map[string]*TextTable),
		cache:       make(map[cacheKey]string),
	}
}

// GetString retrieves a text string by div and hash from permanent storage.
// Checks cache first, then reads from the text table.
func (r *TextRD) GetString(div string, hash int32) (string, error) {
	key := cacheKey{
		Div:  div,
		Hash: hash,
	}

	// Fast path: check cache with read lock
	r.mu.RLock()
	if text, ok := r.cache[key]; ok {
		r.mu.RUnlock()
		return text, nil
	}
	r.mu.RUnlock()

	// Slow path: write lock for cache miss
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check cache after acquiring write lock
	if text, ok := r.cache[key]; ok {
		return text, nil
	}

	// Get table
	table, err := r.getTable()
	if err != nil {
		return "", err
	}

	// Read from table
	text, found, err := table.Get(div, hash)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}

	// Cache the result
	r.cache[key] = text
	return text, nil
}

// getTable returns the permanent text table, opening it if necessary.
func (r *TextRD) getTable() (*TextPermTable, error) {
	if r.table != nil {
		return r.table, nil
	}

	dir := filepath.Join(r.baseDir, textDirName, "text")
	table, err := NewTextPermTable(dir)
	if err != nil {
		return nil, err
	}

	r.table = table
	return table, nil
}

// GetDailyString reads a text from a date-specific directory.
func (r *TextRD) GetDailyString(date, div string, hash int32) (string, error) {
	// Fast path: read lock to check existing table
	r.mu.RLock()
	table, ok := r.dailyTables[date]
	r.mu.RUnlock()

	if !ok {
		// Slow path: write lock to create table
		r.mu.Lock()
		var err error
		table, err = r.getDailyTable(date)
		if err != nil {
			r.mu.Unlock()
			return "", err
		}
		r.mu.Unlock()
	}

	// table.Get has its own mutex
	text, found, err := table.Get(div, hash)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return text, nil
}

// getDailyTable returns the TextTable for a specific date, creating it if necessary.
// Caller must hold the write lock.
func (r *TextRD) getDailyTable(date string) (*TextTable, error) {
	if table, ok := r.dailyTables[date]; ok {
		return table, nil
	}

	dir := filepath.Join(r.baseDir, date, "text")
	table, err := NewTextTable(dir)
	if err != nil {
		return nil, err
	}

	r.dailyTables[date] = table
	return table, nil
}

// Close closes the text table and clears the cache.
func (r *TextRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.table != nil {
		r.table.Close()
		r.table = nil
	}
	for _, t := range r.dailyTables {
		t.Close()
	}
	r.dailyTables = make(map[string]*TextTable)
	r.cache = make(map[cacheKey]string)
}
