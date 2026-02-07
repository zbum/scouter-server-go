package text

import (
	"path/filepath"
	"sync"
)

// cacheKey uniquely identifies a cached text entry.
type cacheKey struct {
	Date string
	Div  string
	Hash int32
}

// TextRD provides text reading with caching.
type TextRD struct {
	mu      sync.Mutex
	baseDir string
	tables  map[string]*TextTable // date -> table
	cache   map[cacheKey]string   // in-memory cache
}

// NewTextRD creates a new text reader.
func NewTextRD(baseDir string) *TextRD {
	return &TextRD{
		baseDir: baseDir,
		tables:  make(map[string]*TextTable),
		cache:   make(map[cacheKey]string),
	}
}

// GetString retrieves a text string by date, div, and hash.
// Checks cache first, then reads from table.
func (r *TextRD) GetString(date, div string, hash int32) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check cache
	key := cacheKey{
		Date: date,
		Div:  div,
		Hash: hash,
	}
	if text, ok := r.cache[key]; ok {
		return text, nil
	}

	// Get table
	table, err := r.getTable(date)
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

// getTable returns a table for the given date, opening it if necessary.
func (r *TextRD) getTable(date string) (*TextTable, error) {
	if table, ok := r.tables[date]; ok {
		return table, nil
	}

	dir := filepath.Join(r.baseDir, date)
	table, err := NewTextTable(dir)
	if err != nil {
		return nil, err
	}

	r.tables[date] = table
	return table, nil
}

// PurgeOldDays closes day containers and clears cache entries not in the keepDates set.
func (r *TextRD) PurgeOldDays(keepDates map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for date, table := range r.tables {
		if keepDates[date] {
			continue
		}
		table.Close()
		delete(r.tables, date)
	}
	for key := range r.cache {
		if !keepDates[key.Date] {
			delete(r.cache, key)
		}
	}
}

// Close closes all open tables and clears the cache.
func (r *TextRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, table := range r.tables {
		table.Close()
	}
	r.tables = make(map[string]*TextTable)
	r.cache = make(map[cacheKey]string)
}
