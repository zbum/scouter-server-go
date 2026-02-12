package text

import (
	"path/filepath"
	"sync"
)

// dailyTextTypes lists the text types that can use daily storage.
var dailyTextTypes = map[string]bool{
	"service": true,
	"apicall": true,
	"ua":      true,
}

// cacheKey uniquely identifies a cached text entry.
type cacheKey struct {
	Div  string
	Hash int32
}

// TextRD provides text reading with caching.
// All text data is read from a single "00000000" directory.
type TextRD struct {
	mu          sync.Mutex
	baseDir     string
	table       *TextTable
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

// GetString retrieves a text string by div and hash.
// Checks cache first, then reads from the text table.
func (r *TextRD) GetString(div string, hash int32) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check cache
	key := cacheKey{
		Div:  div,
		Hash: hash,
	}
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

// getTable returns the single text table, opening it if necessary.
func (r *TextRD) getTable() (*TextTable, error) {
	if r.table != nil {
		return r.table, nil
	}

	dir := filepath.Join(r.baseDir, textDirName)
	table, err := NewTextTable(dir)
	if err != nil {
		return nil, err
	}

	r.table = table
	return table, nil
}

// GetDailyString reads a text from a date-specific directory.
func (r *TextRD) GetDailyString(date, div string, hash int32) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	table, err := r.getDailyTable(date)
	if err != nil {
		return "", err
	}

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
