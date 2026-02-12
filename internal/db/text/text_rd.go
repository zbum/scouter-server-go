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
// All text data is read from a single "00000000" directory.
type TextRD struct {
	mu      sync.Mutex
	baseDir string
	table   *TextTable
	cache   map[cacheKey]string // in-memory cache
}

// NewTextRD creates a new text reader.
func NewTextRD(baseDir string) *TextRD {
	return &TextRD{
		baseDir: baseDir,
		cache:   make(map[cacheKey]string),
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

// Close closes the text table and clears the cache.
func (r *TextRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.table != nil {
		r.table.Close()
		r.table = nil
	}
	r.cache = make(map[cacheKey]string)
}
