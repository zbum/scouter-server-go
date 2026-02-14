package text

import (
	"context"
	"path/filepath"
	"sync"
)

const textDirName = "00000000"

// TextData represents a text record to be written.
type TextData struct {
	Div  string
	Hash int32
	Text string
}

// dupKey uniquely identifies a text entry for deduplication.
type dupKey struct {
	Div  string
	Hash int32
}

// TextWR provides async text writing with deduplication.
// Permanent text is stored in "00000000/text/" using TextPermTable (per-div files with .data).
// Daily text is stored in per-date directories using TextTable (single file with composite key).
type TextWR struct {
	mu          sync.RWMutex
	baseDir     string
	table       *TextPermTable
	dailyTables map[string]*TextTable // date → TextTable for daily text
	dupCheck    map[dupKey]struct{}   // in-memory dedup cache
	queue       chan *TextData
	closed      bool
	wg          sync.WaitGroup
}

// NewTextWR creates a new async text writer.
func NewTextWR(baseDir string) *TextWR {
	return &TextWR{
		baseDir:     baseDir,
		dailyTables: make(map[string]*TextTable),
		dupCheck:    make(map[dupKey]struct{}),
		queue:       make(chan *TextData, 10000),
	}
}

// Start begins the background goroutine that processes the write queue.
func (w *TextWR) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-w.queue:
				if !ok {
					return
				}
				w.process(data)
				w.wg.Done()
			}
		}
	}()
}

// Add enqueues a text record for async writing.
// Non-blocking unless queue is full.
func (w *TextWR) Add(div string, hash int32, text string) {
	w.wg.Add(1)
	select {
	case w.queue <- &TextData{
		Div:  div,
		Hash: hash,
		Text: text,
	}:
	default:
		// Queue full, drop the record
		w.wg.Done()
	}
}

// process handles a single text write with deduplication.
func (w *TextWR) process(data *TextData) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check dedup cache
	key := dupKey{
		Div:  data.Div,
		Hash: data.Hash,
	}
	if _, exists := w.dupCheck[key]; exists {
		return
	}

	// Get or create table
	table, err := w.getTable()
	if err != nil {
		return
	}

	// Write to table
	if err := table.Set(data.Div, data.Hash, data.Text); err != nil {
		return
	}

	// Mark as written
	w.dupCheck[key] = struct{}{}
}

// getTable returns the permanent text table, opening it if necessary.
func (w *TextWR) getTable() (*TextPermTable, error) {
	if w.table != nil {
		return w.table, nil
	}

	dir := filepath.Join(w.baseDir, textDirName, "text")
	table, err := NewTextPermTable(dir)
	if err != nil {
		return nil, err
	}

	w.table = table
	return table, nil
}

// GetString reads a text from the writer's TextPermTable (which has the up-to-date index).
// This is needed because TextRD has a stale MemHashBlock that can't see data
// written after it was opened.
func (w *TextWR) GetString(div string, hash int32) (string, error) {
	// Fast path: read lock if table already exists
	w.mu.RLock()
	table := w.table
	w.mu.RUnlock()

	if table == nil {
		// Slow path: write lock to create table
		w.mu.Lock()
		var err error
		table, err = w.getTable()
		if err != nil {
			w.mu.Unlock()
			return "", err
		}
		w.mu.Unlock()
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

// AddDaily stores a text entry in a date-specific directory (for daily text types like ERROR).
func (w *TextWR) AddDaily(date, div string, hash int32, text string) {
	// Fast path: read lock to check existing table
	w.mu.RLock()
	table, ok := w.dailyTables[date]
	w.mu.RUnlock()

	if !ok {
		// Slow path: write lock to create table
		w.mu.Lock()
		var err error
		table, err = w.getDailyTable(date)
		if err != nil {
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
	}

	// table.Set has its own mutex
	table.Set(div, hash, text)
}

// GetDailyString reads a text from a date-specific directory.
func (w *TextWR) GetDailyString(date, div string, hash int32) (string, error) {
	// Fast path: read lock to check existing table
	w.mu.RLock()
	table, ok := w.dailyTables[date]
	w.mu.RUnlock()

	if !ok {
		// Slow path: write lock to create table
		w.mu.Lock()
		var err error
		table, err = w.getDailyTable(date)
		if err != nil {
			w.mu.Unlock()
			return "", err
		}
		w.mu.Unlock()
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
func (w *TextWR) getDailyTable(date string) (*TextTable, error) {
	if table, ok := w.dailyTables[date]; ok {
		return table, nil
	}

	dir := filepath.Join(w.baseDir, date, "text")
	table, err := NewTextTable(dir)
	if err != nil {
		return nil, err
	}

	w.dailyTables[date] = table
	return table, nil
}

// Flush waits for all pending writes to complete.
func (w *TextWR) Flush() {
	w.wg.Wait()
}

// Close closes the text table and stops accepting new writes.
func (w *TextWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}
	w.closed = true

	close(w.queue)
	if w.table != nil {
		w.table.Close()
		w.table = nil
	}
	for _, t := range w.dailyTables {
		t.Close()
	}
	w.dailyTables = make(map[string]*TextTable)
	w.dupCheck = make(map[dupKey]struct{})
}
