package text

import (
	"context"
	"path/filepath"
	"sync"
)

// TextData represents a text record to be written.
type TextData struct {
	Date string
	Div  string
	Hash int32
	Text string
}

// dupKey uniquely identifies a text entry for deduplication.
type dupKey struct {
	Date string
	Div  string
	Hash int32
}

// TextWR provides async text writing with deduplication.
type TextWR struct {
	mu       sync.Mutex
	baseDir  string
	tables   map[string]*TextTable // date -> table
	dupCheck map[dupKey]struct{}   // in-memory dedup cache
	queue    chan *TextData
	closed   bool
	wg       sync.WaitGroup
}

// NewTextWR creates a new async text writer.
func NewTextWR(baseDir string) *TextWR {
	return &TextWR{
		baseDir:  baseDir,
		tables:   make(map[string]*TextTable),
		dupCheck: make(map[dupKey]struct{}),
		queue:    make(chan *TextData, 10000),
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
func (w *TextWR) Add(date, div string, hash int32, text string) {
	w.wg.Add(1)
	select {
	case w.queue <- &TextData{
		Date: date,
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
		Date: data.Date,
		Div:  data.Div,
		Hash: data.Hash,
	}
	if _, exists := w.dupCheck[key]; exists {
		return
	}

	// Get or create table
	table, err := w.getTable(data.Date)
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

// getTable returns a table for the given date, opening it if necessary.
func (w *TextWR) getTable(date string) (*TextTable, error) {
	if table, ok := w.tables[date]; ok {
		return table, nil
	}

	dir := filepath.Join(w.baseDir, date)
	table, err := NewTextTable(dir)
	if err != nil {
		return nil, err
	}

	w.tables[date] = table
	return table, nil
}

// PurgeOldDays closes day containers and clears dedup entries not in the keepDates set.
func (w *TextWR) PurgeOldDays(keepDates map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for date, table := range w.tables {
		if keepDates[date] {
			continue
		}
		table.Close()
		delete(w.tables, date)
	}
	for key := range w.dupCheck {
		if !keepDates[key.Date] {
			delete(w.dupCheck, key)
		}
	}
}

// Flush waits for all pending writes to complete.
func (w *TextWR) Flush() {
	w.wg.Wait()
}

// Close closes all open tables and stops accepting new writes.
func (w *TextWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}
	w.closed = true

	close(w.queue)
	for _, table := range w.tables {
		table.Close()
	}
	w.tables = make(map[string]*TextTable)
	w.dupCheck = make(map[dupKey]struct{})
}
