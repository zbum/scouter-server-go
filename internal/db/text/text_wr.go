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
// All text data is stored in a single "00000000" directory.
type TextWR struct {
	mu       sync.Mutex
	baseDir  string
	table    *TextTable
	dupCheck map[dupKey]struct{} // in-memory dedup cache
	queue    chan *TextData
	closed   bool
	wg       sync.WaitGroup
}

// NewTextWR creates a new async text writer.
func NewTextWR(baseDir string) *TextWR {
	return &TextWR{
		baseDir:  baseDir,
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

// getTable returns the single text table, opening it if necessary.
func (w *TextWR) getTable() (*TextTable, error) {
	if w.table != nil {
		return w.table, nil
	}

	dir := filepath.Join(w.baseDir, textDirName)
	table, err := NewTextTable(dir)
	if err != nil {
		return nil, err
	}

	w.table = table
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
	w.dupCheck = make(map[dupKey]struct{})
}
