package xlog

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/util"
)

// XLogEntry represents a single XLog entry to be written.
type XLogEntry struct {
	Time    int64
	Txid    int64
	Gxid    int64
	Elapsed int32
	Data    []byte // pre-serialized XLogPack bytes
}

const batchSize = 512 // max entries per batch drain

// XLogWR is an async XLog writer with per-day containers.
// Entries are drained from the queue in batches and flushed together,
// reducing the number of disk I/O syscalls under high write load.
type XLogWR struct {
	mu      sync.Mutex
	baseDir string
	days    map[string]*dayContainer
	queue   chan *XLogEntry
}

type dayContainer struct {
	index *XLogIndex
	data  *XLogData
}

// NewXLogWR creates a new XLog writer.
func NewXLogWR(baseDir string) *XLogWR {
	return &XLogWR{
		baseDir: baseDir,
		days:    make(map[string]*dayContainer),
		queue:   make(chan *XLogEntry, 10000),
	}
}

// Start begins the background processing goroutine.
// Entries are drained in batches: the first entry blocks, then remaining
// queued entries are drained non-blocking up to batchSize. After the batch
// is processed, data files are flushed once.
func (w *XLogWR) Start(ctx context.Context) {
	go func() {
		batch := make([]*XLogEntry, 0, batchSize)
		for {
			// Block until first entry arrives
			select {
			case <-ctx.Done():
				return
			case entry := <-w.queue:
				if entry != nil {
					batch = append(batch, entry)
				}
			}

			// Drain remaining queued entries (non-blocking)
			for len(batch) < batchSize {
				select {
				case entry := <-w.queue:
					if entry != nil {
						batch = append(batch, entry)
					}
				default:
					goto processBatch
				}
			}

		processBatch:
			for _, e := range batch {
				w.process(e)
			}

			if len(batch) > 0 {
				w.flushData()
			}
			batch = batch[:0]
		}
	}()
}

// Add enqueues an XLog entry for async writing.
func (w *XLogWR) Add(entry *XLogEntry) {
	select {
	case w.queue <- entry:
	default:
		// Queue full, drop entry (could log warning here)
	}
}

// getContainer retrieves or creates a day container.
func (w *XLogWR) getContainer(date string) (*dayContainer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	container, exists := w.days[date]
	if exists {
		return container, nil
	}

	// Create directory structure: {baseDir}/{YYYYMMDD}/xlog/
	dir := filepath.Join(w.baseDir, date, "xlog")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Open index and data files
	index, err := NewXLogIndex(dir)
	if err != nil {
		return nil, err
	}

	data, err := NewXLogData(dir)
	if err != nil {
		index.Close()
		return nil, err
	}

	container = &dayContainer{
		index: index,
		data:  data,
	}
	w.days[date] = container
	return container, nil
}

// flushData flushes buffered data for all active day containers.
func (w *XLogWR) flushData() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.days {
		if c.data != nil {
			c.data.Flush()
		}
	}
}

// process writes an XLog entry to disk with triple indexing.
func (w *XLogWR) process(entry *XLogEntry) {
	date := util.FormatDate(entry.Time)
	container, err := w.getContainer(date)
	if err != nil {
		return
	}

	// Write data
	dataPos, err := container.data.Write(entry.Data)
	if err != nil {
		return
	}

	// Index by time
	if err := container.index.SetByTime(entry.Time, dataPos); err != nil {
		return
	}

	// Index by txid
	if err := container.index.SetByTxid(entry.Txid, dataPos); err != nil {
		return
	}

	// Index by gxid (if non-zero)
	if err := container.index.SetByGxid(entry.Gxid, dataPos); err != nil {
		return
	}
}

// PurgeOldDays closes day containers not in the keepDates set.
func (w *XLogWR) PurgeOldDays(keepDates map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for date, container := range w.days {
		if keepDates[date] {
			continue
		}
		if container.data != nil {
			container.data.Flush()
			container.data.Close()
		}
		if container.index != nil {
			container.index.Close()
		}
		delete(w.days, date)
	}
}

// Close closes all open day containers.
func (w *XLogWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, container := range w.days {
		if container.data != nil {
			container.data.Flush()
			container.data.Close()
		}
		if container.index != nil {
			container.index.Close()
		}
	}
	w.days = make(map[string]*dayContainer)
}
