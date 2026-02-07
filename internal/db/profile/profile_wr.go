package profile

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/util"
)

// ProfileEntry represents a single profile block to be written.
type ProfileEntry struct {
	TimeMs int64
	Txid   int64
	Data   []byte // pre-serialized step data
}

// ProfileWR manages async writing of profile data.
type ProfileWR struct {
	mu      sync.Mutex
	baseDir string
	days    map[string]*ProfileData
	queue   chan *ProfileEntry
}

func NewProfileWR(baseDir string) *ProfileWR {
	return &ProfileWR{
		baseDir: baseDir,
		days:    make(map[string]*ProfileData),
		queue:   make(chan *ProfileEntry, 10000),
	}
}

// Start begins the background processing goroutine.
func (w *ProfileWR) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				w.flushAll()
				return
			case entry := <-w.queue:
				w.process(entry)
			}
		}
	}()
}

// Add queues a profile entry for async writing.
func (w *ProfileWR) Add(entry *ProfileEntry) {
	select {
	case w.queue <- entry:
	default:
		slog.Debug("ProfileWR: queue full, dropping")
	}
}

func (w *ProfileWR) process(entry *ProfileEntry) {
	date := util.FormatDate(entry.TimeMs)
	data, err := w.getData(date)
	if err != nil {
		slog.Error("ProfileWR: open error", "date", date, "error", err)
		return
	}

	if err := data.Write(entry.Txid, entry.Data); err != nil {
		slog.Error("ProfileWR: write error", "error", err)
	}
}

func (w *ProfileWR) getData(date string) (*ProfileData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if d, ok := w.days[date]; ok {
		return d, nil
	}

	dir := filepath.Join(w.baseDir, date, "xlog")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	d, err := NewProfileData(dir)
	if err != nil {
		return nil, err
	}
	w.days[date] = d
	return d, nil
}

// PurgeOldDays closes day containers not in the keepDates set.
func (w *ProfileWR) PurgeOldDays(keepDates map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for date, d := range w.days {
		if keepDates[date] {
			continue
		}
		d.Flush()
		d.Close()
		delete(w.days, date)
	}
}

func (w *ProfileWR) flushAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.days {
		d.Flush()
	}
}

// Close closes all open data files.
func (w *ProfileWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.days {
		d.Close()
	}
}
