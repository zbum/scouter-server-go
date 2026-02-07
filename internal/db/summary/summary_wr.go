package summary

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/util"
)

// SummaryEntry represents a single summary entry to be written.
type SummaryEntry struct {
	TimeMs int64
	SType  byte
	Data   []byte // pre-serialized SummaryPack
}

// dayKey is a composite key for date + summary type.
type dayKey struct {
	date  string
	stype byte
}

// SummaryWR is an async summary writer with per-day and per-type containers.
type SummaryWR struct {
	mu      sync.Mutex
	baseDir string
	days    map[dayKey]*SummaryData
	queue   chan *SummaryEntry
}

// NewSummaryWR creates a new summary writer.
func NewSummaryWR(baseDir string) *SummaryWR {
	return &SummaryWR{
		baseDir: baseDir,
		days:    make(map[dayKey]*SummaryData),
		queue:   make(chan *SummaryEntry, 10000),
	}
}

// Start begins the background processing goroutine.
func (w *SummaryWR) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-w.queue:
				if entry != nil {
					w.process(entry)
				}
			}
		}
	}()
}

// Add enqueues a summary entry for async writing.
func (w *SummaryWR) Add(entry *SummaryEntry) {
	select {
	case w.queue <- entry:
	default:
		slog.Warn("SummaryWR queue full, dropping entry")
	}
}

// getContainer retrieves or creates a day+type container.
func (w *SummaryWR) getContainer(date string, stype byte) (*SummaryData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := dayKey{date: date, stype: stype}
	container, exists := w.days[key]
	if exists {
		return container, nil
	}

	// Create directory structure: {baseDir}/{YYYYMMDD}/summary/
	dir := filepath.Join(w.baseDir, date, "summary")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	sd, err := NewSummaryData(dir, stype)
	if err != nil {
		return nil, err
	}

	w.days[key] = sd
	return sd, nil
}

// process writes a summary entry to disk.
func (w *SummaryWR) process(entry *SummaryEntry) {
	date := util.FormatDate(entry.TimeMs)
	container, err := w.getContainer(date, entry.SType)
	if err != nil {
		slog.Error("SummaryWR getContainer error", "error", err)
		return
	}

	if err := container.Write(entry.TimeMs, entry.Data); err != nil {
		slog.Error("SummaryWR write error", "error", err)
	}
}

// PurgeOldDays closes day containers not in the keepDates set.
func (w *SummaryWR) PurgeOldDays(keepDates map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for key, sd := range w.days {
		if keepDates[key.date] {
			continue
		}
		if sd != nil {
			sd.Flush()
			sd.Close()
		}
		delete(w.days, key)
	}
}

// Close closes all open day containers.
func (w *SummaryWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, sd := range w.days {
		if sd != nil {
			sd.Flush()
			sd.Close()
		}
	}
	w.days = make(map[dayKey]*SummaryData)
}
