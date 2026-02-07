package alert

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/util"
)

// AlertEntry represents a single alert entry to be written.
type AlertEntry struct {
	TimeMs int64
	Data   []byte // pre-serialized AlertPack
}

// AlertWR is an async alert writer with per-day containers.
type AlertWR struct {
	mu      sync.Mutex
	baseDir string
	days    map[string]*AlertData
	queue   chan *AlertEntry
}

// NewAlertWR creates a new alert writer.
func NewAlertWR(baseDir string) *AlertWR {
	return &AlertWR{
		baseDir: baseDir,
		days:    make(map[string]*AlertData),
		queue:   make(chan *AlertEntry, 10000),
	}
}

// Start begins the background processing goroutine.
func (w *AlertWR) Start(ctx context.Context) {
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

// Add enqueues an alert entry for async writing.
func (w *AlertWR) Add(entry *AlertEntry) {
	select {
	case w.queue <- entry:
	default:
		slog.Warn("AlertWR queue full, dropping entry")
	}
}

// getContainer retrieves or creates a day container.
func (w *AlertWR) getContainer(date string) (*AlertData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	container, exists := w.days[date]
	if exists {
		return container, nil
	}

	// Create directory structure: {baseDir}/{YYYYMMDD}/alert/
	dir := filepath.Join(w.baseDir, date, "alert")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	ad, err := NewAlertData(dir)
	if err != nil {
		return nil, err
	}

	w.days[date] = ad
	return ad, nil
}

// process writes an alert entry to disk.
func (w *AlertWR) process(entry *AlertEntry) {
	date := util.FormatDate(entry.TimeMs)
	container, err := w.getContainer(date)
	if err != nil {
		slog.Error("AlertWR getContainer error", "error", err)
		return
	}

	if err := container.Write(entry.TimeMs, entry.Data); err != nil {
		slog.Error("AlertWR write error", "error", err)
	}
}

// PurgeOldDays closes day containers not in the keepDates set.
func (w *AlertWR) PurgeOldDays(keepDates map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for date, ad := range w.days {
		if keepDates[date] {
			continue
		}
		if ad != nil {
			ad.Flush()
			ad.Close()
		}
		delete(w.days, date)
	}
}

// Close closes all open day containers.
func (w *AlertWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, ad := range w.days {
		if ad != nil {
			ad.Flush()
			ad.Close()
		}
	}
	w.days = make(map[string]*AlertData)
}
