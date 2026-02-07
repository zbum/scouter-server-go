package counter

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// RealtimeEntry represents a single counter write for realtime storage.
type RealtimeEntry struct {
	TimeMs   int64
	ObjHash  int32
	Counters map[string]value.Value
}

// DailyEntry represents a single counter write for daily 5-min storage.
type DailyEntry struct {
	Date        string
	ObjHash     int32
	CounterName string
	Bucket      int
	Value       float64
}

// CounterWR manages async writing of both realtime and daily counters.
type CounterWR struct {
	mu          sync.Mutex
	baseDir     string
	realtimeDays map[string]*RealtimeCounterData
	dailyDays    map[string]*DailyCounterData
	rtQueue     chan *RealtimeEntry
	dailyQueue  chan *DailyEntry
}

func NewCounterWR(baseDir string) *CounterWR {
	return &CounterWR{
		baseDir:      baseDir,
		realtimeDays: make(map[string]*RealtimeCounterData),
		dailyDays:    make(map[string]*DailyCounterData),
		rtQueue:      make(chan *RealtimeEntry, 10000),
		dailyQueue:   make(chan *DailyEntry, 10000),
	}
}

// Start begins background goroutines for processing both queues.
func (w *CounterWR) Start(ctx context.Context) {
	go w.processRealtime(ctx)
	go w.processDaily(ctx)
}

// AddRealtime queues a realtime counter entry.
func (w *CounterWR) AddRealtime(entry *RealtimeEntry) {
	select {
	case w.rtQueue <- entry:
	default:
		slog.Debug("CounterWR: realtime queue full, dropping")
	}
}

// AddDaily queues a daily counter entry.
func (w *CounterWR) AddDaily(entry *DailyEntry) {
	select {
	case w.dailyQueue <- entry:
	default:
		slog.Debug("CounterWR: daily queue full, dropping")
	}
}

// AddRealtimeFromPerfCounter is a convenience that creates a RealtimeEntry from
// common parameters and queues it.
func (w *CounterWR) AddRealtimeFromPerfCounter(timeMs int64, objHash int32, counters map[string]value.Value) {
	w.AddRealtime(&RealtimeEntry{
		TimeMs:   timeMs,
		ObjHash:  objHash,
		Counters: counters,
	})
}

func (w *CounterWR) processRealtime(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.flushAll()
			return
		case entry := <-w.rtQueue:
			w.writeRealtime(entry)
		}
	}
}

func (w *CounterWR) processDaily(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-w.dailyQueue:
			w.writeDaily(entry)
		}
	}
}

func (w *CounterWR) writeRealtime(entry *RealtimeEntry) {
	date := util.FormatDate(entry.TimeMs)
	t := time.UnixMilli(entry.TimeMs)
	timeSec := int32(t.Hour()*3600 + t.Minute()*60 + t.Second())

	data, err := w.getRealtimeData(date)
	if err != nil {
		slog.Error("CounterWR: open realtime data error", "date", date, "error", err)
		return
	}

	if err := data.Write(entry.ObjHash, timeSec, entry.Counters); err != nil {
		slog.Error("CounterWR: write realtime error", "error", err)
	}
}

func (w *CounterWR) writeDaily(entry *DailyEntry) {
	data, err := w.getDailyData(entry.Date)
	if err != nil {
		slog.Error("CounterWR: open daily data error", "date", entry.Date, "error", err)
		return
	}

	if err := data.Write(entry.ObjHash, entry.CounterName, entry.Bucket, entry.Value); err != nil {
		slog.Error("CounterWR: write daily error", "error", err)
	}
}

func (w *CounterWR) getRealtimeData(date string) (*RealtimeCounterData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if d, ok := w.realtimeDays[date]; ok {
		return d, nil
	}

	dir := filepath.Join(w.baseDir, date, "counter")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	d, err := NewRealtimeCounterData(dir)
	if err != nil {
		return nil, err
	}
	w.realtimeDays[date] = d
	return d, nil
}

func (w *CounterWR) getDailyData(date string) (*DailyCounterData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if d, ok := w.dailyDays[date]; ok {
		return d, nil
	}

	dir := filepath.Join(w.baseDir, date, "counter")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	d, err := NewDailyCounterData(dir)
	if err != nil {
		return nil, err
	}
	w.dailyDays[date] = d
	return d, nil
}

// PurgeOldDays closes day containers not in the keepDates set.
func (w *CounterWR) PurgeOldDays(keepDates map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for date, d := range w.realtimeDays {
		if keepDates[date] {
			continue
		}
		d.Flush()
		d.Close()
		delete(w.realtimeDays, date)
	}
	for date, d := range w.dailyDays {
		if keepDates[date] {
			continue
		}
		d.Close()
		delete(w.dailyDays, date)
	}
}

func (w *CounterWR) flushAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.realtimeDays {
		d.Flush()
	}
}

// Close closes all open data files.
func (w *CounterWR) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.realtimeDays {
		d.Close()
	}
	for _, d := range w.dailyDays {
		d.Close()
	}
}
