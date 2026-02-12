package tagcnt

import (
	"log/slog"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

const (
	maxTopN = 100
)

// TagCountCore processes tag counting from XLog data asynchronously.
type TagCountCore struct {
	mu    sync.Mutex
	store *Store
	queue chan *tagEntry

	// In-memory counters: date → tagKey → tagValue → [24]float64
	data     map[string]map[string]map[int32]*hourlyCounter
	lastDate string
}

type tagEntry struct {
	objType string
	xp      *pack.XLogPack
}

type hourlyCounter struct {
	counts [24]float64
}

// NewTagCountCore creates a new tag counting processor.
func NewTagCountCore(baseDir string) *TagCountCore {
	tc := &TagCountCore{
		store:    NewStore(baseDir),
		queue:    make(chan *tagEntry, 4096),
		data:     make(map[string]map[string]map[int32]*hourlyCounter),
		lastDate: time.Now().Format("20060102"),
	}
	go tc.run()
	go tc.flusher()
	return tc
}

// ProcessXLog queues an XLog for tag counting.
func (tc *TagCountCore) ProcessXLog(objType string, xp *pack.XLogPack) {
	select {
	case tc.queue <- &tagEntry{objType: objType, xp: xp}:
	default:
		slog.Debug("TagCountCore queue overflow")
	}
}

func (tc *TagCountCore) run() {
	for entry := range tc.queue {
		tc.process(entry)
	}
}

func (tc *TagCountCore) process(entry *tagEntry) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	xp := entry.xp
	date := time.UnixMilli(xp.EndTime).Format("20060102")
	hour := time.UnixMilli(xp.EndTime).Hour()

	// Reset on date change
	if date != tc.lastDate {
		tc.flushLocked()
		tc.data = make(map[string]map[string]map[int32]*hourlyCounter)
		tc.lastDate = date
	}

	dateData, ok := tc.data[date]
	if !ok {
		dateData = make(map[string]map[int32]*hourlyCounter)
		tc.data[date] = dateData
	}

	// service.total: count by objType total
	tc.increment(dateData, TagGroupService+"."+TagKeyTotal, 0, hour, 1)

	// service.service: count by service hash
	if xp.Service != 0 {
		tc.increment(dateData, TagGroupService+"."+TagKeyService, xp.Service, hour, 1)
	}

	// error.total: count errors
	if xp.Error != 0 {
		tc.increment(dateData, TagGroupError+"."+TagKeyTotal, 0, hour, 1)
		tc.increment(dateData, TagGroupError+"."+TagKeyError, xp.Error, hour, 1)
	}
}

func (tc *TagCountCore) increment(dateData map[string]map[int32]*hourlyCounter, tagKey string, tagValue int32, hour int, delta float64) {
	keyData, ok := dateData[tagKey]
	if !ok {
		keyData = make(map[int32]*hourlyCounter)
		dateData[tagKey] = keyData
	}

	// Top-N limit per key per date
	if _, exists := keyData[tagValue]; !exists && len(keyData) >= maxTopN {
		return
	}

	hc, ok := keyData[tagValue]
	if !ok {
		hc = &hourlyCounter{}
		keyData[tagValue] = hc
	}
	hc.counts[hour] += delta
}

func (tc *TagCountCore) flusher() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		tc.Flush()
	}
}

// Flush writes all dirty data to disk.
func (tc *TagCountCore) Flush() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.flushLocked()
}

func (tc *TagCountCore) flushLocked() {
	for date, dateData := range tc.data {
		for tagKey, keyData := range dateData {
			tc.store.Save(date, tagKey, keyData)
		}
	}
}
