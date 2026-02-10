package core

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

const (
	// meterBucketCount is the number of per-second buckets in the ring buffer.
	// 600 seconds = 10 minutes of history.
	meterBucketCount = 600

	// defaultPerfStatPeriod is the default period (seconds) for aggregating
	// real-time statistics (matching Java's build() using 30 seconds).
	defaultPerfStatPeriod = 30

	// maxMeterEntries caps the number of (objHash, group) combinations tracked.
	maxMeterEntries = 2000
)

// PerfStat holds aggregated performance statistics for a service group.
type PerfStat struct {
	Count   int64
	Error   int64
	Elapsed int64
}

// AvgElapsed returns the average elapsed time.
func (s *PerfStat) AvgElapsed() float32 {
	if s.Count == 0 {
		return 0
	}
	return float32(s.Elapsed) / float32(s.Count)
}

// Add merges another PerfStat into this one.
func (s *PerfStat) Add(o *PerfStat) {
	s.Count += o.Count
	s.Error += o.Error
	s.Elapsed += o.Elapsed
}

// meterBucket holds per-second counters.
type meterBucket struct {
	timeSec int64
	count   int64
	error   int64
	elapsed int64
}

// meterService tracks per-second metrics using a ring buffer.
type meterService struct {
	buckets [meterBucketCount]meterBucket
}

func (m *meterService) add(elapsed int32, isError bool) {
	sec := time.Now().Unix()
	idx := int(sec % meterBucketCount)
	b := &m.buckets[idx]
	if b.timeSec != sec {
		// New second — reset bucket
		b.timeSec = sec
		b.count = 0
		b.error = 0
		b.elapsed = 0
	}
	b.count++
	b.elapsed += int64(elapsed)
	if isError {
		b.error++
	}
}

func (m *meterService) getPerfStat(periodSec int) *PerfStat {
	now := time.Now().Unix()
	stat := &PerfStat{}
	for i := 0; i < periodSec && i < meterBucketCount; i++ {
		sec := now - int64(i)
		idx := int(sec % meterBucketCount)
		b := &m.buckets[idx]
		if b.timeSec == sec {
			stat.Count += b.count
			stat.Error += b.error
			stat.Elapsed += b.elapsed
		}
	}
	return stat
}

// groupKey identifies a (objHash, group) combination.
type groupKey struct {
	objHash int32
	group   int32
}

// XLogGroupPerf aggregates XLog data by (objHash, service group) for
// real-time service group throughput display.
type XLogGroupPerf struct {
	mu        sync.Mutex
	meters    map[groupKey]*meterService
	textCache *cache.TextCache
	groupUtil *XLogGroupUtil

	// Cached result with 1-second TTL
	cachedResult    map[int32]*PerfStat // nil means no cache
	cachedObjFilter map[int32]bool
	cacheTime       int64 // unix millis

	// Diagnostic counters
	totalCount    atomic.Int64
	fallbackCount atomic.Int64
	lastLogTime   atomic.Int64
}

// NewXLogGroupPerf creates a new XLogGroupPerf aggregator.
func NewXLogGroupPerf(textCache *cache.TextCache, textRD *text.TextRD) *XLogGroupPerf {
	return &XLogGroupPerf{
		meters:        make(map[groupKey]*meterService),
		textCache:     textCache,
		groupUtil:     NewXLogGroupUtil(textCache, textRD),
	}
}

// Process derives the group hash from the service URL if not already set.
// Must be called before serializing the XLogPack.
func (x *XLogGroupPerf) Process(xp *pack.XLogPack) {
	if x.groupUtil != nil {
		x.groupUtil.Process(xp)
	}
}

// Add records an XLogPack's metrics for service group aggregation.
// Called from XLogCore.run() for each incoming XLogPack.
// Matching Scala's XLogGroupPerf.process(): if group == 0, the XLog is dropped.
func (x *XLogGroupPerf) Add(xp *pack.XLogPack) {
	x.totalCount.Add(1)

	group := xp.Group
	if group == 0 {
		// Drop XLogs with no group, matching Scala behavior.
		x.fallbackCount.Add(1)
		now := time.Now().Unix()
		if now-x.lastLogTime.Load() >= 10 {
			x.lastLogTime.Store(now)
			total := x.totalCount.Load()
			fallback := x.fallbackCount.Load()
			slog.Debug("XLogGroupPerf: XLogs dropped (group=0)",
				"dropped", fallback, "total", total,
				"dropRate", fmt.Sprintf("%.1f%%", float64(fallback)/float64(total)*100),
				"serviceHash", xp.Service, "objHash", xp.ObjHash)
		}
		return
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	key := groupKey{objHash: xp.ObjHash, group: group}
	m, ok := x.meters[key]
	if !ok {
		if len(x.meters) >= maxMeterEntries {
			// Evict a random entry to stay under the cap
			for k := range x.meters {
				delete(x.meters, k)
				break
			}
		}
		m = &meterService{}
		x.meters[key] = m
	}
	m.add(xp.Elapsed, xp.Error != 0)
}

// GetGroupPerfStat returns per-group aggregated PerfStat for the given
// object hashes. If objHashes is nil or empty, all objects are included.
// Results are cached for 1 second.
func (x *XLogGroupPerf) GetGroupPerfStat(objHashes map[int32]bool) map[int32]*PerfStat {
	x.mu.Lock()
	defer x.mu.Unlock()

	now := time.Now().UnixMilli()

	// Return cached result if within 1 second and same filter
	if x.cachedResult != nil && now-x.cacheTime < 1000 && mapsEqual(x.cachedObjFilter, objHashes) {
		return x.cachedResult
	}

	result := make(map[int32]*PerfStat)
	for key, meter := range x.meters {
		if len(objHashes) > 0 && !objHashes[key.objHash] {
			continue
		}
		stat := meter.getPerfStat(defaultPerfStatPeriod)
		if stat.Count == 0 {
			continue
		}
		if existing, ok := result[key.group]; ok {
			existing.Add(stat)
		} else {
			result[key.group] = stat
		}
	}

	x.cachedResult = result
	x.cachedObjFilter = objHashes
	x.cacheTime = now

	return result
}

func mapsEqual(a, b map[int32]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
