package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// Use cache.TimeTypeRealtime instead of local constant.

// PerfCountCore processes incoming PerfCounterPack data.
type PerfCountCore struct {
	counterCache *cache.CounterCache
	counterWR    *counter.CounterWR
	queue        chan *pack.PerfCounterPack
}

func NewPerfCountCore(counterCache *cache.CounterCache, counterWR *counter.CounterWR) *PerfCountCore {
	pc := &PerfCountCore{
		counterCache: counterCache,
		counterWR:    counterWR,
		queue:        make(chan *pack.PerfCounterPack, 4096),
	}
	go pc.run()
	return pc
}

func (pc *PerfCountCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		cp, ok := p.(*pack.PerfCounterPack)
		if !ok {
			return
		}
		if cp.Time == 0 {
			cp.Time = time.Now().UnixMilli()
		}
		select {
		case pc.queue <- cp:
		default:
			slog.Warn("PerfCountCore queue overflow")
		}
	}
}

func (pc *PerfCountCore) run() {
	for cp := range pc.queue {
		objHash := util.HashString(cp.ObjName)

		// Cache each counter value
		for _, entry := range cp.Data.Entries {
			key := cache.CounterKey{
				ObjHash:  objHash,
				Counter:  entry.Key,
				TimeType: cp.TimeType,
			}
			pc.counterCache.Put(key, entry.Value)
		}

		slog.Debug("PerfCountCore processing",
			"objName", cp.ObjName,
			"objHash", objHash,
			"counters", cp.Data.Size())
		if pc.counterWR != nil {
			if cp.TimeType == cache.TimeTypeRealtime {
				// Convert MapValue entries to map[string]value.Value
				counters := make(map[string]value.Value)
				for _, entry := range cp.Data.Entries {
					counters[entry.Key] = entry.Value
				}
				pc.counterWR.AddRealtimeFromPerfCounter(cp.Time, objHash, counters)
			}
		}
	}
}
