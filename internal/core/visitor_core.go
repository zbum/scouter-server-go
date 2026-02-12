package core

import (
	"log/slog"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/visitor"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// VisitorCore processes visitor data from XLog transactions.
type VisitorCore struct {
	visitorDB     *visitor.VisitorDB
	hourlyDB      *visitor.VisitorHourlyDB
	objCache      *cache.ObjectCache
	queue         chan *visitorEntry
	hourlyEnabled bool
}

type visitorEntry struct {
	objType string
	objHash int32
	userid  int64
}

// NewVisitorCore creates a new visitor counting processor.
func NewVisitorCore(visitorDB *visitor.VisitorDB, hourlyDB *visitor.VisitorHourlyDB, objCache *cache.ObjectCache, hourlyEnabled bool) *VisitorCore {
	vc := &VisitorCore{
		visitorDB:     visitorDB,
		hourlyDB:      hourlyDB,
		objCache:      objCache,
		queue:         make(chan *visitorEntry, 4096),
		hourlyEnabled: hourlyEnabled,
	}
	go vc.run()
	return vc
}

// Add queues a visitor event from an XLogPack.
func (vc *VisitorCore) Add(xp *pack.XLogPack) {
	if xp.Userid == 0 {
		return
	}

	objType := ""
	if vc.objCache != nil {
		if info, ok := vc.objCache.Get(xp.ObjHash); ok {
			objType = info.Pack.ObjType
		}
	}

	select {
	case vc.queue <- &visitorEntry{
		objType: objType,
		objHash: xp.ObjHash,
		userid:  xp.Userid,
	}:
	default:
		slog.Debug("VisitorCore queue overflow")
	}
}

func (vc *VisitorCore) run() {
	for entry := range vc.queue {
		vc.process(entry)
	}
}

func (vc *VisitorCore) process(entry *visitorEntry) {
	if vc.visitorDB != nil {
		vc.visitorDB.Offer(entry.objType, entry.objHash, entry.userid)
	}
	if vc.hourlyEnabled && vc.hourlyDB != nil {
		vc.hourlyDB.Offer(entry.objHash, entry.userid)
	}
}

// FormatVisitorDate returns date string for visitor lookups.
func FormatVisitorDate(t time.Time) string {
	return t.Format("20060102")
}
