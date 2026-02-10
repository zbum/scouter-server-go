package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// XLogCore processes incoming XLogPack data, caching and storing transaction logs.
type XLogCore struct {
	xlogCache     *cache.XLogCache
	xlogWR        *xlog.XLogWR
	profileWR     *profile.ProfileWR
	xlogGroupPerf *XLogGroupPerf
	queue         chan *pack.XLogPack
}

func NewXLogCore(xlogCache *cache.XLogCache, xlogWR *xlog.XLogWR, profileWR *profile.ProfileWR, xlogGroupPerf *XLogGroupPerf) *XLogCore {
	xc := &XLogCore{
		xlogCache:     xlogCache,
		xlogWR:        xlogWR,
		profileWR:     profileWR,
		xlogGroupPerf: xlogGroupPerf,
		queue:         make(chan *pack.XLogPack, 4096),
	}
	go xc.run()
	return xc
}

func (xc *XLogCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		xp, ok := p.(*pack.XLogPack)
		if !ok {
			return
		}
		if xp.EndTime == 0 {
			xp.EndTime = time.Now().UnixMilli()
		}
		select {
		case xc.queue <- xp:
		default:
			slog.Warn("XLogCore queue overflow")
		}
	}
}

func (xc *XLogCore) run() {
	for xp := range xc.queue {
		// Only WEB_SERVICE(0) and APP_SERVICE(1) participate in service group
		// throughput aggregation, matching Scala's XLogCore.calc() filter.
		isService := xp.XType == pack.XLogTypeWebService || xp.XType == pack.XLogTypeAppService

		// Derive group hash from service URL if not already set (before serialization)
		if isService && xc.xlogGroupPerf != nil {
			xc.xlogGroupPerf.Process(xp)
		}

		// Serialize and cache for real-time streaming
		o := protocol.NewDataOutputX()
		pack.WritePack(o, xp)
		b := o.ToByteArray()
		xc.xlogCache.Put(xp.ObjHash, xp.Elapsed, xp.Error != 0, b)

		// Aggregate by service group for real-time throughput display
		if isService && xc.xlogGroupPerf != nil {
			xc.xlogGroupPerf.Add(xp)
		}

		slog.Debug("XLogCore processing",
			"objHash", xp.ObjHash,
			"service", xp.Service,
			"elapsed", xp.Elapsed,
			"txid", xp.Txid)
		if xc.xlogWR != nil {
			xc.xlogWR.Add(&xlog.XLogEntry{
				Time:    xp.EndTime,
				Txid:    xp.Txid,
				Gxid:    xp.Gxid,
				Elapsed: xp.Elapsed,
				Data:    b,
			})
		}
	}
}
