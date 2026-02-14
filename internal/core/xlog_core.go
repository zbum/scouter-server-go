package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/geoip"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/tagcnt"
)

// XLogCore processes incoming XLogPack data, caching and storing transaction logs.
type XLogCore struct {
	xlogCache     *cache.XLogCache
	xlogWR        *xlog.XLogWR
	profileWR     *profile.ProfileWR
	xlogGroupPerf *XLogGroupPerf
	queue         chan *pack.XLogPack
	geoIP         *geoip.GeoIPUtil
	sqlTables     *SqlTables
	visitorCore   *VisitorCore
	tagCountCore  *tagcnt.TagCountCore
	objectCache   *cache.ObjectCache
}

// XLogCoreOption configures optional XLogCore dependencies.
type XLogCoreOption func(*XLogCore)

// WithGeoIP sets the GeoIP lookup util.
func WithGeoIP(g *geoip.GeoIPUtil) XLogCoreOption {
	return func(xc *XLogCore) { xc.geoIP = g }
}

// WithSqlTables sets the SQL table extractor.
func WithSqlTables(st *SqlTables) XLogCoreOption {
	return func(xc *XLogCore) { xc.sqlTables = st }
}

// WithVisitorCore sets the visitor counting core.
func WithVisitorCore(vc *VisitorCore) XLogCoreOption {
	return func(xc *XLogCore) { xc.visitorCore = vc }
}

// WithTagCountCore sets the tag counting core.
func WithTagCountCore(tc *tagcnt.TagCountCore) XLogCoreOption {
	return func(xc *XLogCore) { xc.tagCountCore = tc }
}

// WithObjectCache sets the object cache for type lookups.
func WithObjectCache(oc *cache.ObjectCache) XLogCoreOption {
	return func(xc *XLogCore) { xc.objectCache = oc }
}

func NewXLogCore(xlogCache *cache.XLogCache, xlogWR *xlog.XLogWR, profileWR *profile.ProfileWR, xlogGroupPerf *XLogGroupPerf, opts ...XLogCoreOption) *XLogCore {
	queueSize := 10000
	if cfg := config.Get(); cfg != nil {
		queueSize = cfg.XLogQueueSize()
	}
	xc := &XLogCore{
		xlogCache:     xlogCache,
		xlogWR:        xlogWR,
		profileWR:     profileWR,
		xlogGroupPerf: xlogGroupPerf,
		queue:         make(chan *pack.XLogPack, queueSize),
	}
	for _, opt := range opts {
		opt(xc)
	}
	// Multiple workers to avoid single-goroutine bottleneck.
	// Go channel supports concurrent receivers safely.
	numWorkers := 4
	for i := 0; i < numWorkers; i++ {
		go xc.run()
	}
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

		// Only WEB_SERVICE and APP_SERVICE go through calc (matching Java's XLogCore)
		if isService {
			// Derive group hash from service URL if not already set
			if xc.xlogGroupPerf != nil {
				xc.xlogGroupPerf.Process(xp)
			}
			// GeoIP lookup (only for service types, matching Java)
			if xc.geoIP != nil && len(xp.IPAddr) > 0 {
				countryCode, _, cityHash := xc.geoIP.Lookup(xp.IPAddr)
				if countryCode != "" {
					xp.CountryCode = countryCode
				}
				if cityHash != 0 {
					xp.City = cityHash
				}
			}
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

		// Visitor counting
		if xc.visitorCore != nil && xp.Userid != 0 {
			xc.visitorCore.Add(xp)
		}

		// Tag counting
		if xc.tagCountCore != nil {
			if cfg := config.Get(); cfg != nil && cfg.TagcntEnabled() {
				objType := ""
				if xc.objectCache != nil {
					if info, ok := xc.objectCache.Get(xp.ObjHash); ok {
						objType = info.Pack.ObjType
					}
				}
				xc.tagCountCore.ProcessXLog(objType, xp)
			}
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
