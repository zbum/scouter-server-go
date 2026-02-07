package service

import (
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterXLogHandlers registers TRANX_REAL_TIME_GROUP and related handlers.
func RegisterXLogHandlers(r *Registry, xlogCache *cache.XLogCache, xlogRD *xlog.XLogRD) {
	// TRANX_REAL_TIME_GROUP: stream recent XLogs for real-time monitoring
	r.Register(protocol.TRANX_REAL_TIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		limit := int(param.GetInt("limit"))
		if limit <= 0 {
			limit = 100
		}

		// Get objHash filter set
		objHashFilter := make(map[int32]bool)
		objHashVal := param.Get("objHash")
		if lv, ok := objHashVal.(*value.ListValue); ok && len(lv.Value) > 0 {
			for _, v := range lv.Value {
				if dv, ok := v.(*value.DecimalValue); ok {
					objHashFilter[int32(dv.Value)] = true
				}
			}
		}

		var entries []cache.XLogEntry
		if len(objHashFilter) > 0 {
			// Filtered: get entries for specified objects
			all := xlogCache.GetRecent(limit * 10) // get more and filter
			for _, e := range all {
				if objHashFilter[e.ObjHash] {
					entries = append(entries, e)
					if len(entries) >= limit {
						break
					}
				}
			}
		} else {
			entries = xlogCache.GetRecent(limit)
		}

		// First packet: metadata (loop/index for pagination)
		outparam := &pack.MapPack{}
		outparam.PutLong("loop", 0)
		outparam.PutLong("index", int64(len(entries)))
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, outparam)

		// Stream XLog data (pre-serialized bytes)
		for _, entry := range entries {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(entry.Data)
		}
	})
}
