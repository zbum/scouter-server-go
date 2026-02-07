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
	// TRANX_REAL_TIME_GROUP: stream recent XLogs for real-time monitoring.
	// Uses loop/index pagination matching Java's XLogLoopCache.
	// Client sends (loop, index) from previous response; server returns only new entries.
	r.Register(protocol.TRANX_REAL_TIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		lastIndex := int(param.GetInt("index"))
		lastLoop := param.GetLong("loop")
		limit := param.GetInt("limit") // min elapsed ms (not count)

		// Build objHash filter set
		var objHashSet map[int32]bool
		objHashVal := param.Get("objHash")
		if lv, ok := objHashVal.(*value.ListValue); ok && len(lv.Value) > 0 {
			objHashSet = make(map[int32]bool, len(lv.Value))
			for _, v := range lv.Value {
				if dv, ok := v.(*value.DecimalValue); ok {
					objHashSet[int32(dv.Value)] = true
				}
			}
		}

		d := xlogCache.Get(lastLoop, lastIndex, limit, objHashSet)

		// First packet: metadata (loop/index for pagination)
		outparam := &pack.MapPack{}
		outparam.PutLong("loop", d.Loop)
		outparam.PutLong("index", int64(d.Index))
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, outparam)

		// Stream XLog data (pre-serialized bytes)
		for _, entry := range d.Data {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(entry.Data)
		}
	})

	// TRANX_REAL_TIME_GROUP_LATEST: same as above but uses count-based retrieval.
	r.Register(protocol.TRANX_REAL_TIME_GROUP_LATEST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		lastIndex := int(param.GetInt("index"))
		lastLoop := param.GetLong("loop")

		// Build objHash filter set
		var objHashSet map[int32]bool
		objHashVal := param.Get("objHash")
		if lv, ok := objHashVal.(*value.ListValue); ok && len(lv.Value) > 0 {
			objHashSet = make(map[int32]bool, len(lv.Value))
			for _, v := range lv.Value {
				if dv, ok := v.(*value.DecimalValue); ok {
					objHashSet[int32(dv.Value)] = true
				}
			}
		}

		// Use minElapsed=0 to return all entries (count-based, no time filter)
		d := xlogCache.Get(lastLoop, lastIndex, 0, objHashSet)

		outparam := &pack.MapPack{}
		outparam.PutLong("loop", d.Loop)
		outparam.PutLong("index", int64(d.Index))
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, outparam)

		for _, entry := range d.Data {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(entry.Data)
		}
	})
}
