package service

import (
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterAlertHandlers registers handlers for loading historical and real-time alerts.
func RegisterAlertHandlers(r *Registry, alertRD *alert.AlertRD, alertCache *cache.AlertCache) {

	// ALERT_LOAD_TIME: load historical alerts by time range.
	r.Register(protocol.ALERT_LOAD_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")

		alertRD.ReadRange(date, stime, etime, func(data []byte) {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		})
	})

	// ALERT_REAL_TIME: return real-time alerts from cache.
	r.Register(protocol.ALERT_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		clientIndex := int(param.GetLong("index"))
		clientLoop := param.GetLong("loop")

		// Check "first" parameter
		first := false
		if v := param.Get("first"); v != nil {
			if bv, ok := v.(*value.BooleanValue); ok {
				first = bv.Value
			}
		}

		// Get current position and delta alerts
		alerts, curLoop, curIndex := alertCache.GetSince(clientLoop, clientIndex)

		// First packet: metadata with current position (always sent)
		resp := &pack.MapPack{}
		resp.PutLong("loop", curLoop)
		resp.PutLong("index", int64(curIndex))
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)

		// On first call, return only position - no alert data
		if first {
			return
		}

		// Send each alert
		for _, data := range alerts {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		}
	})
}
