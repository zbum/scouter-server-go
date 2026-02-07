package service

import (
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterXLogReadHandlers registers handlers that read XLog data from storage.
func RegisterXLogReadHandlers(r *Registry, xlogRD *xlog.XLogRD, profileRD *profile.ProfileRD) {

	// XLOG_READ_BY_TXID: retrieve a single XLog by transaction ID.
	r.Register(protocol.XLOG_READ_BY_TXID, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txid := param.GetLong("txid")

		data, err := xlogRD.GetByTxid(date, txid)
		if err != nil || data == nil {
			return
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		dout.Write(data)
	})

	// XLOG_READ_BY_GXID: retrieve all XLogs related to a global transaction ID.
	r.Register(protocol.XLOG_READ_BY_GXID, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		gxid := param.GetLong("gxid")

		xlogRD.ReadByGxid(date, gxid, func(data []byte) {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		})
	})

	// TRANX_LOAD_TIME_GROUP: load XLogs by time range with optional objHash filter.
	tranxLoadTimeGroupHandler := func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")

		// Build objHash filter if present
		objHashFilter := make(map[int32]bool)
		objHashVal := param.Get("objHash")
		if lv, ok := objHashVal.(*value.ListValue); ok && len(lv.Value) > 0 {
			for _, v := range lv.Value {
				if dv, ok := v.(*value.DecimalValue); ok {
					objHashFilter[int32(dv.Value)] = true
				}
			}
		}

		// First packet: metadata
		outparam := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, outparam)

		// Stream XLog data
		xlogRD.ReadByTime(date, stime, etime, func(data []byte) {
			// If filter exists, deserialize to check objHash
			if len(objHashFilter) > 0 {
				innerDin := protocol.NewDataInputX(data)
				xp, err := pack.ReadPack(innerDin)
				if err != nil {
					return
				}
				if xlp, ok := xp.(*pack.XLogPack); ok {
					if !objHashFilter[xlp.ObjHash] {
						return
					}
				}
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		})
	}
	r.Register(protocol.TRANX_LOAD_TIME_GROUP, tranxLoadTimeGroupHandler)
	r.Register(protocol.TRANX_LOAD_TIME_GROUP_V2, tranxLoadTimeGroupHandler)

	// TRANX_PROFILE: retrieve profile blocks for a transaction.
	r.Register(protocol.TRANX_PROFILE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txid := param.GetLong("txid")

		blocks, err := profileRD.GetProfile(date, txid, -1)
		if err != nil || len(blocks) == 0 {
			return
		}

		for _, block := range blocks {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.WriteBlob(block)
		}
	})
}
