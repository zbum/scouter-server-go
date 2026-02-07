package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// RegisterXLogReadHandlers registers handlers that read XLog data from storage.
func RegisterXLogReadHandlers(r *Registry, xlogRD *xlog.XLogRD, profileRD *profile.ProfileRD, profileWR *profile.ProfileWR) {

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
	// Java's processGetProfile concatenates all blocks into one byte array,
	// wraps it in XLogProfilePack, and sends via writePack.
	r.Register(protocol.TRANX_PROFILE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txid := param.GetLong("txid")

		// Read through ProfileWR which has up-to-date MemHashBlock index.
		// ProfileRD has a stale index snapshot from when it was opened.
		blocks, err := profileWR.Read(date, txid, -1)
		if err != nil || len(blocks) == 0 {
			return
		}

		// Concatenate all blocks into a single byte array (matching Java's XLogProfileRD.getProfile)
		var allData []byte
		for _, block := range blocks {
			allData = append(allData, block...)
		}

		// Wrap in XLogProfilePack (matching Java's processGetProfile)
		profilePack := &pack.XLogProfilePack{
			Profile: allData,
		}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, profilePack)
	})

	// TRANX_PROFILE_FULL: retrieve full profile including related transactions.
	r.Register(protocol.TRANX_PROFILE_FULL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txid := param.GetLong("txid")
		if date == "" {
			date = time.Now().Format("20060102")
		}

		blocks, err := profileWR.Read(date, txid, -1)
		if err != nil || len(blocks) == 0 {
			return
		}

		var allData []byte
		for _, block := range blocks {
			allData = append(allData, block...)
		}

		profilePack := &pack.XLogProfilePack{
			Profile: allData,
		}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, profilePack)
	})

	// XLOG_LOAD_BY_TXIDS: retrieve XLogs by a list of transaction IDs.
	r.Register(protocol.XLOG_LOAD_BY_TXIDS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txidLv := param.GetList("txid")
		if txidLv == nil {
			return
		}

		for _, hv := range txidLv.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			txid := dv.Value
			data, err := xlogRD.GetByTxid(date, txid)
			if err != nil || data == nil {
				continue
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		}
	})

	// XLOG_LOAD_BY_GXID: retrieve all XLogs by global transaction ID with time range.
	r.Register(protocol.XLOG_LOAD_BY_GXID, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")
		gxid := param.GetLong("gxid")
		date := util.FormatDate(stime)
		date2 := util.FormatDate(etime)

		xlogRD.ReadByGxid(date, gxid, func(data []byte) {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		})

		if date != date2 {
			xlogRD.ReadByGxid(date2, gxid, func(data []byte) {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				dout.Write(data)
			})
		}
	})

	// QUICKSEARCH_XLOG_LIST: search XLogs by txid or gxid.
	r.Register(protocol.QUICKSEARCH_XLOG_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txid := param.GetLong("txid")
		gxid := param.GetLong("gxid")

		if txid != 0 {
			data, err := xlogRD.GetByTxid(date, txid)
			if err == nil && data != nil {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				dout.Write(data)
			}
		}
		if gxid != 0 {
			xlogRD.ReadByGxid(date, gxid, func(data []byte) {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				dout.Write(data)
			})
		}
	})

	// SEARCH_XLOG_LIST: search XLogs by time range with optional objHash filter.
	r.Register(protocol.SEARCH_XLOG_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")
		objHash := param.GetInt("objHash")

		date := util.FormatDate(stime)
		date2 := util.FormatDate(etime)

		handler := func(data []byte) {
			if objHash != 0 {
				innerDin := protocol.NewDataInputX(data)
				xp, err := pack.ReadPack(innerDin)
				if err != nil {
					return
				}
				if xlp, ok := xp.(*pack.XLogPack); ok {
					if xlp.ObjHash != objHash {
						return
					}
				}
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		}

		if date == date2 {
			xlogRD.ReadByTime(date, stime, etime, handler)
		} else {
			mtime := util.DateToMillis(date2)
			xlogRD.ReadByTime(date, stime, mtime-1, handler)
			xlogRD.ReadByTime(date2, mtime, etime, handler)
		}
	})
}
