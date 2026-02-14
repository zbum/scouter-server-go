package service

import (
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// RegisterXLogReadHandlers registers handlers that read XLog data from storage.
// xlogWR is used for reading the current day's data (always up-to-date in memory),
// with fallback to xlogRD for dates not held by the writer.
func RegisterXLogReadHandlers(r *Registry, xlogRD *xlog.XLogRD, profileRD *profile.ProfileRD, profileWR *profile.ProfileWR, xlogWR *xlog.XLogWR) {

	// XLOG_READ_BY_TXID: retrieve a single XLog by transaction ID.
	r.Register(protocol.XLOG_READ_BY_TXID, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		txid := param.GetLong("txid")

		// Try writer first (current day), then fall back to reader
		data, found, err := xlogWR.GetByTxid(date, txid)
		if !found {
			data, err = xlogRD.GetByTxid(date, txid)
		}
		if err != nil || data == nil {
			return
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		dout.Write(data)
		dout.Flush()
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

		gxidHandler := func(data []byte) {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
			dout.Flush()
		}
		if found, _ := xlogWR.ReadByGxid(date, gxid, gxidHandler); !found {
			xlogRD.ReadByGxid(date, gxid, gxidHandler)
		}
	})

	// TRANX_LOAD_TIME_GROUP: load XLogs by time range with optional objHash filter.
	// Try xlogWR first (which holds the up-to-date in-memory index for the
	// current day), then fall back to xlogRD for dates the writer doesn't hold.
	tranxLoadTimeGroupHandler := func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")
		max := param.GetInt("max")
		rev := param.GetBoolean("reverse")
		limitTime := param.GetInt("limit")

		// elapsed lower bound: max(config, request param) — matches Java behavior
		limit := int32(0)
		if cfg := config.Get(); cfg != nil {
			limit = int32(cfg.XLogPasttimeLowerBoundMs())
		}
		if int32(limitTime) > limit {
			limit = int32(limitTime)
		}

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

		cnt := 0
		needFilter := len(objHashFilter) > 0 || limit > 0
		dataHandler := func(data []byte) bool {
			if max > 0 && cnt >= int(max) {
				return false
			}
			if needFilter {
				objHash, elapsed, err := pack.ReadXLogFilterFields(data)
				if err != nil {
					return true
				}
				if len(objHashFilter) > 0 && !objHashFilter[objHash] {
					return true
				}
				// elapsed filter — skip fast transactions (matches Java's x.elapsed > limit)
				if limit > 0 && elapsed <= limit {
					return true
				}
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
			dout.Flush()
			cnt++
			return true
		}

		// Try xlogWR first (current day has up-to-date in-memory index),
		// fall back to xlogRD for past dates.
		if rev {
			if found, _ := xlogWR.ReadFromEndTime(date, stime, etime, dataHandler); !found {
				xlogRD.ReadFromEndTime(date, stime, etime, dataHandler)
			}
		} else {
			if found, _ := xlogWR.ReadByTime(date, stime, etime, dataHandler); !found {
				xlogRD.ReadByTime(date, stime, etime, dataHandler)
			}
		}
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

		// Parallel lookup: each txid does disk I/O (index + data read via pread),
		// so concurrent goroutines overlap I/O latency.
		results := make([][]byte, len(txidLv.Value))
		var wg sync.WaitGroup

		for i, hv := range txidLv.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			wg.Add(1)
			go func(idx int, txid int64) {
				defer wg.Done()
				data, found, err := xlogWR.GetByTxid(date, txid)
				if !found {
					data, err = xlogRD.GetByTxid(date, txid)
				}
				if err == nil && data != nil {
					results[idx] = data
				}
			}(i, dv.Value)
		}

		wg.Wait()

		// Write results sequentially (dout is not thread-safe)
		for _, data := range results {
			if data != nil {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				dout.Write(data)
				dout.Flush()
			}
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

		gxidHandler := func(data []byte) {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
			dout.Flush()
		}

		if found, _ := xlogWR.ReadByGxid(date, gxid, gxidHandler); !found {
			xlogRD.ReadByGxid(date, gxid, gxidHandler)
		}

		if date != date2 {
			if found, _ := xlogWR.ReadByGxid(date2, gxid, gxidHandler); !found {
				xlogRD.ReadByGxid(date2, gxid, gxidHandler)
			}
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
			data, found, err := xlogWR.GetByTxid(date, txid)
			if !found {
				data, err = xlogRD.GetByTxid(date, txid)
			}
			if err == nil && data != nil {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				dout.Write(data)
				dout.Flush()
			}
		}
		if gxid != 0 {
			gxidHandler := func(data []byte) {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				dout.Write(data)
				dout.Flush()
			}
			if found, _ := xlogWR.ReadByGxid(date, gxid, gxidHandler); !found {
				xlogRD.ReadByGxid(date, gxid, gxidHandler)
			}
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

		// req_search_xlog_max_count: limit max results
		maxCount := 0
		if cfg := config.Get(); cfg != nil {
			maxCount = cfg.ReqSearchXLogMaxCount()
		}
		cnt := 0

		searchHandler := func(data []byte) bool {
			if maxCount > 0 && cnt >= maxCount {
				return false
			}
			if objHash != 0 {
				packObjHash, _, err := pack.ReadXLogFilterFields(data)
				if err != nil {
					return true
				}
				if packObjHash != objHash {
					return true
				}
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
			dout.Flush()
			cnt++
			return true
		}

		readByTime := func(d string, s, e int64) {
			if found, _ := xlogWR.ReadByTime(d, s, e, searchHandler); !found {
				xlogRD.ReadByTime(d, s, e, searchHandler)
			}
		}

		if date == date2 {
			readByTime(date, stime, etime)
		} else {
			mtime := util.DateToMillis(date2)
			readByTime(date, stime, mtime-1)
			readByTime(date2, mtime, etime)
		}
	})
}
