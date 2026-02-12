package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/visitor"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterVisitorHandlers registers visitor-related handlers.
func RegisterVisitorHandlers(r *Registry, visitorDB *visitor.VisitorDB, hourlyDB *visitor.VisitorHourlyDB, objectCache *cache.ObjectCache, deadTimeout time.Duration) {

	// VISITOR_REALTIME: real-time visitor count for a single object.
	r.Register(protocol.VISITOR_REALTIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)
		objHash := int32(mp.GetLong("objHash"))

		var count int64
		if visitorDB != nil {
			count = visitorDB.CountByObj(objHash)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(count))
	})

	// VISITOR_REALTIME_TOTAL: real-time visitor count for all objects of a type.
	r.Register(protocol.VISITOR_REALTIME_TOTAL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)
		objType := mp.GetText("objType")

		var count int64
		if visitorDB != nil {
			count = visitorDB.CountByType(objType)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(count))
	})

	// VISITOR_REALTIME_GROUP: real-time visitor count for a group of objects.
	r.Register(protocol.VISITOR_REALTIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)

		var objHashes []int32
		if hashList := mp.Get("objHash"); hashList != nil {
			if lv, ok := hashList.(*value.ListValue); ok {
				for _, v := range lv.Value {
					if dv, ok := v.(*value.DecimalValue); ok {
						objHashes = append(objHashes, int32(dv.Value))
					}
				}
			}
		}

		var count int64
		if visitorDB != nil && len(objHashes) > 0 {
			count = visitorDB.CountByObjGroup(objHashes)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(count))
	})

	// VISITOR_LOADDATE: historical visitor count for an object on a date.
	r.Register(protocol.VISITOR_LOADDATE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)
		objHash := int32(mp.GetLong("objHash"))
		date := mp.GetText("date")
		if date == "" {
			date = time.Now().Format("20060102")
		}

		var count int64
		if visitorDB != nil {
			count = visitorDB.LoadDate(date, objHash)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(count))
	})

	// VISITOR_LOADDATE_TOTAL: historical visitor count for a type on a date.
	r.Register(protocol.VISITOR_LOADDATE_TOTAL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)
		objType := mp.GetText("objType")
		date := mp.GetText("date")
		if date == "" {
			date = time.Now().Format("20060102")
		}

		var count int64
		if visitorDB != nil {
			count = visitorDB.LoadDateTotal(date, objType)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(count))
	})

	// VISITOR_LOADDATE_GROUP: historical visitor count per date for a group of objects.
	r.Register(protocol.VISITOR_LOADDATE_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)

		var objHashes []int32
		if hashList := mp.Get("objHash"); hashList != nil {
			if lv, ok := hashList.(*value.ListValue); ok {
				for _, v := range lv.Value {
					if dv, ok := v.(*value.DecimalValue); ok {
						objHashes = append(objHashes, int32(dv.Value))
					}
				}
			}
		}

		dateFrom := mp.GetText("date")
		dateTo := mp.GetText("dateTo")
		if dateFrom == "" {
			dateFrom = time.Now().Format("20060102")
		}
		if dateTo == "" {
			dateTo = dateFrom
		}

		// Iterate over date range
		startTime, _ := time.Parse("20060102", dateFrom)
		endTime, _ := time.Parse("20060102", dateTo)

		for d := startTime; !d.After(endTime); d = d.AddDate(0, 0, 1) {
			date := d.Format("20060102")
			var count int64
			if visitorDB != nil && len(objHashes) > 0 {
				count = visitorDB.CountByObjGroup(objHashes)
				// For historical dates, load from disk
				if date != time.Now().Format("20060102") {
					merged := int64(0)
					for _, hash := range objHashes {
						merged += visitorDB.LoadDate(date, hash)
					}
					count = merged
				}
			}

			resp := &pack.MapPack{}
			resp.PutStr("date", date)
			resp.PutLong("count", count)
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, resp)
		}
	})

	// VISITOR_LOADHOUR_GROUP: historical visitor count per hour for a group of objects.
	r.Register(protocol.VISITOR_LOADHOUR_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		p, _ := pack.ReadPack(din)
		mp := p.(*pack.MapPack)

		var objHashes []int32
		if hashList := mp.Get("objHash"); hashList != nil {
			if lv, ok := hashList.(*value.ListValue); ok {
				for _, v := range lv.Value {
					if dv, ok := v.(*value.DecimalValue); ok {
						objHashes = append(objHashes, int32(dv.Value))
					}
				}
			}
		}

		date := mp.GetText("date")
		if date == "" {
			date = time.Now().Format("20060102")
		}

		if hourlyDB != nil && len(objHashes) > 0 {
			hours := hourlyDB.LoadAllHours(date, objHashes)
			resp := &pack.MapPack{}
			hourList := value.NewListValue()
			for _, h := range hours {
				hourList.Value = append(hourList.Value, value.NewDecimalValue(h))
			}
			resp.Put("hours", hourList)
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, resp)
		}
	})
}
