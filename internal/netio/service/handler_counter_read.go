package service

import (
	"math"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterCounterReadHandlers registers handlers that read counter data from storage.
func RegisterCounterReadHandlers(r *Registry, counterRD *counter.CounterRD, objectCache *cache.ObjectCache, deadTimeout time.Duration) {

	// COUNTER_PAST_TIME: read realtime counter range for a single object.
	r.Register(protocol.COUNTER_PAST_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		objHash := param.GetInt("objHash")
		counterName := param.GetText("counter")
		stime := int32(param.GetInt("stime"))
		etime := int32(param.GetInt("etime"))

		timeList := value.NewListValue()
		valueList := value.NewListValue()

		counterRD.ReadRealtimeRange(date, objHash, stime, etime, func(timeSec int32, counters map[string]value.Value) {
			if v, ok := counters[counterName]; ok {
				timeList.Value = append(timeList.Value, value.NewDecimalValue(int64(timeSec)))
				valueList.Value = append(valueList.Value, v)
			}
		})

		if len(timeList.Value) > 0 {
			result := &pack.MapPack{}
			result.Put("time", timeList)
			result.Put("value", valueList)
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	// COUNTER_PAST_TIME_ALL: read realtime counter range for all live objects of a type.
	r.Register(protocol.COUNTER_PAST_TIME_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		counterName := param.GetText("counter")
		objType := param.GetText("objType")
		stime := int32(param.GetInt("stime"))
		etime := int32(param.GetInt("etime"))

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}

			timeList := value.NewListValue()
			valueList := value.NewListValue()

			counterRD.ReadRealtimeRange(date, info.Pack.ObjHash, stime, etime, func(timeSec int32, counters map[string]value.Value) {
				if v, ok := counters[counterName]; ok {
					timeList.Value = append(timeList.Value, value.NewDecimalValue(int64(timeSec)))
					valueList.Value = append(valueList.Value, v)
				}
			})

			if len(timeList.Value) > 0 {
				result := &pack.MapPack{}
				result.PutLong("objHash", int64(info.Pack.ObjHash))
				result.Put("time", timeList)
				result.Put("value", valueList)
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, result)
			}
		}
	})

	// COUNTER_PAST_DATE: read daily (5-min bucket) counter for a single object.
	r.Register(protocol.COUNTER_PAST_DATE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		objHash := param.GetInt("objHash")
		counterName := param.GetText("counter")

		values, err := counterRD.ReadDailyAll(date, objHash, counterName)
		if err != nil || values == nil {
			return
		}

		// Convert to float32 array for FloatArray value.
		// NaN values are preserved so the client can distinguish empty buckets.
		floats := make([]float32, len(values))
		for i, v := range values {
			if math.IsNaN(v) {
				floats[i] = 0
			} else {
				floats[i] = float32(v)
			}
		}

		result := &pack.MapPack{}
		result.Put("value", &value.FloatArray{Value: floats})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// COUNTER_PAST_DATE_ALL: read daily counter for all live objects of a type.
	r.Register(protocol.COUNTER_PAST_DATE_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		counterName := param.GetText("counter")
		objType := param.GetText("objType")

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}

			values, err := counterRD.ReadDailyAll(date, info.Pack.ObjHash, counterName)
			if err != nil || values == nil {
				continue
			}

			floats := make([]float32, len(values))
			for i, v := range values {
				if math.IsNaN(v) {
					floats[i] = 0
				} else {
					floats[i] = float32(v)
				}
			}

			result := &pack.MapPack{}
			result.PutLong("objHash", int64(info.Pack.ObjHash))
			result.Put("value", &value.FloatArray{Value: floats})
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})
}
