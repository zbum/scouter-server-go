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

// RegisterCounterExtHandlers registers extended counter service handlers (P2).
func RegisterCounterExtHandlers(r *Registry, counterCache *cache.CounterCache, objectCache *cache.ObjectCache, deadTimeout time.Duration, counterRD *counter.CounterRD) {

	// COUNTER_REAL_TIME_MULTI: get multiple counter values for a single object.
	r.Register(protocol.COUNTER_REAL_TIME_MULTI, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		objHash := param.GetInt("objHash")
		counterVal := param.Get("counter")

		result := &pack.MapPack{}
		if lv, ok := counterVal.(*value.ListValue); ok {
			for _, cv := range lv.Value {
				if tv, ok := cv.(*value.TextValue); ok {
					key := cache.CounterKey{ObjHash: objHash, Counter: tv.Value, TimeType: 0}
					v, found := counterCache.Get(key)
					if found && v != nil {
						result.Put(tv.Value, v)
					}
				}
			}
		}

		if result.Size() > 0 {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	// COUNTER_REAL_TIME_ALL_MULTI: get multiple counter values for all live objects of a type.
	r.Register(protocol.COUNTER_REAL_TIME_ALL_MULTI, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		objType := param.GetText("objType")
		counterVal := param.Get("counter")
		if objType == "" {
			return
		}

		// Extract counter names from the ListValue.
		var counterNames []string
		if lv, ok := counterVal.(*value.ListValue); ok {
			for _, cv := range lv.Value {
				if tv, ok := cv.(*value.TextValue); ok {
					counterNames = append(counterNames, tv.Value)
				}
			}
		}
		if len(counterNames) == 0 {
			return
		}

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}

			result := &pack.MapPack{}
			result.PutLong("objHash", int64(info.Pack.ObjHash))

			for _, counterName := range counterNames {
				key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counterName, TimeType: 0}
				v, found := counterCache.Get(key)
				if found && v != nil {
					result.Put(counterName, v)
				}
			}

			if result.Size() > 1 { // has more than just objHash
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, result)
			}
		}
	})

	// COUNTER_TODAY: today's 5-min counter data for a single object.
	r.Register(protocol.COUNTER_TODAY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		objHash := param.GetInt("objHash")
		counterName := param.GetText("counter")

		date := time.Now().Format("20060102")

		values, err := counterRD.ReadDailyAll(date, objHash, counterName)
		if err != nil || values == nil {
			return
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
		result.Put("value", &value.FloatArray{Value: floats})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// COUNTER_TODAY_ALL: today's 5-min counter data for all live objects of a type.
	r.Register(protocol.COUNTER_TODAY_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		objType := param.GetText("objType")
		counterName := param.GetText("counter")
		if objType == "" {
			return
		}

		date := time.Now().Format("20060102")

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

	// COUNTER_REAL_TIME_TOT: total (sum) of a counter across all live objects of a type.
	r.Register(protocol.COUNTER_REAL_TIME_TOT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		objType := param.GetText("objType")
		counterName := param.GetText("counter")
		if objType == "" {
			return
		}

		var totalFloat float64
		hasValue := false

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}
			key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counterName, TimeType: 0}
			v, found := counterCache.Get(key)
			if !found || v == nil {
				continue
			}

			switch tv := v.(type) {
			case *value.DecimalValue:
				totalFloat += float64(tv.Value)
				hasValue = true
			case *value.FloatValue:
				totalFloat += float64(tv.Value)
				hasValue = true
			case *value.DoubleValue:
				totalFloat += tv.Value
				hasValue = true
			}
		}

		if hasValue {
			result := &pack.MapPack{}
			result.Put("value", &value.DoubleValue{Value: totalFloat})
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})
}
