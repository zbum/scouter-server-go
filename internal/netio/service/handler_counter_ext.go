package service

import (
	"math"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
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
					key := cache.CounterKey{ObjHash: objHash, Counter: tv.Value, TimeType: cache.TimeTypeRealtime}
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
				key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counterName, TimeType: cache.TimeTypeRealtime}
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
			key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counterName, TimeType: cache.TimeTypeRealtime}
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

	// COUNTER_TODAY_TOT: total/avg of today's daily counter across all objects of a type.
	r.Register(protocol.COUNTER_TODAY_TOT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		mode := param.GetText("mode")
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		date := time.Now().Format("20060102")
		values := make([]float64, util.BucketsPerDay)
		cnt := make([]int, util.BucketsPerDay)

		all := objectCache.GetAll()
		for _, info := range all {
			if info.Pack.ObjType != objType {
				continue
			}
			v, err := counterRD.ReadDailyAll(date, info.Pack.ObjHash, counterName)
			if err != nil || v == nil {
				continue
			}
			for j, val := range v {
				if j >= util.BucketsPerDay {
					break
				}
				if !math.IsNaN(val) && val > 0 {
					cnt[j]++
					values[j] += val
				}
			}
		}

		stime := util.DateToMillis(date)
		isAvg := mode == "avg"

		timeList := value.NewListValue()
		valueList := value.NewListValue()
		for i := 0; i < util.BucketsPerDay; i++ {
			timeList.Value = append(timeList.Value, value.NewDecimalValue(stime+int64(i)*int64(util.MillisPerFiveMinute)))
			v := values[i]
			if isAvg && cnt[i] > 1 {
				v /= float64(cnt[i])
			}
			valueList.Value = append(valueList.Value, &value.DoubleValue{Value: v})
		}

		result := &pack.MapPack{}
		result.Put("time", timeList)
		result.Put("value", valueList)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// COUNTER_TODAY_GROUP: today's daily counter for a list of objHashes.
	r.Register(protocol.COUNTER_TODAY_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		objHashLv := param.GetList("objHash")
		if objHashLv == nil {
			return
		}

		date := time.Now().Format("20060102")
		stime := util.DateToMillis(date)
		delta := int64(util.MillisPerFiveMinute)

		for _, hv := range objHashLv.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			objHash := int32(dv.Value)
			v, err := counterRD.ReadDailyAll(date, objHash, counterName)

			timeList := value.NewListValue()
			valueList := value.NewListValue()

			if err == nil && v != nil {
				for j, val := range v {
					timeList.Value = append(timeList.Value, value.NewDecimalValue(stime+int64(j)*delta))
					if math.IsNaN(val) {
						valueList.Value = append(valueList.Value, &value.NullValue{})
					} else {
						valueList.Value = append(valueList.Value, &value.DoubleValue{Value: val})
					}
				}
			}

			result := &pack.MapPack{}
			result.PutLong("objHash", int64(objHash))
			result.Put("time", timeList)
			result.Put("value", valueList)
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	// COUNTER_REAL_TIME_OBJECT_ALL: all counter values for a single object.
	r.Register(protocol.COUNTER_REAL_TIME_OBJECT_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHash := param.GetInt("objHash")

		counters := counterCache.GetByObjHash(objHash)
		counterList := value.NewListValue()
		valueList := value.NewListValue()

		for name, v := range counters {
			counterList.Value = append(counterList.Value, value.NewTextValue(name))
			valueList.Value = append(valueList.Value, v)
		}

		result := &pack.MapPack{}
		result.Put("counter", counterList)
		result.Put("value", valueList)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// COUNTER_REAL_TIME_OBJECT_TYPE_ALL: all counter values for all objects of a type.
	r.Register(protocol.COUNTER_REAL_TIME_OBJECT_TYPE_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		result := &pack.MapPack{}
		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}
			counters := counterCache.GetByObjHash(info.Pack.ObjHash)
			mv := value.NewMapValue()
			for name, v := range counters {
				mv.Put(name, v)
			}
			result.Put(info.Pack.ObjName, mv)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// COUNTER_MAP_REAL_TIME: map-type counter for a list of objects.
	r.Register(protocol.COUNTER_MAP_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHashLv := param.GetList("objHash")
		counterName := param.GetText("counter")
		if objHashLv == nil {
			return
		}

		for _, hv := range objHashLv.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			objHash := int32(dv.Value)
			key := cache.CounterKey{ObjHash: objHash, Counter: counterName, TimeType: cache.TimeTypeRealtime}
			v, found := counterCache.Get(key)
			if found && v != nil {
				if _, isMap := v.(*value.MapValue); isMap {
					dout.WriteByte(protocol.FLAG_HAS_NEXT)
					value.WriteValue(dout, v)
				}
			}
		}
	})
}
