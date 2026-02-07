package service

import (
	"math"
	"sort"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
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

	// COUNTER_PAST_TIME_TOT: total/avg of realtime counter across all objects of a type.
	r.Register(protocol.COUNTER_PAST_TIME_TOT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")
		counterName := param.GetText("counter")
		objType := param.GetText("objType")
		mode := param.GetText("mode")
		if objType == "" {
			return
		}
		date := util.FormatDate(stime)
		startSec := int32(stime / 1000)
		endSec := int32(etime / 1000)

		type aggEntry struct {
			sum   float64
			count int
		}
		timeAgg := make(map[int32]*aggEntry)

		all := objectCache.GetAll()
		for _, info := range all {
			if info.Pack.ObjType != objType {
				continue
			}
			counterRD.ReadRealtimeRange(date, info.Pack.ObjHash, startSec, endSec, func(timeSec int32, counters map[string]value.Value) {
				if v, ok := counters[counterName]; ok {
					e, exists := timeAgg[timeSec]
					if !exists {
						e = &aggEntry{}
						timeAgg[timeSec] = e
					}
					e.count++
					e.sum += toFloat64(v)
				}
			})
		}

		if len(timeAgg) == 0 {
			return
		}

		times := make([]int32, 0, len(timeAgg))
		for t := range timeAgg {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })

		timeList := value.NewListValue()
		valueList := value.NewListValue()
		for _, t := range times {
			e := timeAgg[t]
			timeList.Value = append(timeList.Value, value.NewDecimalValue(int64(t)))
			v := e.sum
			if mode == "avg" && e.count > 0 {
				v = e.sum / float64(e.count)
			}
			valueList.Value = append(valueList.Value, &value.DoubleValue{Value: v})
		}

		result := &pack.MapPack{}
		result.Put("time", timeList)
		result.Put("value", valueList)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// COUNTER_PAST_TIME_GROUP: realtime counter for a list of objHashes.
	r.Register(protocol.COUNTER_PAST_TIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")
		objHashLv := param.GetList("objHash")
		if objHashLv == nil {
			return
		}
		date := util.FormatDate(stime)
		startSec := int32(stime / 1000)
		endSec := int32(etime / 1000)

		for _, hv := range objHashLv.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			objHash := int32(dv.Value)

			timeList := value.NewListValue()
			valueList := value.NewListValue()

			counterRD.ReadRealtimeRange(date, objHash, startSec, endSec, func(timeSec int32, counters map[string]value.Value) {
				if v, ok := counters[counterName]; ok {
					timeList.Value = append(timeList.Value, value.NewDecimalValue(int64(timeSec)))
					valueList.Value = append(valueList.Value, v)
				}
			})

			result := &pack.MapPack{}
			result.PutLong("objHash", int64(objHash))
			result.Put("time", timeList)
			result.Put("value", valueList)
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	// COUNTER_PAST_DATE_TOT: total/avg of daily counter across all objects of a type.
	r.Register(protocol.COUNTER_PAST_DATE_TOT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		date := param.GetText("date")
		mode := param.GetText("mode")
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		values := make([]float64, util.BucketsPerDay)
		cnt := make([]int, util.BucketsPerDay)

		all := objectCache.GetAll()
		for _, info := range all {
			if info.Pack.ObjType != objType {
				continue
			}
			dv, err := counterRD.ReadDailyAll(date, info.Pack.ObjHash, counterName)
			if err != nil || dv == nil {
				continue
			}
			for j, v := range dv {
				if j >= util.BucketsPerDay {
					break
				}
				if !math.IsNaN(v) && v != 0 {
					cnt[j]++
					values[j] += v
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

	// COUNTER_PAST_DATE_GROUP: daily counter for a list of objHashes.
	r.Register(protocol.COUNTER_PAST_DATE_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		date := param.GetText("date")
		objHashLv := param.GetList("objHash")
		if objHashLv == nil {
			return
		}
		stime := util.DateToMillis(date)

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
					t := stime + int64(j)*int64(util.MillisPerFiveMinute)
					timeList.Value = append(timeList.Value, value.NewDecimalValue(t))
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

	// COUNTER_PAST_LONGDATE_ALL: daily counter across multiple days for objects.
	r.Register(protocol.COUNTER_PAST_LONGDATE_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		sDate := param.GetText("sDate")
		eDate := param.GetText("eDate")
		objType := param.GetText("objType")
		objHashLv := param.GetList("objHash")

		// Determine object list
		var objHashes []int32
		if objHashLv != nil && len(objHashLv.Value) > 0 {
			for _, hv := range objHashLv.Value {
				if dv, ok := hv.(*value.DecimalValue); ok {
					objHashes = append(objHashes, int32(dv.Value))
				}
			}
		} else if objType != "" {
			for _, info := range objectCache.GetAll() {
				if info.Pack.ObjType == objType {
					objHashes = append(objHashes, info.Pack.ObjHash)
				}
			}
		}

		stime := util.DateToMillis(sDate)
		etime := util.DateToMillis(eDate) + int64(util.MillisPerDay)

		for date := stime; date <= etime-int64(util.MillisPerDay); date += int64(util.MillisPerDay) {
			d := util.FormatDate(date)
			for _, objHash := range objHashes {
				timeList := value.NewListValue()
				valueList := value.NewListValue()

				v, err := counterRD.ReadDailyAll(d, objHash, counterName)
				if err == nil && v != nil {
					for j, val := range v {
						t := date + int64(j)*int64(util.MillisPerFiveMinute)
						timeList.Value = append(timeList.Value, value.NewDecimalValue(t))
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
		}
	})

	// COUNTER_PAST_LONGDATE_TOT: total/avg daily counter across multiple days.
	r.Register(protocol.COUNTER_PAST_LONGDATE_TOT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		sDate := param.GetText("sDate")
		eDate := param.GetText("eDate")
		mode := param.GetText("mode")
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		stime := util.DateToMillis(sDate)
		etime := util.DateToMillis(eDate) + int64(util.MillisPerDay)
		totalBuckets := int((etime - stime) / int64(util.MillisPerFiveMinute))
		if totalBuckets <= 0 || totalBuckets > 100000 {
			return
		}

		values := make([]float64, totalBuckets)
		cnt := make([]int, totalBuckets)

		dayPointer := 0
		for date := stime; date <= etime-int64(util.MillisPerDay); date += int64(util.MillisPerDay) {
			d := util.FormatDate(date)
			for _, info := range objectCache.GetAll() {
				if info.Pack.ObjType != objType {
					continue
				}
				v, err := counterRD.ReadDailyAll(d, info.Pack.ObjHash, counterName)
				if err != nil || v == nil {
					continue
				}
				for j, val := range v {
					idx := dayPointer + j
					if idx >= totalBuckets {
						break
					}
					if !math.IsNaN(val) && val > 0 {
						cnt[idx]++
						values[idx] += val
					}
				}
			}
			dayPointer += util.BucketsPerDay
		}

		isAvg := mode == "avg"
		timeList := value.NewListValue()
		valueList := value.NewListValue()
		for i := 0; i < totalBuckets; i++ {
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

	// COUNTER_PAST_LONGDATE_GROUP: daily counter across multiple days for a list of objHashes.
	r.Register(protocol.COUNTER_PAST_LONGDATE_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")
		objHashLv := param.GetList("objHash")
		if objHashLv == nil {
			return
		}
		lastDay := util.FormatDate(etime)

		for _, hv := range objHashLv.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			objHash := int32(dv.Value)

			timeList := value.NewListValue()
			valueList := value.NewListValue()

			t := stime
			var date string
			for date != lastDay {
				date = util.FormatDate(t)
				oclock := util.DateToMillis(date)
				v, err := counterRD.ReadDailyAll(date, objHash, counterName)
				if err == nil && v != nil {
					for j, val := range v {
						timeList.Value = append(timeList.Value, value.NewDecimalValue(oclock+int64(j)*int64(util.MillisPerFiveMinute)))
						if math.IsNaN(val) {
							valueList.Value = append(valueList.Value, &value.NullValue{})
						} else {
							valueList.Value = append(valueList.Value, &value.DoubleValue{Value: val})
						}
					}
				} else {
					for j := 0; j < util.BucketsPerDay; j++ {
						timeList.Value = append(timeList.Value, value.NewDecimalValue(oclock+int64(j)*int64(util.MillisPerFiveMinute)))
						valueList.Value = append(valueList.Value, &value.NullValue{})
					}
				}
				t += int64(util.MillisPerDay)
			}

			result := &pack.MapPack{}
			result.PutLong("objHash", int64(objHash))
			result.Put("time", timeList)
			result.Put("value", valueList)
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	// GET_COUNTER_EXIST_DAYS: check which days have counter data.
	r.Register(protocol.GET_COUNTER_EXIST_DAYS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counter")
		objType := param.GetText("objType")
		duration := param.GetInt("duration")
		etime := param.GetLong("etime")
		stime := etime - int64(duration)*int64(util.MillisPerDay)

		dateLv := value.NewListValue()
		existLv := value.NewListValue()

		t := stime
		for i := int32(0); i <= duration; i++ {
			d := util.FormatDate(t)
			found := false
			for _, info := range objectCache.GetAll() {
				if info.Pack.ObjType != objType {
					continue
				}
				v, err := counterRD.ReadDailyAll(d, info.Pack.ObjHash, counterName)
				if err == nil && v != nil && len(v) > 0 {
					found = true
					break
				}
			}
			dateLv.Value = append(dateLv.Value, value.NewTextValue(d))
			existLv.Value = append(existLv.Value, &value.BooleanValue{Value: found})
			t += int64(util.MillisPerDay)
		}

		result := &pack.MapPack{}
		result.Put("date", dateLv)
		result.Put("exist", existLv)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})
}

func toFloat64(v value.Value) float64 {
	switch tv := v.(type) {
	case *value.DecimalValue:
		return float64(tv.Value)
	case *value.FloatValue:
		return float64(tv.Value)
	case *value.DoubleValue:
		return tv.Value
	}
	return 0
}
