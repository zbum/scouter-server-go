package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterCounterHandlers registers COUNTER_REAL_TIME and COUNTER_REAL_TIME_ALL handlers.
func RegisterCounterHandlers(r *Registry, counterCache *cache.CounterCache, objectCache *cache.ObjectCache, deadTimeout time.Duration, counterRD *counter.CounterRD) {
	// COUNTER_REAL_TIME: get a single counter value for a specific object
	r.Register(protocol.COUNTER_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		objHash := param.GetInt("objHash")
		counter := param.GetText("counter")

		key := cache.CounterKey{ObjHash: objHash, Counter: counter, TimeType: cache.TimeTypeRealtime}
		v, ok := counterCache.Get(key)
		if ok && v != nil {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			value.WriteValue(dout, v)
		}
	})

	// COUNTER_REAL_TIME_GROUP: get counter values for a specific list of objects
	r.Register(protocol.COUNTER_REAL_TIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		counterName := param.GetText("counter")
		objHashLv := param.GetList("objHash")

		mpack := &pack.MapPack{}
		instList := value.NewListValue()
		valueList := value.NewListValue()

		if objHashLv != nil {
			for i := 0; i < len(objHashLv.Value); i++ {
				objHash := objHashLv.GetInt(i)
				key := cache.CounterKey{ObjHash: objHash, Counter: counterName, TimeType: cache.TimeTypeRealtime}
				v, ok := counterCache.Get(key)
				if ok && v != nil {
					instList.Value = append(instList.Value, objHashLv.Value[i])
					valueList.Value = append(valueList.Value, v)
				}
			}
		}

		mpack.Put("objHash", instList)
		mpack.Put("value", valueList)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, mpack)
	})

	// COUNTER_REAL_TIME_ALL: get a counter value for all live objects of a type
	r.Register(protocol.COUNTER_REAL_TIME_ALL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		counter := param.GetText("counter")
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		mpack := &pack.MapPack{}
		objHashList := value.NewListValue()
		valueList := value.NewListValue()

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}
			key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counter, TimeType: cache.TimeTypeRealtime}
			v, ok := counterCache.Get(key)
			if ok && v != nil {
				objHashList.Value = append(objHashList.Value, value.NewDecimalValue(int64(info.Pack.ObjHash)))
				valueList.Value = append(valueList.Value, v)
			}
		}

		mpack.Put("objHash", objHashList)
		mpack.Put("value", valueList)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, mpack)
	})
}
