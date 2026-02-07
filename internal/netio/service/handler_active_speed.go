package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

const counterActiveSpeed = "ActiveSpeed"

// RegisterActiveSpeedHandlers registers active speed service handlers.
func RegisterActiveSpeedHandlers(r *Registry, counterCache *cache.CounterCache, objectCache *cache.ObjectCache, deadTimeout time.Duration) {

	// ACTIVESPEED_GROUP_REAL_TIME: get active speed for a list of objHash values.
	r.Register(protocol.ACTIVESPEED_GROUP_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHashLv := param.GetList("objHash")
		if objHashLv == nil {
			return
		}

		for i := 0; i < len(objHashLv.Value); i++ {
			objHash := objHashLv.GetInt(i)
			var act1, act2, act3 int32

			key := cache.CounterKey{ObjHash: objHash, Counter: counterActiveSpeed, TimeType: cache.TimeTypeRealtime}
			v, found := counterCache.Get(key)
			if found && v != nil {
				if lv, ok := v.(*value.ListValue); ok && len(lv.Value) >= 3 {
					act1 = lv.GetInt(0)
					act2 = lv.GetInt(1)
					act3 = lv.GetInt(2)
				}
			}

			m := &pack.MapPack{}
			m.Put("objHash", objHashLv.Value[i])
			m.PutLong("act1", int64(act1))
			m.PutLong("act2", int64(act2))
			m.PutLong("act3", int64(act3))
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, m)
		}
	})

	// ACTIVESPEED_REAL_TIME: get active speed for all live objects of a type.
	r.Register(protocol.ACTIVESPEED_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType {
				continue
			}
			var act1, act2, act3 int32

			key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counterActiveSpeed, TimeType: cache.TimeTypeRealtime}
			v, found := counterCache.Get(key)
			if found && v != nil {
				if lv, ok := v.(*value.ListValue); ok && len(lv.Value) >= 3 {
					act1 = lv.GetInt(0)
					act2 = lv.GetInt(1)
					act3 = lv.GetInt(2)
				}
			}

			m := &pack.MapPack{}
			m.PutLong("objHash", int64(info.Pack.ObjHash))
			m.PutLong("act1", int64(act1))
			m.PutLong("act2", int64(act2))
			m.PutLong("act3", int64(act3))
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, m)
		}
	})

	// ACTIVESPEED_REAL_TIME_GROUP: aggregated active speed across all live objects of a type.
	r.Register(protocol.ACTIVESPEED_REAL_TIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objType := param.GetText("objType")
		if objType == "" {
			return
		}

		var act1, act2, act3 int32
		var tps float32

		live := objectCache.GetLive(deadTimeout)
		for _, info := range live {
			if info.Pack.ObjType != objType || !info.Pack.Alive {
				continue
			}

			// TPS
			tpsKey := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: "TPS", TimeType: cache.TimeTypeRealtime}
			if tv, ok := counterCache.Get(tpsKey); ok && tv != nil {
				switch v := tv.(type) {
				case *value.FloatValue:
					tps += v.Value
				case *value.DecimalValue:
					tps += float32(v.Value)
				}
			}

			// ActiveSpeed
			key := cache.CounterKey{ObjHash: info.Pack.ObjHash, Counter: counterActiveSpeed, TimeType: cache.TimeTypeRealtime}
			v, found := counterCache.Get(key)
			if found && v != nil {
				if lv, ok := v.(*value.ListValue); ok && len(lv.Value) >= 3 {
					act1 += lv.GetInt(0)
					act2 += lv.GetInt(1)
					act3 += lv.GetInt(2)
				}
			}
		}

		m := &pack.MapPack{}
		m.PutLong("act1", int64(act1))
		m.PutLong("act2", int64(act2))
		m.PutLong("act3", int64(act3))
		m.Put("tps", &value.FloatValue{Value: tps})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, m)
	})

	// ACTIVESPEED_GROUP_REAL_TIME_GROUP: aggregated active speed for a list of objHash values.
	r.Register(protocol.ACTIVESPEED_GROUP_REAL_TIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHashLv := param.GetList("objHash")
		if objHashLv == nil {
			return
		}

		var act1, act2, act3 int32
		var tps float32

		for i := 0; i < len(objHashLv.Value); i++ {
			objHash := objHashLv.GetInt(i)

			info, ok := objectCache.Get(objHash)
			if !ok || !info.Pack.Alive {
				continue
			}

			// TPS
			tpsKey := cache.CounterKey{ObjHash: objHash, Counter: "TPS", TimeType: cache.TimeTypeRealtime}
			if tv, ok := counterCache.Get(tpsKey); ok && tv != nil {
				switch v := tv.(type) {
				case *value.FloatValue:
					tps += v.Value
				case *value.DecimalValue:
					tps += float32(v.Value)
				}
			}

			// ActiveSpeed
			key := cache.CounterKey{ObjHash: objHash, Counter: counterActiveSpeed, TimeType: cache.TimeTypeRealtime}
			v, found := counterCache.Get(key)
			if found && v != nil {
				if lv, ok := v.(*value.ListValue); ok && len(lv.Value) >= 3 {
					act1 += lv.GetInt(0)
					act2 += lv.GetInt(1)
					act3 += lv.GetInt(2)
				}
			}
		}

		m := &pack.MapPack{}
		m.PutLong("act1", int64(act1))
		m.PutLong("act2", int64(act2))
		m.PutLong("act3", int64(act3))
		m.Put("tps", &value.FloatValue{Value: tps})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, m)
	})
}
