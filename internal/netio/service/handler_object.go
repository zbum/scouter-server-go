package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/counter"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterObjectHandlers registers OBJECT_LIST_REAL_TIME and related handlers.
func RegisterObjectHandlers(r *Registry, objectCache *cache.ObjectCache, deadTimeout time.Duration, counterCache *cache.CounterCache, typeManager *counter.ObjectTypeManager) {
	r.Register(protocol.OBJECT_LIST_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		all := objectCache.GetAll()
		for _, info := range all {
			p := info.Pack
			if p.Alive {
				masterCounter := typeManager.GetMasterCounter(p.ObjType)
				if masterCounter != "" {
					key := cache.CounterKey{ObjHash: p.ObjHash, Counter: masterCounter, TimeType: cache.TimeTypeRealtime}
					v, found := counterCache.Get(key)
					if found && v != nil {
						if p.Tags == nil {
							p.Tags = value.NewMapValue()
						}
						p.Tags.Put("counter", v)
					}
				}
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, p)
		}
	})
}
