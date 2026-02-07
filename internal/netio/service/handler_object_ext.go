package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// RegisterObjectExtHandlers registers extended object service handlers (P2).
func RegisterObjectExtHandlers(r *Registry, objectCache *cache.ObjectCache, deadTimeout time.Duration) {

	// OBJECT_TODAY_FULL_LIST: return all objects seen today (including dead ones).
	r.Register(protocol.OBJECT_TODAY_FULL_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		all := objectCache.GetAll()
		for _, info := range all {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, info.Pack)
		}
	})

	// OBJECT_REMOVE: mark an object as removed by deleting it from the cache.
	r.Register(protocol.OBJECT_REMOVE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHash := param.GetInt("objHash")
		objectCache.Remove(objHash)
	})
}
