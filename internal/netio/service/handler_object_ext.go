package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
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

	// OBJECT_INFO: return a single object's info by objHash.
	r.Register(protocol.OBJECT_INFO, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHash := param.GetInt("objHash")

		info, ok := objectCache.Get(objHash)
		if ok && info != nil {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, info.Pack)
		}
	})

	// OBJECT_LIST_LOAD_DATE: return objects for a given date.
	// In Go we don't have per-date disk storage for agents, so we return all cached objects.
	r.Register(protocol.OBJECT_LIST_LOAD_DATE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din) // reads date param (ignored - we only have in-memory cache)
		all := objectCache.GetAll()
		for _, info := range all {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, info.Pack)
		}
	})

	// OBJECT_REMOVE_INACTIVE: clear dead (non-alive) objects from cache.
	r.Register(protocol.OBJECT_REMOVE_INACTIVE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		objectCache.ClearInactive()
		// Return updated full list
		all := objectCache.GetAll()
		for _, info := range all {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, info.Pack)
		}
	})

	// OBJECT_REMOVE_IN_MEMORY: remove specific objects by objHash list.
	r.Register(protocol.OBJECT_REMOVE_IN_MEMORY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHashLv := param.GetList("objHash")
		if objHashLv != nil {
			for _, hv := range objHashLv.Value {
				if dv, ok := hv.(*value.DecimalValue); ok {
					objectCache.Remove(int32(dv.Value))
				}
			}
		}
		// Return updated full list
		all := objectCache.GetAll()
		for _, info := range all {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, info.Pack)
		}
	})
}
