package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// RegisterObjectHandlers registers OBJECT_LIST_REAL_TIME and related handlers.
func RegisterObjectHandlers(r *Registry, objectCache *cache.ObjectCache, deadTimeout time.Duration) {
	r.Register(protocol.OBJECT_LIST_REAL_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		all := objectCache.GetAll()
		for _, info := range all {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, info.Pack)
		}
	})
}
