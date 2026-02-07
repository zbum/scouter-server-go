package service

import (
	"fmt"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterTextHandlers registers GET_TEXT_100 and related handlers.
func RegisterTextHandlers(r *Registry, textCache *cache.TextCache, textRD *text.TextRD) {
	// GET_TEXT_100: resolve text hashes to strings in batches of 100
	r.Register(protocol.GET_TEXT_100, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		typeName := param.GetText("type")
		hashVal := param.Get("hash")
		if hashVal == nil {
			return
		}
		hashList, ok := hashVal.(*value.ListValue)
		if !ok || len(hashList.Value) == 0 {
			return
		}

		result := &pack.MapPack{}
		count := 0
		for _, hv := range hashList.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			h := int32(dv.Value)
			text, found := textCache.Get(typeName, h)
			if found {
				key := fmt.Sprintf("%x", uint32(h))
				result.PutStr(key, text)
				count++
				if count == 100 {
					dout.WriteByte(protocol.FLAG_HAS_NEXT)
					pack.WritePack(dout, result)
					result = &pack.MapPack{}
					count = 0
				}
			}
		}
		if count > 0 {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})
}
