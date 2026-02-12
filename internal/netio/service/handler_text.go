package service

import (
	"log/slog"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// resolveText looks up text by hash: memory cache → disk.
func resolveText(textCache *cache.TextCache, textRD *text.TextRD, typeName string, h int32) (string, bool) {
	if txt, found := textCache.Get(typeName, h); found {
		return txt, true
	}
	if textRD == nil {
		return "", false
	}
	if diskTxt, err := textRD.GetString(typeName, h); err == nil && diskTxt != "" {
		textCache.Put(typeName, h, diskTxt)
		return diskTxt, true
	}
	return "", false
}

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

			txt, found := resolveText(textCache, textRD, typeName, h)
			if found {
				key := util.Hexa32ToString32(h)
				result.PutStr(key, txt)
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

	// GET_TEXT_PACK: resolve text hashes, return as TextPack stream
	r.Register(protocol.GET_TEXT_PACK, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
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

		for _, hv := range hashList.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			h := int32(dv.Value)

			txt, found := resolveText(textCache, textRD, typeName, h)
			if found {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, &pack.TextPack{
					XType: typeName,
					Hash:  h,
					Text:  txt,
				})
			}
		}
	})

	// GET_TEXT_ANY_TYPE: resolve mixed-type text hashes
	r.Register(protocol.GET_TEXT_ANY_TYPE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		typeVal := param.Get("type")
		hashVal := param.Get("hash")
		if typeVal == nil || hashVal == nil {
			return
		}
		typeList, ok1 := typeVal.(*value.ListValue)
		hashList, ok2 := hashVal.(*value.ListValue)
		if !ok1 || !ok2 || len(hashList.Value) == 0 {
			return
		}

		for i, hv := range hashList.Value {
			if i >= len(typeList.Value) {
				break
			}
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			h := int32(dv.Value)

			tv, ok := typeList.Value[i].(*value.TextValue)
			if !ok {
				continue
			}
			typeName := tv.Value

			txt, found := resolveText(textCache, textRD, typeName, h)
			if found {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, &pack.TextPack{
					XType: typeName,
					Hash:  h,
					Text:  txt,
				})
			}
		}
	})

	// GET_TEXT: resolve text hashes to strings (single MapPack response, no batching)
	r.Register(protocol.GET_TEXT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
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
		for _, hv := range hashList.Value {
			dv, ok := hv.(*value.DecimalValue)
			if !ok {
				continue
			}
			h := int32(dv.Value)

			txt, found := resolveText(textCache, textRD, typeName, h)
			if found {
				key := util.Hexa32ToString32(h)
				result.PutStr(key, txt)
			}
		}

		if result.Size() > 0 {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	slog.Debug("TextHandlers registered", "commands", "GET_TEXT, GET_TEXT_100, GET_TEXT_PACK, GET_TEXT_ANY_TYPE")
}
