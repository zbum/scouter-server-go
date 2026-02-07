package service

import (
	"github.com/zbum/scouter-server-go/internal/db/kv"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterKVHandlers registers handlers for KV store operations.
func RegisterKVHandlers(r *Registry, globalKV, customKV *kv.KVStore) {

	// GET_GLOBAL_KV: retrieve a value from the global namespace
	r.Register(protocol.GET_GLOBAL_KV, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		key := param.GetText("key")

		response := &pack.MapPack{}
		if val, ok := globalKV.Get(key); ok {
			response.PutStr("value", val)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// SET_GLOBAL_KV: store a key-value pair in the global namespace
	r.Register(protocol.SET_GLOBAL_KV, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		key := param.GetText("key")
		val := param.GetText("value")

		globalKV.Set(key, val)

		response := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// SET_GLOBAL_TTL: store a key-value pair with TTL in the global namespace
	r.Register(protocol.SET_GLOBAL_TTL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		key := param.GetText("key")
		val := param.GetText("value")
		ttl := param.GetLong("ttl")

		globalKV.SetTTL(key, val, ttl)

		response := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// GET_CUSTOM_KV: retrieve a value from the custom namespace
	r.Register(protocol.GET_CUSTOM_KV, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		key := param.GetText("key")

		response := &pack.MapPack{}
		if val, ok := customKV.Get(key); ok {
			response.PutStr("value", val)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// SET_CUSTOM_KV: store a key-value pair in the custom namespace
	r.Register(protocol.SET_CUSTOM_KV, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		key := param.GetText("key")
		val := param.GetText("value")

		customKV.Set(key, val)

		response := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// SET_CUSTOM_TTL: store a key-value pair with TTL in the custom namespace
	r.Register(protocol.SET_CUSTOM_TTL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		key := param.GetText("key")
		val := param.GetText("value")
		ttl := param.GetLong("ttl")

		customKV.SetTTL(key, val, ttl)

		response := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// GET_GLOBAL_KV_BULK: retrieve multiple values from the global namespace
	r.Register(protocol.GET_GLOBAL_KV_BULK, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		// Extract keys from ListValue
		keysVal := param.Get("keys")
		keys := make([]string, 0)
		if keysVal != nil {
			if listVal, ok := keysVal.(*value.ListValue); ok {
				for _, v := range listVal.Value {
					if textVal, ok := v.(*value.TextValue); ok {
						keys = append(keys, textVal.Value)
					}
				}
			}
		}

		// Get bulk values
		result := globalKV.GetBulk(keys)

		// Build response MapPack with key-value pairs
		response := &pack.MapPack{}
		for k, v := range result {
			response.PutStr(k, v)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// SET_GLOBAL_KV_BULK: store multiple key-value pairs in the global namespace
	r.Register(protocol.SET_GLOBAL_KV_BULK, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		// Extract key-value pairs from MapPack
		pairs := make(map[string]string)
		for _, entry := range param.Table {
			if textVal, ok := entry.Val.(*value.TextValue); ok {
				pairs[entry.Key] = textVal.Value
			}
		}

		globalKV.SetBulk(pairs)

		response := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// GET_CUSTOM_KV_BULK: retrieve multiple values from the custom namespace
	r.Register(protocol.GET_CUSTOM_KV_BULK, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		// Extract keys from ListValue
		keysVal := param.Get("keys")
		keys := make([]string, 0)
		if keysVal != nil {
			if listVal, ok := keysVal.(*value.ListValue); ok {
				for _, v := range listVal.Value {
					if textVal, ok := v.(*value.TextValue); ok {
						keys = append(keys, textVal.Value)
					}
				}
			}
		}

		// Get bulk values
		result := customKV.GetBulk(keys)

		// Build response MapPack with key-value pairs
		response := &pack.MapPack{}
		for k, v := range result {
			response.PutStr(k, v)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})

	// SET_CUSTOM_KV_BULK: store multiple key-value pairs in the custom namespace
	r.Register(protocol.SET_CUSTOM_KV_BULK, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)

		// Extract key-value pairs from MapPack
		pairs := make(map[string]string)
		for _, entry := range param.Table {
			if textVal, ok := entry.Val.(*value.TextValue); ok {
				pairs[entry.Key] = textVal.Value
			}
		}

		customKV.SetBulk(pairs)

		response := &pack.MapPack{}
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, response)
	})
}
