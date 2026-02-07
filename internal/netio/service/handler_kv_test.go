package service

import (
	"testing"

	"github.com/zbum/scouter-server-go/internal/db/kv"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

func TestKVHandlers_SetAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	globalKV := kv.NewKVStore(tmpDir, "test_global.json")
	customKV := kv.NewKVStore(tmpDir, "test_custom.json")
	defer globalKV.Close()
	defer customKV.Close()

	registry := NewRegistry()
	RegisterKVHandlers(registry, globalKV, customKV)

	// Test SET_GLOBAL_KV
	t.Run("SET_GLOBAL_KV", func(t *testing.T) {
		handler := registry.Get(protocol.SET_GLOBAL_KV)
		if handler == nil {
			t.Fatal("SET_GLOBAL_KV handler not registered")
		}

		// Prepare request
		req := &pack.MapPack{}
		req.PutStr("key", "test_key")
		req.PutStr("value", "test_value")

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Verify response
		respIn := protocol.NewDataInputX(respOut.ToByteArray())
		flag, err := respIn.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read response flag: %v", err)
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Errorf("Expected FLAG_HAS_NEXT, got %d", flag)
		}

		// Verify value was stored
		val, ok := globalKV.Get("test_key")
		if !ok || val != "test_value" {
			t.Errorf("Value not stored correctly: got (%v, %v), want (test_value, true)", val, ok)
		}
	})

	// Test GET_GLOBAL_KV
	t.Run("GET_GLOBAL_KV", func(t *testing.T) {
		handler := registry.Get(protocol.GET_GLOBAL_KV)
		if handler == nil {
			t.Fatal("GET_GLOBAL_KV handler not registered")
		}

		// Store a value first
		globalKV.Set("retrieve_key", "retrieve_value")

		// Prepare request
		req := &pack.MapPack{}
		req.PutStr("key", "retrieve_key")

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Parse response
		respIn := protocol.NewDataInputX(respOut.ToByteArray())
		flag, err := respIn.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read response flag: %v", err)
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Errorf("Expected FLAG_HAS_NEXT, got %d", flag)
		}

		respPack, err := pack.ReadPack(respIn)
		if err != nil {
			t.Fatalf("Failed to read response pack: %v", err)
		}

		response := respPack.(*pack.MapPack)
		val := response.GetText("value")
		if val != "retrieve_value" {
			t.Errorf("Got value %q, want %q", val, "retrieve_value")
		}
	})

	// Test GET_GLOBAL_KV for non-existent key
	t.Run("GET_GLOBAL_KV_NonExistent", func(t *testing.T) {
		handler := registry.Get(protocol.GET_GLOBAL_KV)

		// Prepare request
		req := &pack.MapPack{}
		req.PutStr("key", "nonexistent_key")

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Parse response
		respIn := protocol.NewDataInputX(respOut.ToByteArray())
		flag, _ := respIn.ReadByte()
		if flag != protocol.FLAG_HAS_NEXT {
			t.Errorf("Expected FLAG_HAS_NEXT, got %d", flag)
		}

		respPack, _ := pack.ReadPack(respIn)
		response := respPack.(*pack.MapPack)
		val := response.GetText("value")
		if val != "" {
			t.Errorf("Expected empty value for nonexistent key, got %q", val)
		}
	})
}

func TestKVHandlers_SetAndGetCustom(t *testing.T) {
	tmpDir := t.TempDir()
	globalKV := kv.NewKVStore(tmpDir, "test_global.json")
	customKV := kv.NewKVStore(tmpDir, "test_custom.json")
	defer globalKV.Close()
	defer customKV.Close()

	registry := NewRegistry()
	RegisterKVHandlers(registry, globalKV, customKV)

	// Test SET_CUSTOM_KV
	t.Run("SET_CUSTOM_KV", func(t *testing.T) {
		handler := registry.Get(protocol.SET_CUSTOM_KV)
		if handler == nil {
			t.Fatal("SET_CUSTOM_KV handler not registered")
		}

		// Prepare request
		req := &pack.MapPack{}
		req.PutStr("key", "custom_key")
		req.PutStr("value", "custom_value")

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Verify value was stored in custom namespace
		val, ok := customKV.Get("custom_key")
		if !ok || val != "custom_value" {
			t.Errorf("Value not stored correctly in custom namespace: got (%v, %v), want (custom_value, true)", val, ok)
		}

		// Verify value NOT in global namespace
		_, ok = globalKV.Get("custom_key")
		if ok {
			t.Errorf("Value should not be in global namespace")
		}
	})
}

func TestKVHandlers_Bulk(t *testing.T) {
	tmpDir := t.TempDir()
	globalKV := kv.NewKVStore(tmpDir, "test_global.json")
	customKV := kv.NewKVStore(tmpDir, "test_custom.json")
	defer globalKV.Close()
	defer customKV.Close()

	registry := NewRegistry()
	RegisterKVHandlers(registry, globalKV, customKV)

	// Test SET_GLOBAL_KV_BULK
	t.Run("SET_GLOBAL_KV_BULK", func(t *testing.T) {
		handler := registry.Get(protocol.SET_GLOBAL_KV_BULK)
		if handler == nil {
			t.Fatal("SET_GLOBAL_KV_BULK handler not registered")
		}

		// Prepare request with multiple key-value pairs
		req := &pack.MapPack{}
		req.PutStr("bulk_key1", "bulk_value1")
		req.PutStr("bulk_key2", "bulk_value2")
		req.PutStr("bulk_key3", "bulk_value3")

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Verify values were stored
		val, ok := globalKV.Get("bulk_key1")
		if !ok || val != "bulk_value1" {
			t.Errorf("bulk_key1 not stored correctly")
		}
		val, ok = globalKV.Get("bulk_key2")
		if !ok || val != "bulk_value2" {
			t.Errorf("bulk_key2 not stored correctly")
		}
		val, ok = globalKV.Get("bulk_key3")
		if !ok || val != "bulk_value3" {
			t.Errorf("bulk_key3 not stored correctly")
		}
	})

	// Test GET_GLOBAL_KV_BULK
	t.Run("GET_GLOBAL_KV_BULK", func(t *testing.T) {
		handler := registry.Get(protocol.GET_GLOBAL_KV_BULK)
		if handler == nil {
			t.Fatal("GET_GLOBAL_KV_BULK handler not registered")
		}

		// Prepare request with keys list
		req := &pack.MapPack{}
		keysList := value.NewListValue()
		keysList.Value = append(keysList.Value, value.NewTextValue("bulk_key1"))
		keysList.Value = append(keysList.Value, value.NewTextValue("bulk_key2"))
		keysList.Value = append(keysList.Value, value.NewTextValue("nonexistent"))
		req.Put("keys", keysList)

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Parse response
		respIn := protocol.NewDataInputX(respOut.ToByteArray())
		flag, err := respIn.ReadByte()
		if err != nil {
			t.Fatalf("Failed to read response flag: %v", err)
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Errorf("Expected FLAG_HAS_NEXT, got %d", flag)
		}

		respPack, err := pack.ReadPack(respIn)
		if err != nil {
			t.Fatalf("Failed to read response pack: %v", err)
		}

		response := respPack.(*pack.MapPack)

		// Verify response contains expected keys
		val1 := response.GetText("bulk_key1")
		if val1 != "bulk_value1" {
			t.Errorf("Got bulk_key1=%q, want %q", val1, "bulk_value1")
		}

		val2 := response.GetText("bulk_key2")
		if val2 != "bulk_value2" {
			t.Errorf("Got bulk_key2=%q, want %q", val2, "bulk_value2")
		}

		// Nonexistent key should not be present
		valNone := response.GetText("nonexistent")
		if valNone != "" {
			t.Errorf("Expected nonexistent key to return empty, got %q", valNone)
		}
	})
}

func TestKVHandlers_TTL(t *testing.T) {
	tmpDir := t.TempDir()
	globalKV := kv.NewKVStore(tmpDir, "test_global.json")
	customKV := kv.NewKVStore(tmpDir, "test_custom.json")
	defer globalKV.Close()
	defer customKV.Close()

	registry := NewRegistry()
	RegisterKVHandlers(registry, globalKV, customKV)

	// Test SET_GLOBAL_TTL
	t.Run("SET_GLOBAL_TTL", func(t *testing.T) {
		handler := registry.Get(protocol.SET_GLOBAL_TTL)
		if handler == nil {
			t.Fatal("SET_GLOBAL_TTL handler not registered")
		}

		// Prepare request
		req := &pack.MapPack{}
		req.PutStr("key", "ttl_key")
		req.PutStr("value", "ttl_value")
		req.PutLong("ttl", 5000) // 5 seconds

		// Serialize request
		dout := protocol.NewDataOutputX()
		pack.WritePack(dout, req)

		// Prepare response
		respOut := protocol.NewDataOutputX()

		// Execute handler
		din := protocol.NewDataInputX(dout.ToByteArray())
		handler(din, respOut, true)

		// Verify value was stored with TTL
		val, ok := globalKV.Get("ttl_key")
		if !ok || val != "ttl_value" {
			t.Errorf("TTL value not stored correctly: got (%v, %v), want (ttl_value, true)", val, ok)
		}
	})
}
