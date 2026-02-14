package pack

import (
	"testing"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

func TestMapPack(t *testing.T) {
	// Create a MapPack
	mp := &MapPack{}
	mp.PutStr("key1", "value1")
	mp.PutLong("key2", int64(123))

	// Write to buffer
	out := protocol.NewDataOutputX()
	WritePack(out, mp)

	// Read from buffer
	in := protocol.NewDataInputX(out.ToByteArray())
	pack, err := ReadPack(in)
	if err != nil {
		t.Fatalf("Failed to read pack: %v", err)
	}

	// Verify type
	readMap, ok := pack.(*MapPack)
	if !ok {
		t.Fatalf("Expected MapPack, got %T", pack)
	}

	// Verify values
	if readMap.GetText("key1") != "value1" {
		t.Errorf("Expected 'value1', got '%s'", readMap.GetText("key1"))
	}
	if readMap.GetLong("key2") != 123 {
		t.Errorf("Expected 123, got %d", readMap.GetLong("key2"))
	}
}

func TestTextPack(t *testing.T) {
	// Create a TextPack
	tp := &TextPack{
		XType: "service",
		Hash:  12345,
		Text:  "MyService",
	}

	// Write to buffer
	out := protocol.NewDataOutputX()
	WritePack(out, tp)

	// Read from buffer
	in := protocol.NewDataInputX(out.ToByteArray())
	pack, err := ReadPack(in)
	if err != nil {
		t.Fatalf("Failed to read pack: %v", err)
	}

	// Verify type
	readText, ok := pack.(*TextPack)
	if !ok {
		t.Fatalf("Expected TextPack, got %T", pack)
	}

	// Verify values
	if readText.XType != "service" {
		t.Errorf("Expected 'service', got '%s'", readText.XType)
	}
	if readText.Hash != 12345 {
		t.Errorf("Expected 12345, got %d", readText.Hash)
	}
	if readText.Text != "MyService" {
		t.Errorf("Expected 'MyService', got '%s'", readText.Text)
	}
}

func TestXLogPack(t *testing.T) {
	// Create an XLogPack
	xp := &XLogPack{
		EndTime:  1234567890,
		ObjHash:  100,
		Service:  200,
		Txid:     999888777,
		Elapsed:  1500,
		Error:    0,
		Cpu:      100,
		SqlCount: 5,
		SqlTime:  200,
		Kbytes:   64,
		Status:   200,
	}

	// Write to buffer
	out := protocol.NewDataOutputX()
	WritePack(out, xp)

	// Read from buffer
	in := protocol.NewDataInputX(out.ToByteArray())
	pack, err := ReadPack(in)
	if err != nil {
		t.Fatalf("Failed to read pack: %v", err)
	}

	// Verify type
	readXLog, ok := pack.(*XLogPack)
	if !ok {
		t.Fatalf("Expected XLogPack, got %T", pack)
	}

	// Verify values
	if readXLog.EndTime != 1234567890 {
		t.Errorf("Expected 1234567890, got %d", readXLog.EndTime)
	}
	if readXLog.ObjHash != 100 {
		t.Errorf("Expected 100, got %d", readXLog.ObjHash)
	}
	if readXLog.Service != 200 {
		t.Errorf("Expected 200, got %d", readXLog.Service)
	}
	if readXLog.Elapsed != 1500 {
		t.Errorf("Expected 1500, got %d", readXLog.Elapsed)
	}
}

func TestObjectPack(t *testing.T) {
	// Create an ObjectPack
	op := &ObjectPack{
		ObjType: "java",
		ObjHash: 12345,
		ObjName: "MyApp",
		Address: "10.0.0.1",
		Version: "1.0.0",
		Alive:   true,
		Wakeup:  1234567890,
		Tags:    value.NewMapValue(),
	}
	op.Tags.Put("region", value.NewTextValue("us-west"))

	// Write to buffer
	out := protocol.NewDataOutputX()
	WritePack(out, op)

	// Read from buffer
	in := protocol.NewDataInputX(out.ToByteArray())
	pack, err := ReadPack(in)
	if err != nil {
		t.Fatalf("Failed to read pack: %v", err)
	}

	// Verify type
	readObj, ok := pack.(*ObjectPack)
	if !ok {
		t.Fatalf("Expected ObjectPack, got %T", pack)
	}

	// Verify values
	if readObj.ObjType != "java" {
		t.Errorf("Expected 'java', got '%s'", readObj.ObjType)
	}
	if readObj.ObjName != "MyApp" {
		t.Errorf("Expected 'MyApp', got '%s'", readObj.ObjName)
	}
	if !readObj.Alive {
		t.Errorf("Expected Alive to be true")
	}
	if readObj.Tags == nil {
		t.Fatal("Tags should not be nil")
	}
}

func TestReadXLogFilterFields(t *testing.T) {
	tests := []struct {
		name    string
		objHash int32
		elapsed int32
	}{
		{"zero values", 0, 0},
		{"small values", 42, 150},
		{"typical values", 123456, 1500},
		{"negative objHash", -999, 300},
		{"large elapsed", 100, 2147483647},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xp := &XLogPack{
				EndTime: 1700000000000,
				ObjHash: tt.objHash,
				Service: 999,
				Txid:    111222333,
				Caller:  444555666,
				Gxid:    777888999,
				Elapsed: tt.elapsed,
				Error:   1,
			}

			out := protocol.NewDataOutputX()
			WritePack(out, xp)
			data := out.ToByteArray()

			objHash, elapsed, err := ReadXLogFilterFields(data)
			if err != nil {
				t.Fatalf("ReadXLogFilterFields failed: %v", err)
			}
			if objHash != tt.objHash {
				t.Errorf("ObjHash: expected %d, got %d", tt.objHash, objHash)
			}
			if elapsed != tt.elapsed {
				t.Errorf("Elapsed: expected %d, got %d", tt.elapsed, elapsed)
			}
		})
	}
}

func TestAllPackTypes(t *testing.T) {
	packTypes := []byte{
		PackTypeMap,
		PackTypeXLog,
		PackTypeDroppedXLog,
		PackTypeXLogProfile,
		PackTypeXLogProfile2,
		PackTypeSpan,
		PackTypeSpanContainer,
		PackTypeText,
		PackTypePerfCounter,
		PackTypePerfStatus,
		PackTypeStack,
		PackTypeSummary,
		PackTypeBatch,
		PackTypePerfInteractionCounter,
		PackTypeAlert,
		PackTypeObject,
	}

	for _, typeCode := range packTypes {
		pack, err := CreatePack(typeCode)
		if err != nil {
			t.Errorf("Failed to create pack type %d: %v", typeCode, err)
			continue
		}
		if pack.PackType() != typeCode {
			t.Errorf("Pack type mismatch: expected %d, got %d", typeCode, pack.PackType())
		}
	}
}
