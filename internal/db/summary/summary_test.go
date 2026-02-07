package summary

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// serializeSummary creates a serialized SummaryPack for testing.
func serializeSummary(t *testing.T, stype byte, key string, timeMs int64) []byte {
	t.Helper()
	table := value.NewMapValue()
	table.Put("key", value.NewTextValue(key))
	table.Put("count", value.NewDecimalValue(100))

	sp := &pack.SummaryPack{
		Time:    timeMs,
		ObjHash: 123,
		ObjType: "java",
		SType:   stype,
		Table:   table,
	}
	o := protocol.NewDataOutputX()
	pack.WritePack(o, sp)
	return o.ToByteArray()
}

func TestSummaryData_WriteRead(t *testing.T) {
	dir := t.TempDir()

	sd, err := NewSummaryData(dir, 1) // APP type
	if err != nil {
		t.Fatalf("NewSummaryData failed: %v", err)
	}
	defer sd.Close()

	timeMs := time.Now().UnixMilli()
	summaryBytes := serializeSummary(t, 1, "service1", timeMs)

	if err := sd.Write(timeMs, summaryBytes); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := sd.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	var results [][]byte
	err = sd.ReadRange(timeMs-1000, timeMs+1000, func(readTime int64, data []byte) {
		results = append(results, data)
	})
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Deserialize and verify
	din := protocol.NewDataInputX(results[0])
	p, err := pack.ReadPack(din)
	if err != nil {
		t.Fatalf("ReadPack failed: %v", err)
	}
	sp := p.(*pack.SummaryPack)
	if sp.SType != 1 {
		t.Fatalf("expected SType 1, got %d", sp.SType)
	}
	if sp.ObjHash != 123 {
		t.Fatalf("expected objHash 123, got %d", sp.ObjHash)
	}
}

func TestSummaryData_MultipleSummaries(t *testing.T) {
	dir := t.TempDir()

	sd, err := NewSummaryData(dir, 2) // SQL type
	if err != nil {
		t.Fatalf("NewSummaryData failed: %v", err)
	}
	defer sd.Close()

	baseTime := time.Now().UnixMilli()
	keys := []string{"sql1", "sql2", "sql3"}

	for i, key := range keys {
		timeMs := baseTime + int64(i)*1000 // 1 second apart
		summaryBytes := serializeSummary(t, 2, key, timeMs)
		if err := sd.Write(timeMs, summaryBytes); err != nil {
			t.Fatalf("Write[%d] failed: %v", i, err)
		}
	}
	if err := sd.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read all in range
	var results [][]byte
	err = sd.ReadRange(baseTime-1000, baseTime+5000, func(readTime int64, data []byte) {
		results = append(results, data)
	})
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify each entry
	for i, data := range results {
		din := protocol.NewDataInputX(data)
		p, err := pack.ReadPack(din)
		if err != nil {
			t.Fatalf("ReadPack[%d] failed: %v", i, err)
		}
		sp := p.(*pack.SummaryPack)
		if sp.SType != 2 {
			t.Fatalf("expected SType 2, got %d", sp.SType)
		}
		keyVal, ok := sp.Table.Get("key")
		if !ok || keyVal == nil {
			t.Fatalf("expected key field in table")
		}
		keyText := keyVal.(*value.TextValue).Value
		if keyText != keys[i] {
			t.Fatalf("expected key %q, got %q", keys[i], keyText)
		}
	}
}

func TestSummaryWR_Async(t *testing.T) {
	baseDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr := NewSummaryWR(baseDir)
	wr.Start(ctx)

	timeMs := time.Now().UnixMilli()
	summaryBytes := serializeSummary(t, 3, "api1", timeMs) // APICALL type

	wr.Add(&SummaryEntry{
		TimeMs: timeMs,
		SType:  3,
		Data:   summaryBytes,
	})

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)
	wr.Close()

	// Now read it back using SummaryRD
	rd := NewSummaryRD(baseDir)
	defer rd.Close()

	date := time.UnixMilli(timeMs).Format("20060102")
	var results [][]byte
	err := rd.ReadRange(date, 3, timeMs-1000, timeMs+1000, func(data []byte) {
		results = append(results, data)
	})
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	din := protocol.NewDataInputX(results[0])
	p, err := pack.ReadPack(din)
	if err != nil {
		t.Fatalf("ReadPack failed: %v", err)
	}
	sp := p.(*pack.SummaryPack)
	if sp.SType != 3 {
		t.Fatalf("expected SType 3, got %d", sp.SType)
	}
}

func TestSummaryRD_NonExistentDate(t *testing.T) {
	baseDir := t.TempDir()

	rd := NewSummaryRD(baseDir)
	defer rd.Close()

	var results [][]byte
	err := rd.ReadRange("20990101", 1, 0, 999999999999, func(data []byte) {
		results = append(results, data)
	})
	if err != nil {
		t.Fatalf("expected no error for non-existent date, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for non-existent date, got %d", len(results))
	}

	// Also verify the directory does not exist
	dir := filepath.Join(baseDir, "20990101", "summary")
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("expected directory to not exist")
	}
}

func TestSummaryData_MultipleTypes(t *testing.T) {
	baseDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr := NewSummaryWR(baseDir)
	wr.Start(ctx)

	timeMs := time.Now().UnixMilli()
	date := time.UnixMilli(timeMs).Format("20060102")

	// Write different summary types
	types := []byte{1, 2, 3, 4, 5, 6, 7}
	for _, stype := range types {
		summaryBytes := serializeSummary(t, stype, "test", timeMs)
		wr.Add(&SummaryEntry{
			TimeMs: timeMs,
			SType:  stype,
			Data:   summaryBytes,
		})
	}

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)
	wr.Close()

	// Read each type back
	rd := NewSummaryRD(baseDir)
	defer rd.Close()

	for _, stype := range types {
		var results [][]byte
		err := rd.ReadRange(date, stype, timeMs-1000, timeMs+1000, func(data []byte) {
			results = append(results, data)
		})
		if err != nil {
			t.Fatalf("ReadRange for type %d failed: %v", stype, err)
		}

		if len(results) != 1 {
			t.Fatalf("expected 1 result for type %d, got %d", stype, len(results))
		}

		din := protocol.NewDataInputX(results[0])
		p, err := pack.ReadPack(din)
		if err != nil {
			t.Fatalf("ReadPack for type %d failed: %v", stype, err)
		}
		sp := p.(*pack.SummaryPack)
		if sp.SType != stype {
			t.Fatalf("expected SType %d, got %d", stype, sp.SType)
		}
	}
}
