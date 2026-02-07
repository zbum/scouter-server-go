package alert

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// serializeAlert creates a serialized AlertPack for testing.
func serializeAlert(t *testing.T, title string, timeMs int64) []byte {
	t.Helper()
	ap := &pack.AlertPack{
		Time:    timeMs,
		Level:   2,
		ObjType: "java",
		ObjHash: 42,
		Title:   title,
		Message: "test message for " + title,
	}
	o := protocol.NewDataOutputX()
	pack.WritePack(o, ap)
	return o.ToByteArray()
}

func TestAlertData_WriteRead(t *testing.T) {
	dir := t.TempDir()

	ad, err := NewAlertData(dir)
	if err != nil {
		t.Fatalf("NewAlertData failed: %v", err)
	}
	defer ad.Close()

	timeMs := time.Now().UnixMilli()
	alertBytes := serializeAlert(t, "CPU High", timeMs)

	if err := ad.Write(timeMs, alertBytes); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := ad.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	var results [][]byte
	err = ad.ReadRange(timeMs-1000, timeMs+1000, func(readTime int64, data []byte) {
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
	ap := p.(*pack.AlertPack)
	if ap.Title != "CPU High" {
		t.Fatalf("expected title 'CPU High', got %q", ap.Title)
	}
	if ap.ObjHash != 42 {
		t.Fatalf("expected objHash 42, got %d", ap.ObjHash)
	}
}

func TestAlertData_MultipleAlerts(t *testing.T) {
	dir := t.TempDir()

	ad, err := NewAlertData(dir)
	if err != nil {
		t.Fatalf("NewAlertData failed: %v", err)
	}
	defer ad.Close()

	baseTime := time.Now().UnixMilli()
	titles := []string{"Alert A", "Alert B", "Alert C"}

	for i, title := range titles {
		timeMs := baseTime + int64(i)*1000 // 1 second apart
		alertBytes := serializeAlert(t, title, timeMs)
		if err := ad.Write(timeMs, alertBytes); err != nil {
			t.Fatalf("Write[%d] failed: %v", i, err)
		}
	}
	if err := ad.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read all in range
	var results [][]byte
	err = ad.ReadRange(baseTime-1000, baseTime+5000, func(readTime int64, data []byte) {
		results = append(results, data)
	})
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify each title
	for i, data := range results {
		din := protocol.NewDataInputX(data)
		p, err := pack.ReadPack(din)
		if err != nil {
			t.Fatalf("ReadPack[%d] failed: %v", i, err)
		}
		ap := p.(*pack.AlertPack)
		if ap.Title != titles[i] {
			t.Fatalf("expected title %q, got %q", titles[i], ap.Title)
		}
	}
}

func TestAlertWR_Async(t *testing.T) {
	baseDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wr := NewAlertWR(baseDir)
	wr.Start(ctx)

	timeMs := time.Now().UnixMilli()
	alertBytes := serializeAlert(t, "Disk Full", timeMs)

	wr.Add(&AlertEntry{
		TimeMs: timeMs,
		Data:   alertBytes,
	})

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)
	wr.Close()

	// Now read it back using AlertRD
	rd := NewAlertRD(baseDir)
	defer rd.Close()

	date := time.UnixMilli(timeMs).Format("20060102")
	var results [][]byte
	err := rd.ReadRange(date, timeMs-1000, timeMs+1000, func(data []byte) {
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
	ap := p.(*pack.AlertPack)
	if ap.Title != "Disk Full" {
		t.Fatalf("expected title 'Disk Full', got %q", ap.Title)
	}
}

func TestAlertRD_NonExistentDate(t *testing.T) {
	baseDir := t.TempDir()

	rd := NewAlertRD(baseDir)
	defer rd.Close()

	var results [][]byte
	err := rd.ReadRange("20990101", 0, 999999999999, func(data []byte) {
		results = append(results, data)
	})
	if err != nil {
		t.Fatalf("expected no error for non-existent date, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for non-existent date, got %d", len(results))
	}

	// Also verify the directory does not exist
	dir := filepath.Join(baseDir, "20990101", "alert")
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("expected directory to not exist")
	}
}
