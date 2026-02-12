package xlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

func setupTestDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "xlog_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func cleanupTestDir(dir string) {
	os.RemoveAll(dir)
}

// TestXLogIndex tests triple indexing: time, txid, gxid.
func TestXLogIndex(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	indexDir := filepath.Join(dir, "20260207", "xlog")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		t.Fatalf("Failed to create index dir: %v", err)
	}

	index, err := NewXLogIndex(indexDir)
	if err != nil {
		t.Fatalf("Failed to create XLogIndex: %v", err)
	}
	defer index.Close()

	// Test time index
	timeMs := time.Now().UnixMilli()
	dataPos := int64(1234)
	if err := index.SetByTime(timeMs, dataPos); err != nil {
		t.Errorf("SetByTime failed: %v", err)
	}

	// Test txid index
	txid := int64(999888777)
	if err := index.SetByTxid(txid, dataPos); err != nil {
		t.Errorf("SetByTxid failed: %v", err)
	}

	retrievedPos, err := index.GetByTxid(txid)
	if err != nil {
		t.Errorf("GetByTxid failed: %v", err)
	}
	if retrievedPos != dataPos {
		t.Errorf("Expected dataPos %d, got %d", dataPos, retrievedPos)
	}

	// Test non-existent txid
	missingPos, err := index.GetByTxid(int64(111))
	if err != nil {
		t.Errorf("GetByTxid for missing key failed: %v", err)
	}
	if missingPos != -1 {
		t.Errorf("Expected -1 for missing txid, got %d", missingPos)
	}

	// Test gxid index (multi-value)
	gxid := int64(555444333)
	if err := index.SetByGxid(gxid, dataPos); err != nil {
		t.Errorf("SetByGxid failed: %v", err)
	}
	if err := index.SetByGxid(gxid, dataPos+100); err != nil {
		t.Errorf("SetByGxid (second) failed: %v", err)
	}

	offsets, err := index.GetByGxid(gxid)
	if err != nil {
		t.Errorf("GetByGxid failed: %v", err)
	}
	if len(offsets) != 2 {
		t.Errorf("Expected 2 offsets, got %d", len(offsets))
	}

	// Test gxid == 0 (should skip)
	if err := index.SetByGxid(0, dataPos); err != nil {
		t.Errorf("SetByGxid with gxid=0 should not error: %v", err)
	}
}

// TestXLogDataWriteRead tests data file write/read round-trip.
func TestXLogDataWriteRead(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	dataDir := filepath.Join(dir, "20260207", "xlog")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	xdata, err := NewXLogData(dataDir)
	if err != nil {
		t.Fatalf("Failed to create XLogData: %v", err)
	}
	defer xdata.Close()

	// Write test data
	testData := []byte("This is test XLog data with some content")
	offset, err := xdata.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0 for first write, got %d", offset)
	}

	// Flush to ensure data is on disk
	if err := xdata.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read back
	readData, err := xdata.Read(offset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, readData)
	}

	// Write second entry
	testData2 := []byte("Second entry")
	offset2, err := xdata.Write(testData2)
	if err != nil {
		t.Fatalf("Second write failed: %v", err)
	}
	if offset2 <= offset {
		t.Errorf("Expected offset2 > offset, got offset2=%d, offset=%d", offset2, offset)
	}

	if err := xdata.Flush(); err != nil {
		t.Fatalf("Second flush failed: %v", err)
	}

	readData2, err := xdata.Read(offset2)
	if err != nil {
		t.Fatalf("Second read failed: %v", err)
	}
	if string(readData2) != string(testData2) {
		t.Errorf("Second data mismatch: expected %q, got %q", testData2, readData2)
	}
}

// TestXLogWRAsync tests async writer with XLogRD reader.
func TestXLogWRAsync(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	// Create writer
	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)
	defer writer.Close()

	// Create test entries
	now := time.Now().UnixMilli()
	entries := []*XLogEntry{
		{
			Time:    now,
			Txid:    1001,
			Gxid:    5001,
			Elapsed: 100,
			Data:    []byte("xlog-1"),
		},
		{
			Time:    now + 1000,
			Txid:    1002,
			Gxid:    5001, // Same gxid for distributed tracing
			Elapsed: 200,
			Data:    []byte("xlog-2"),
		},
		{
			Time:    now + 2000,
			Txid:    1003,
			Gxid:    0, // No gxid
			Elapsed: 150,
			Data:    []byte("xlog-3"),
		},
	}

	// Add entries
	for _, e := range entries {
		writer.Add(e)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	writer.Close() // Flush all

	// Now read back
	reader := NewXLogRD(dir)
	defer reader.Close()

	date := time.UnixMilli(now).Format("20060102")

	// Test read by time range
	var readCount int
	err := reader.ReadByTime(date, now-1000, now+3000, func(data []byte) bool {
		readCount++
		t.Logf("Read data: %s", string(data))
		return true
	})
	if err != nil {
		t.Fatalf("ReadByTime failed: %v", err)
	}
	if readCount != 3 {
		t.Errorf("Expected 3 entries by time, got %d", readCount)
	}

	// Test read by txid
	data, err := reader.GetByTxid(date, 1002)
	if err != nil {
		t.Fatalf("GetByTxid failed: %v", err)
	}
	if data == nil {
		t.Fatal("Expected data for txid 1002, got nil")
	}
	if string(data) != "xlog-2" {
		t.Errorf("Expected 'xlog-2', got %q", string(data))
	}

	// Test read by gxid (should get 2 entries)
	var gxidCount int
	err = reader.ReadByGxid(date, 5001, func(data []byte) {
		gxidCount++
		t.Logf("Gxid data: %s", string(data))
	})
	if err != nil {
		t.Fatalf("ReadByGxid failed: %v", err)
	}
	if gxidCount != 2 {
		t.Errorf("Expected 2 entries for gxid 5001, got %d", gxidCount)
	}
}

// TestXLogMultipleDays tests entries spanning multiple days.
func TestXLogMultipleDays(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)
	defer writer.Close()

	// Create entries for different days
	day1 := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC).UnixMilli()
	day2 := time.Date(2026, 2, 8, 12, 0, 0, 0, time.UTC).UnixMilli()

	writer.Add(&XLogEntry{
		Time:    day1,
		Txid:    2001,
		Gxid:    0,
		Elapsed: 50,
		Data:    []byte("day1-entry"),
	})

	writer.Add(&XLogEntry{
		Time:    day2,
		Txid:    2002,
		Gxid:    0,
		Elapsed: 60,
		Data:    []byte("day2-entry"),
	})

	time.Sleep(100 * time.Millisecond)
	writer.Close()

	// Read from both days
	reader := NewXLogRD(dir)
	defer reader.Close()

	date1 := time.UnixMilli(day1).Format("20060102")
	data1, err := reader.GetByTxid(date1, 2001)
	if err != nil {
		t.Fatalf("GetByTxid day1 failed: %v", err)
	}
	if data1 == nil || string(data1) != "day1-entry" {
		t.Errorf("Expected 'day1-entry', got %q", string(data1))
	}

	date2 := time.UnixMilli(day2).Format("20060102")
	data2, err := reader.GetByTxid(date2, 2002)
	if err != nil {
		t.Fatalf("GetByTxid day2 failed: %v", err)
	}
	if data2 == nil || string(data2) != "day2-entry" {
		t.Errorf("Expected 'day2-entry', got %q", string(data2))
	}
}

// TestXLogProtocolConversion tests byte conversion utilities.
func TestXLogProtocolConversion(t *testing.T) {
	// Test ToBytes5 and ToLong5
	testVal := int64(123456789)
	bytes5 := protocol.ToBytes5(testVal)
	if len(bytes5) != 5 {
		t.Errorf("Expected 5 bytes, got %d", len(bytes5))
	}
	recovered := protocol.ToLong5(bytes5, 0)
	if recovered != testVal {
		t.Errorf("ToBytes5/ToLong5 round-trip failed: expected %d, got %d", testVal, recovered)
	}

	// Test ToBytesLong and ToLong
	testLong := int64(9876543210)
	bytesLong := protocol.ToBytesLong(testLong)
	if len(bytesLong) != 8 {
		t.Errorf("Expected 8 bytes, got %d", len(bytesLong))
	}
	recoveredLong := protocol.ToLong(bytesLong, 0)
	if recoveredLong != testLong {
		t.Errorf("ToBytesLong/ToLong round-trip failed: expected %d, got %d", testLong, recoveredLong)
	}
}

// TestXLogWRBatchProcessing tests that entries queued together are processed as a batch.
func TestXLogWRBatchProcessing(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	n := 500

	// Enqueue many entries at once so they accumulate in the queue
	for i := 0; i < n; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i),
			Txid:    int64(3000 + i),
			Gxid:    0,
			Elapsed: int32(i),
			Data:    []byte("batch-entry"),
		})
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	writer.Close()

	// Verify all entries are readable
	reader := NewXLogRD(dir)
	defer reader.Close()
	date := time.UnixMilli(now).Format("20060102")

	for i := 0; i < n; i++ {
		data, err := reader.GetByTxid(date, int64(3000+i))
		if err != nil {
			t.Fatalf("GetByTxid %d failed: %v", 3000+i, err)
		}
		if data == nil {
			t.Fatalf("Expected data for txid %d, got nil", 3000+i)
		}
		if string(data) != "batch-entry" {
			t.Errorf("txid %d: expected 'batch-entry', got %q", 3000+i, string(data))
		}
	}

	// Verify time range read returns all entries
	var count int
	err := reader.ReadByTime(date, now-1, now+int64(n)+1, func(data []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ReadByTime failed: %v", err)
	}
	if count != n {
		t.Errorf("Expected %d entries by time range, got %d", n, count)
	}
}

// TestXLogWRBatchWithGxid tests batch processing with gxid indexing.
func TestXLogWRBatchWithGxid(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	gxid := int64(77777)

	// 10 entries sharing the same gxid (distributed transaction)
	for i := 0; i < 10; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i),
			Txid:    int64(4000 + i),
			Gxid:    gxid,
			Elapsed: 50,
			Data:    protocol.ToBytesLong(int64(i)),
		})
	}

	time.Sleep(200 * time.Millisecond)
	writer.Close()

	reader := NewXLogRD(dir)
	defer reader.Close()
	date := time.UnixMilli(now).Format("20060102")

	var gxidCount int
	err := reader.ReadByGxid(date, gxid, func(data []byte) {
		gxidCount++
	})
	if err != nil {
		t.Fatalf("ReadByGxid failed: %v", err)
	}
	if gxidCount != 10 {
		t.Errorf("Expected 10 entries for gxid, got %d", gxidCount)
	}
}

// TestXLogReaderNonExistentDate tests reading from a date that has no data.
func TestXLogReaderNonExistentDate(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(dir)

	reader := NewXLogRD(dir)
	defer reader.Close()

	// Try to read from a non-existent date
	err := reader.ReadByTime("20991231", 0, 1000000, func(data []byte) bool {
		t.Error("Should not receive any data for non-existent date")
		return true
	})
	if err != nil {
		t.Errorf("ReadByTime for non-existent date should not error: %v", err)
	}

	data, err := reader.GetByTxid("20991231", 12345)
	if err != nil {
		t.Errorf("GetByTxid for non-existent date should not error: %v", err)
	}
	if data != nil {
		t.Error("Expected nil data for non-existent date")
	}
}
