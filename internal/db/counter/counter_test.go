package counter

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

func TestRealtimeCounterData_WriteRead(t *testing.T) {
	dir := t.TempDir()

	data, err := NewRealtimeCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	counters := map[string]value.Value{
		"TPS":        value.NewDecimalValue(42),
		"ActiveUser": value.NewDecimalValue(100),
	}

	if err := data.Write(1, 3600, counters); err != nil {
		t.Fatal(err)
	}
	data.Flush()

	result, err := data.Read(1, 3600)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	tps, ok := result["TPS"]
	if !ok {
		t.Fatal("missing TPS")
	}
	if dv, ok := tps.(*value.DecimalValue); !ok || dv.Value != 42 {
		t.Fatalf("expected TPS=42, got %v", tps)
	}

	au, ok := result["ActiveUser"]
	if !ok {
		t.Fatal("missing ActiveUser")
	}
	if dv, ok := au.(*value.DecimalValue); !ok || dv.Value != 100 {
		t.Fatalf("expected ActiveUser=100, got %v", au)
	}
}

func TestRealtimeCounterData_ReadNonExistent(t *testing.T) {
	dir := t.TempDir()

	data, err := NewRealtimeCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	result, err := data.Read(999, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatal("expected nil for non-existent key")
	}
}

func TestRealtimeCounterData_ReadRange(t *testing.T) {
	dir := t.TempDir()

	data, err := NewRealtimeCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	for sec := int32(100); sec <= 105; sec++ {
		counters := map[string]value.Value{
			"TPS": value.NewDecimalValue(int64(sec)),
		}
		if err := data.Write(1, sec, counters); err != nil {
			t.Fatal(err)
		}
	}
	data.Flush()

	count := 0
	data.ReadRange(1, 100, 105, func(timeSec int32, counters map[string]value.Value) {
		count++
		tps := counters["TPS"].(*value.DecimalValue).Value
		if tps != int64(timeSec) {
			t.Fatalf("at sec %d, expected TPS=%d, got %d", timeSec, timeSec, tps)
		}
	})

	if count != 6 {
		t.Fatalf("expected 6 entries, got %d", count)
	}
}

func TestDailyCounterData_WriteRead(t *testing.T) {
	dir := t.TempDir()

	data, err := NewDailyCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	// Write value at bucket 0 (00:00-00:05)
	if err := data.Write(1, "TPS", 0, 42.5); err != nil {
		t.Fatal(err)
	}

	// Write value at bucket 144 (12:00-12:05)
	if err := data.Write(1, "TPS", 144, 100.0); err != nil {
		t.Fatal(err)
	}

	// Read back
	val, ok, err := data.Read(1, "TPS", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || val != 42.5 {
		t.Fatalf("expected 42.5, got %f (ok=%v)", val, ok)
	}

	val, ok, err = data.Read(1, "TPS", 144)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || val != 100.0 {
		t.Fatalf("expected 100.0, got %f", val)
	}

	// Read unwritten bucket
	_, ok, err = data.Read(1, "TPS", 50)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no value for unwritten bucket")
	}
}

func TestDailyCounterData_ReadAll(t *testing.T) {
	dir := t.TempDir()

	data, err := NewDailyCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	data.Write(1, "TPS", 0, 10.0)
	data.Write(1, "TPS", 287, 99.0)

	values, err := data.ReadAll(1, "TPS")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != BucketsPerDay {
		t.Fatalf("expected %d values, got %d", BucketsPerDay, len(values))
	}
	if values[0] != 10.0 {
		t.Fatalf("expected 10.0, got %f", values[0])
	}
	if values[287] != 99.0 {
		t.Fatalf("expected 99.0, got %f", values[287])
	}
	if !math.IsNaN(values[1]) {
		t.Fatalf("expected NaN for unwritten bucket, got %f", values[1])
	}
}

func TestDailyCounterData_ReadNonExistent(t *testing.T) {
	dir := t.TempDir()

	data, err := NewDailyCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	_, ok, err := data.Read(999, "NonExistent", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no value for non-existent counter")
	}
}

func TestHHMMToBucket(t *testing.T) {
	tests := []struct {
		hhmm   int
		bucket int
	}{
		{0, 0},
		{5, 1},      // 00:05
		{100, 12},   // 01:00
		{1200, 144}, // 12:00
		{2355, 287}, // 23:55
	}

	for _, tc := range tests {
		got := HHMMToBucket(tc.hhmm)
		if got != tc.bucket {
			t.Errorf("HHMMToBucket(%d) = %d, want %d", tc.hhmm, got, tc.bucket)
		}
	}
}

func TestBucketToHHMM(t *testing.T) {
	tests := []struct {
		bucket int
		hhmm   int
	}{
		{0, 0},
		{1, 5},
		{12, 100},
		{144, 1200},
		{287, 2355},
	}

	for _, tc := range tests {
		got := BucketToHHMM(tc.bucket)
		if got != tc.hhmm {
			t.Errorf("BucketToHHMM(%d) = %d, want %d", tc.bucket, got, tc.hhmm)
		}
	}
}

func TestCounterWR_AsyncRealtimeWrite(t *testing.T) {
	baseDir := t.TempDir()

	wr := NewCounterWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	wr.Start(ctx)

	now := time.Now()
	counters := map[string]value.Value{
		"TPS": value.NewDecimalValue(50),
	}
	wr.AddRealtimeFromPerfCounter(now.UnixMilli(), 1, counters)

	time.Sleep(200 * time.Millisecond)
	cancel()
	wr.Close()

	// Verify via reader
	rd := NewCounterRD(baseDir)
	defer rd.Close()

	date := now.Format("20060102")
	timeSec := int32(now.Hour()*3600 + now.Minute()*60 + now.Second())

	result, err := rd.ReadRealtime(date, 1, timeSec)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	tps := result["TPS"].(*value.DecimalValue).Value
	if tps != 50 {
		t.Fatalf("expected TPS=50, got %d", tps)
	}
}

func TestCounterWR_AsyncDailyWrite(t *testing.T) {
	baseDir := t.TempDir()

	wr := NewCounterWR(baseDir)
	ctx, cancel := context.WithCancel(context.Background())
	wr.Start(ctx)

	date := "20250101"
	wr.AddDaily(&DailyEntry{
		Date:        date,
		ObjHash:     1,
		CounterName: "TPS",
		Bucket:      144,
		Value:       75.0,
	})

	time.Sleep(200 * time.Millisecond)
	cancel()
	wr.Close()

	// Verify
	rd := NewCounterRD(baseDir)
	defer rd.Close()

	// Create the directory so reader can find it
	val, ok, err := rd.ReadDaily(date, 1, "TPS", 144)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || val != 75.0 {
		t.Fatalf("expected 75.0, got %f (ok=%v)", val, ok)
	}
}

func TestCounterRD_NonExistentDate(t *testing.T) {
	baseDir := t.TempDir()
	rd := NewCounterRD(baseDir)
	defer rd.Close()

	result, err := rd.ReadRealtime("99991231", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatal("expected nil for non-existent date")
	}

	_, ok, err := rd.ReadDaily("99991231", 1, "TPS", 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no value for non-existent date")
	}
}

func TestMultipleCountersPerObject(t *testing.T) {
	dir := t.TempDir()

	data, err := NewDailyCounterData(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()

	data.Write(1, "TPS", 0, 10.0)
	data.Write(1, "ResponseTime", 0, 250.0)
	data.Write(2, "TPS", 0, 20.0)

	val1, ok1, _ := data.Read(1, "TPS", 0)
	val2, ok2, _ := data.Read(1, "ResponseTime", 0)
	val3, ok3, _ := data.Read(2, "TPS", 0)

	if !ok1 || val1 != 10.0 {
		t.Fatalf("obj1 TPS: expected 10.0, got %f", val1)
	}
	if !ok2 || val2 != 250.0 {
		t.Fatalf("obj1 RT: expected 250.0, got %f", val2)
	}
	if !ok3 || val3 != 20.0 {
		t.Fatalf("obj2 TPS: expected 20.0, got %f", val3)
	}
}

func init() {
	// Ensure temp directories are cleaned up
	os.Setenv("TMPDIR", os.TempDir())
}
