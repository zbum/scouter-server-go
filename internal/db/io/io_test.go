package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "scouter-io-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// --- MemHashBlock tests ---

func TestMemHashBlockPutGet(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	m, err := NewMemHashBlock(path, 1024*5) // small: 1024 buckets
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.Put(42, 12345)
	v := m.Get(42)
	if v != 12345 {
		t.Errorf("expected 12345, got %d", v)
	}

	if m.GetCount() != 1 {
		t.Errorf("expected count 1, got %d", m.GetCount())
	}

	// Overwrite same bucket
	m.Put(42, 99999)
	v = m.Get(42)
	if v != 99999 {
		t.Errorf("expected 99999, got %d", v)
	}
	// Count should still be 1 (overwrite, not new)
	if m.GetCount() != 1 {
		t.Errorf("expected count 1, got %d", m.GetCount())
	}
}

func TestMemHashBlockPersistence(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	m, err := NewMemHashBlock(path, 1024*5)
	if err != nil {
		t.Fatal(err)
	}
	m.Put(100, 777)
	m.Flush()
	m.Close()

	// Reopen
	m2, err := NewMemHashBlock(path, 1024*5)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	v := m2.Get(100)
	if v != 777 {
		t.Errorf("expected 777 after reopen, got %d", v)
	}
	if m2.GetCount() != 1 {
		t.Errorf("expected count 1, got %d", m2.GetCount())
	}
}

func TestMemHashBlockEmptyGet(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	m, err := NewMemHashBlock(path, 1024*5)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	v := m.Get(999)
	if v != 0 {
		t.Errorf("expected 0 for empty bucket, got %d", v)
	}
}

// --- MemTimeBlock tests ---

func TestMemTimeBlockPutGet(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	m, err := NewMemTimeBlock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	timeMs := int64(1705312245123) // some timestamp
	m.Put(timeMs, 54321)
	v := m.Get(timeMs)
	if v != 54321 {
		t.Errorf("expected 54321, got %d", v)
	}
}

func TestMemTimeBlockPersistence(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	timeMs := int64(1705312245123)
	m, err := NewMemTimeBlock(path)
	if err != nil {
		t.Fatal(err)
	}
	m.Put(timeMs, 11111)
	m.Flush()
	m.Close()

	m2, err := NewMemTimeBlock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	v := m2.Get(timeMs)
	if v != 11111 {
		t.Errorf("expected 11111 after reopen, got %d", v)
	}
}

// --- RealKeyFile tests ---

func TestRealKeyFileAppendAndRead(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	indexKey := []byte("mykey")
	dataPos := protocol.ToBytes5(12345)

	pos, err := kf.Append(0, indexKey, dataPos)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 2 { // right after 2-byte header
		t.Errorf("expected pos 2, got %d", pos)
	}

	// Read back
	r, err := kf.GetRecord(pos)
	if err != nil {
		t.Fatal(err)
	}
	if r.Deleted {
		t.Error("expected not deleted")
	}
	if r.PrevPos != 0 {
		t.Errorf("expected prevPos 0, got %d", r.PrevPos)
	}
	if string(r.TimeKey) != "mykey" {
		t.Errorf("expected 'mykey', got %q", string(r.TimeKey))
	}
	gotDataPos := protocol.ToLong5(r.DataPos, 0)
	if gotDataPos != 12345 {
		t.Errorf("expected dataPos 12345, got %d", gotDataPos)
	}
}

func TestRealKeyFileChain(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	// Append first record
	pos1, err := kf.Append(0, []byte("key1"), protocol.ToBytes5(100))
	if err != nil {
		t.Fatal(err)
	}

	// Append second record pointing back to first
	pos2, err := kf.Append(pos1, []byte("key2"), protocol.ToBytes5(200))
	if err != nil {
		t.Fatal(err)
	}

	// Verify chain
	prevPos, err := kf.GetPrevPos(pos2)
	if err != nil {
		t.Fatal(err)
	}
	if prevPos != pos1 {
		t.Errorf("expected prevPos %d, got %d", pos1, prevPos)
	}

	prevPos, err = kf.GetPrevPos(pos1)
	if err != nil {
		t.Fatal(err)
	}
	if prevPos != 0 {
		t.Errorf("expected prevPos 0, got %d", prevPos)
	}
}

func TestRealKeyFileDelete(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	pos, err := kf.Append(0, []byte("key"), protocol.ToBytes5(100))
	if err != nil {
		t.Fatal(err)
	}

	del, err := kf.IsDeleted(pos)
	if err != nil {
		t.Fatal(err)
	}
	if del {
		t.Error("expected not deleted")
	}

	kf.SetDelete(pos, true)
	del, err = kf.IsDeleted(pos)
	if err != nil {
		t.Fatal(err)
	}
	if !del {
		t.Error("expected deleted")
	}
}

// --- RealDataFile tests ---

func TestRealDataFileWriteAndOffset(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "data.dat")

	df, err := NewRealDataFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	pos1, err := df.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if pos1 != 0 {
		t.Errorf("expected pos 0, got %d", pos1)
	}

	pos2, err := df.Write([]byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if pos2 != 5 {
		t.Errorf("expected pos 5, got %d", pos2)
	}

	if df.GetOffset() != 10 {
		t.Errorf("expected offset 10, got %d", df.GetOffset())
	}
}

func TestRealDataFileWriteInt(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "data.dat")

	df, err := NewRealDataFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	pos, err := df.WriteInt(42)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Errorf("expected pos 0, got %d", pos)
	}
	if df.GetOffset() != 4 {
		t.Errorf("expected offset 4, got %d", df.GetOffset())
	}
}

// --- IndexKeyFile tests ---

func TestIndexKeyFilePutGet(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "idx")

	idx, err := NewIndexKeyFile(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	key := []byte("mykey")
	dataOff := protocol.ToBytes5(9999)

	if err := idx.Put(key, dataOff); err != nil {
		t.Fatal(err)
	}

	got, err := idx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if protocol.ToLong5(got, 0) != 9999 {
		t.Errorf("expected 9999, got %d", protocol.ToLong5(got, 0))
	}
}

func TestIndexKeyFileMultipleKeys(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "idx")

	idx, err := NewIndexKeyFile(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	for i := 0; i < 100; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i * 1000))
		if err := idx.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		key := protocol.ToBytesInt(int32(i))
		got, err := idx.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("get %d: nil result", i)
		}
		v := protocol.ToLong5(got, 0)
		if v != int64(i*1000) {
			t.Errorf("get %d: expected %d, got %d", i, i*1000, v)
		}
	}
}

func TestIndexKeyFileHasKey(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "idx")

	idx, err := NewIndexKeyFile(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	key := []byte("testkey")
	idx.Put(key, protocol.ToBytes5(100))

	has, err := idx.HasKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("expected HasKey=true")
	}

	has, err = idx.HasKey([]byte("nokey"))
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("expected HasKey=false for missing key")
	}
}

func TestIndexKeyFileDelete(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "idx")

	idx, err := NewIndexKeyFile(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	key := []byte("delkey")
	idx.Put(key, protocol.ToBytes5(100))

	n, err := idx.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 deleted, got %d", n)
	}

	got, err := idx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestIndexKeyFileRead(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "idx")

	idx, err := NewIndexKeyFile(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	idx.Put([]byte("k1"), protocol.ToBytes5(1))
	idx.Put([]byte("k2"), protocol.ToBytes5(2))
	idx.Put([]byte("k3"), protocol.ToBytes5(3))

	count := 0
	idx.Read(func(key []byte, data []byte) {
		count++
	})
	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestIndexKeyFileGetStat(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "idx")

	idx, err := NewIndexKeyFile(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	idx.Put([]byte("k1"), protocol.ToBytes5(1))
	idx.Put([]byte("k2"), protocol.ToBytes5(2))
	idx.Delete([]byte("k1"))

	stat := idx.GetStat()
	if stat["count"].(int) != 1 {
		t.Errorf("expected count 1, got %v", stat["count"])
	}
	if stat["deleted"].(int) != 1 {
		t.Errorf("expected deleted 1, got %v", stat["deleted"])
	}
}

// --- IndexTimeFile tests ---

func TestIndexTimeFilePutAndRead(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "tidx")

	idx, err := NewIndexTimeFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000) // a reasonable timestamp
	dp1 := protocol.ToBytes5(100)
	dp2 := protocol.ToBytes5(200)

	_, err = idx.Put(baseTime, dp1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx.Put(baseTime+100, dp2) // same 500ms bucket
	if err != nil {
		t.Fatal(err)
	}

	var results []TimeToData
	idx.Read(baseTime, baseTime+500, func(time int64, dataPos []byte) {
		results = append(results, TimeToData{Time: time, DataPos: dataPos})
	})
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestIndexTimeFileReadFromEnd(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "tidx")

	idx, err := NewIndexTimeFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	idx.Put(baseTime, protocol.ToBytes5(100))
	idx.Put(baseTime+100, protocol.ToBytes5(200))

	var results []int64
	idx.ReadFromEnd(baseTime, baseTime+500, func(time int64, dataPos []byte) {
		results = append(results, time)
	})
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
	// Results should be in reverse order
	if len(results) == 2 && results[0] < results[1] {
		t.Error("expected reverse chronological order")
	}
}

func TestIndexTimeFileDelete(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "tidx")

	idx, err := NewIndexTimeFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	timeMs := int64(1705312245000)
	idx.Put(timeMs, protocol.ToBytes5(100))

	n, err := idx.Delete(timeMs)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 deleted, got %d", n)
	}

	// Should now read 0 items for that time
	var count int
	idx.Read(timeMs, timeMs+500, func(time int64, dataPos []byte) {
		count++
	})
	if count != 0 {
		t.Errorf("expected 0 after delete, got %d", count)
	}
}

func TestIndexTimeFileMultipleBuckets(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "tidx")

	idx, err := NewIndexTimeFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	// Put entries in different 500ms buckets
	for i := 0; i < 5; i++ {
		timeMs := baseTime + int64(i)*500
		idx.Put(timeMs, protocol.ToBytes5(int64(i*100)))
	}

	var count int
	idx.Read(baseTime, baseTime+2500, func(time int64, dataPos []byte) {
		count++
	})
	if count != 5 {
		t.Errorf("expected 5, got %d", count)
	}
}

func TestIndexTimeFileGetDirect(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "tidx")

	idx, err := NewIndexTimeFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	timeMs := int64(1705312245000)
	pos, err := idx.Put(timeMs, protocol.ToBytes5(42))
	if err != nil {
		t.Fatal(err)
	}

	td, err := idx.GetDirect(pos)
	if err != nil {
		t.Fatal(err)
	}
	if td == nil {
		t.Fatal("expected non-nil")
	}
	if td.Time != timeMs {
		t.Errorf("expected time %d, got %d", timeMs, td.Time)
	}
}
