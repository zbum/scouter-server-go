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

	if m.Count() != 1 {
		t.Errorf("expected count 1, got %d", m.Count())
	}

	// Overwrite same bucket
	m.Put(42, 99999)
	v = m.Get(42)
	if v != 99999 {
		t.Errorf("expected 99999, got %d", v)
	}
	// Count should still be 1 (overwrite, not new)
	if m.Count() != 1 {
		t.Errorf("expected count 1, got %d", m.Count())
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
	if m2.Count() != 1 {
		t.Errorf("expected count 1, got %d", m2.Count())
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
	dataPos := protocol.BigEndian.Bytes5(12345)

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
	gotDataPos := protocol.BigEndian.Int5(r.DataPos)
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
	pos1, err := kf.Append(0, []byte("key1"), protocol.BigEndian.Bytes5(100))
	if err != nil {
		t.Fatal(err)
	}

	// Append second record pointing back to first
	pos2, err := kf.Append(pos1, []byte("key2"), protocol.BigEndian.Bytes5(200))
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

	pos, err := kf.Append(0, []byte("key"), protocol.BigEndian.Bytes5(100))
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

	if df.Offset() != 10 {
		t.Errorf("expected offset 10, got %d", df.Offset())
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
	if df.Offset() != 4 {
		t.Errorf("expected offset 4, got %d", df.Offset())
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
	dataOff := protocol.BigEndian.Bytes5(9999)

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
	if protocol.BigEndian.Int5(got) != 9999 {
		t.Errorf("expected 9999, got %d", protocol.BigEndian.Int5(got))
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
		key := protocol.BigEndian.Bytes4(int32(i))
		val := protocol.BigEndian.Bytes5(int64(i * 1000))
		if err := idx.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		key := protocol.BigEndian.Bytes4(int32(i))
		got, err := idx.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("get %d: nil result", i)
		}
		v := protocol.BigEndian.Int5(got)
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
	idx.Put(key, protocol.BigEndian.Bytes5(100))

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
	idx.Put(key, protocol.BigEndian.Bytes5(100))

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

	idx.Put([]byte("k1"), protocol.BigEndian.Bytes5(1))
	idx.Put([]byte("k2"), protocol.BigEndian.Bytes5(2))
	idx.Put([]byte("k3"), protocol.BigEndian.Bytes5(3))

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

	idx.Put([]byte("k1"), protocol.BigEndian.Bytes5(1))
	idx.Put([]byte("k2"), protocol.BigEndian.Bytes5(2))
	idx.Delete([]byte("k1"))

	stat := idx.Stat()
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
	dp1 := protocol.BigEndian.Bytes5(100)
	dp2 := protocol.BigEndian.Bytes5(200)

	_, err = idx.Put(baseTime, dp1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx.Put(baseTime+100, dp2) // same 500ms bucket
	if err != nil {
		t.Fatal(err)
	}

	var results []TimeToData
	idx.Read(baseTime, baseTime+500, func(time int64, dataPos []byte) bool {
		results = append(results, TimeToData{Time: time, DataPos: dataPos})
		return true
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
	idx.Put(baseTime, protocol.BigEndian.Bytes5(100))
	idx.Put(baseTime+100, protocol.BigEndian.Bytes5(200))

	var results []int64
	idx.ReadFromEnd(baseTime, baseTime+500, func(time int64, dataPos []byte) bool {
		results = append(results, time)
		return true
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
	idx.Put(timeMs, protocol.BigEndian.Bytes5(100))

	n, err := idx.Delete(timeMs)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 deleted, got %d", n)
	}

	// Should now read 0 items for that time
	var count int
	idx.Read(timeMs, timeMs+500, func(time int64, dataPos []byte) bool {
		count++
		return true
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
		idx.Put(timeMs, protocol.BigEndian.Bytes5(int64(i*100)))
	}

	var count int
	idx.Read(baseTime, baseTime+2500, func(time int64, dataPos []byte) bool {
		count++
		return true
	})
	if count != 5 {
		t.Errorf("expected 5, got %d", count)
	}
}

// --- RealKeyFile buffered append tests ---

func TestRealKeyFileBufferedAppendReadBack(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	// Append multiple records (all stay in buffer since < 16KB)
	positions := make([]int64, 10)
	for i := 0; i < 10; i++ {
		key := protocol.BigEndian.Bytes4(int32(i))
		val := protocol.BigEndian.Bytes5(int64(i * 100))
		pos, err := kf.Append(0, key, val)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		positions[i] = pos
	}

	// IsDirty should be true (unflushed data in buffer)
	if !kf.IsDirty() {
		t.Error("expected IsDirty=true after append")
	}

	// Read back all records (should trigger flush and return correct data)
	for i := 0; i < 10; i++ {
		r, err := kf.GetRecord(positions[i])
		if err != nil {
			t.Fatalf("GetRecord %d at pos %d: %v", i, positions[i], err)
		}
		gotKey := protocol.BigEndian.Int32(r.TimeKey)
		if gotKey != int32(i) {
			t.Errorf("record %d: expected key %d, got %d", i, i, gotKey)
		}
		gotVal := protocol.BigEndian.Int5(r.DataPos)
		if gotVal != int64(i*100) {
			t.Errorf("record %d: expected dataPos %d, got %d", i, i*100, gotVal)
		}
	}
}

func TestRealKeyFileBufferedChain(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	// Build a chain of 5 records: each points back to the previous
	var prevPos int64
	positions := make([]int64, 5)
	for i := 0; i < 5; i++ {
		key := protocol.BigEndian.Bytes4(int32(i))
		val := protocol.BigEndian.Bytes5(int64(i))
		pos, err := kf.Append(prevPos, key, val)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		positions[i] = pos
		prevPos = pos
	}

	// Walk the chain backwards from the last record
	pos := positions[4]
	for i := 4; i >= 0; i-- {
		r, err := kf.GetRecord(pos)
		if err != nil {
			t.Fatalf("GetRecord at step %d: %v", i, err)
		}
		gotKey := protocol.BigEndian.Int32(r.TimeKey)
		if gotKey != int32(i) {
			t.Errorf("chain step %d: expected key %d, got %d", i, i, gotKey)
		}
		pos = r.PrevPos
	}
	if pos != 0 {
		t.Errorf("expected chain to end at 0, got %d", pos)
	}
}

func TestRealKeyFileAutoFlushOnThreshold(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	// Each record is roughly ~20 bytes. Need ~820 records to exceed 16KB.
	// Append enough records to trigger auto-flush at least once.
	n := 1000
	positions := make([]int64, n)
	for i := 0; i < n; i++ {
		key := protocol.BigEndian.Bytes8(int64(i))
		val := protocol.BigEndian.Bytes5(int64(i * 10))
		pos, err := kf.Append(0, key, val)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		positions[i] = pos
	}

	// Verify file on disk has data (auto-flush should have written some)
	fi, err := os.Stat(path + ".kfile")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() <= kfileHeaderSize {
		t.Errorf("expected file to have flushed data, size=%d", fi.Size())
	}

	// All records should still be readable
	for i := 0; i < n; i++ {
		dp, err := kf.GetDataPos(positions[i])
		if err != nil {
			t.Fatalf("GetDataPos %d: %v", i, err)
		}
		got := protocol.BigEndian.Int5(dp)
		if got != int64(i*10) {
			t.Errorf("record %d: expected %d, got %d", i, i*10, got)
		}
	}
}

func TestRealKeyFileLengthIncludesBuffer(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	lenBefore := kf.Length()
	if lenBefore != kfileHeaderSize {
		t.Errorf("expected initial length %d, got %d", kfileHeaderSize, lenBefore)
	}

	kf.Append(0, []byte("key"), protocol.BigEndian.Bytes5(100))

	lenAfter := kf.Length()
	if lenAfter <= lenBefore {
		t.Errorf("expected length to increase after append, before=%d after=%d", lenBefore, lenAfter)
	}
}

func TestRealKeyFilePersistenceAfterFlush(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}

	pos, err := kf.Append(0, []byte("persist"), protocol.BigEndian.Bytes5(777))
	if err != nil {
		t.Fatal(err)
	}

	// Flush and close
	kf.Close()

	// Reopen and verify
	kf2, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf2.Close()

	r, err := kf2.GetRecord(pos)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.TimeKey) != "persist" {
		t.Errorf("expected key 'persist', got %q", string(r.TimeKey))
	}
	if protocol.BigEndian.Int5(r.DataPos) != 777 {
		t.Errorf("expected dataPos 777, got %d", protocol.BigEndian.Int5(r.DataPos))
	}
}

func TestRealKeyFileDeleteWithBufferedData(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	pos1, _ := kf.Append(0, []byte("a"), protocol.BigEndian.Bytes5(1))
	pos2, _ := kf.Append(pos1, []byte("b"), protocol.BigEndian.Bytes5(2))

	// Delete first record (triggers flush before positional write)
	if err := kf.SetDelete(pos1, true); err != nil {
		t.Fatal(err)
	}

	// Verify first is deleted, second is not
	del, _ := kf.IsDeleted(pos1)
	if !del {
		t.Error("expected pos1 deleted")
	}
	del, _ = kf.IsDeleted(pos2)
	if del {
		t.Error("expected pos2 not deleted")
	}
}

func TestRealKeyFileFlushIdempotent(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	kf.Append(0, []byte("x"), protocol.BigEndian.Bytes5(1))

	// Multiple flushes should be safe
	kf.Flush()
	kf.Flush()
	kf.Flush()

	if kf.IsDirty() {
		t.Error("expected not dirty after flush")
	}
}

func TestRealKeyFileInterleavedAppendAndRead(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test")

	kf, err := NewRealKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer kf.Close()

	// Interleave appends and reads to verify flush-before-read works correctly
	for i := 0; i < 50; i++ {
		key := protocol.BigEndian.Bytes4(int32(i))
		val := protocol.BigEndian.Bytes5(int64(i))
		pos, err := kf.Append(0, key, val)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}

		// Immediately read back (triggers flush)
		r, err := kf.GetRecord(pos)
		if err != nil {
			t.Fatalf("GetRecord %d: %v", i, err)
		}
		gotKey := protocol.BigEndian.Int32(r.TimeKey)
		if gotKey != int32(i) {
			t.Errorf("step %d: expected key %d, got %d", i, i, gotKey)
		}
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
	pos, err := idx.Put(timeMs, protocol.BigEndian.Bytes5(42))
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
