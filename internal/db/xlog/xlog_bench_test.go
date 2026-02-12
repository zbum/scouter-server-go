package xlog

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func benchDir(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "xlog-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// ============================================================================
// XLogData Benchmarks
// ============================================================================

// makeTestData creates test data that won't conflict with compress format detection.
// Real XLogPack starts with a non-zero pack type byte; 0x00 is the compression flag.
func makeTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i + 1) // avoid 0x00 in first byte
	}
	return data
}

func BenchmarkXLogData_Write(b *testing.B) {
	for _, size := range []int{64, 256, 512} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			dir := benchDir(b)
			dataDir := filepath.Join(dir, "xlog")
			os.MkdirAll(dataDir, 0755)

			xd, err := NewXLogData(dataDir)
			if err != nil {
				b.Fatal(err)
			}
			defer xd.Close()

			data := makeTestData(size)

			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := xd.Write(data); err != nil {
					b.Fatal(err)
				}
				if i%500 == 499 {
					xd.Flush()
				}
			}
		})
	}
}

func BenchmarkXLogData_Read_Sequential(b *testing.B) {
	dir := benchDir(b)
	dataDir := filepath.Join(dir, "xlog")
	os.MkdirAll(dataDir, 0755)

	xd, err := NewXLogData(dataDir)
	if err != nil {
		b.Fatal(err)
	}
	defer xd.Close()

	// Pre-populate with 100K entries
	n := 100000
	offsets := make([]int64, n)
	data := makeTestData(256)
	for i := 0; i < n; i++ {
		off, err := xd.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		offsets[i] = off
	}
	xd.Flush()

	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := xd.Read(offsets[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXLogData_Read_Random(b *testing.B) {
	dir := benchDir(b)
	dataDir := filepath.Join(dir, "xlog")
	os.MkdirAll(dataDir, 0755)

	xd, err := NewXLogData(dataDir)
	if err != nil {
		b.Fatal(err)
	}
	defer xd.Close()

	n := 100000
	offsets := make([]int64, n)
	data := makeTestData(256)
	for i := 0; i < n; i++ {
		off, err := xd.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		offsets[i] = off
	}
	xd.Flush()

	// Shuffle for random access
	rng := rand.New(rand.NewSource(42))
	shuffled := make([]int64, n)
	copy(shuffled, offsets)
	rng.Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := xd.Read(shuffled[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXLogData_Read_Concurrent(b *testing.B) {
	dir := benchDir(b)
	dataDir := filepath.Join(dir, "xlog")
	os.MkdirAll(dataDir, 0755)

	xd, err := NewXLogData(dataDir)
	if err != nil {
		b.Fatal(err)
	}
	defer xd.Close()

	n := 100000
	offsets := make([]int64, n)
	data := makeTestData(256)
	for i := 0; i < n; i++ {
		off, err := xd.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		offsets[i] = off
	}
	xd.Flush()

	b.SetBytes(256)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			if _, err := xd.Read(offsets[idx%n]); err != nil {
				b.Fatal(err)
			}
			idx++
		}
	})
}

// ============================================================================
// XLogIndex Benchmarks
// ============================================================================

func BenchmarkXLogIndex_SetByTime(b *testing.B) {
	dir := benchDir(b)
	indexDir := filepath.Join(dir, "xlog")
	os.MkdirAll(indexDir, 0755)

	idx, err := NewXLogIndex(indexDir)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SetByTime(baseTime+int64(i), int64(i*100))
	}
}

func BenchmarkXLogIndex_SetByTxid(b *testing.B) {
	dir := benchDir(b)
	indexDir := filepath.Join(dir, "xlog")
	os.MkdirAll(indexDir, 0755)

	idx, err := NewXLogIndex(indexDir)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SetByTxid(int64(i+1000000), int64(i*100))
	}
}

func BenchmarkXLogIndex_GetByTxid(b *testing.B) {
	dir := benchDir(b)
	indexDir := filepath.Join(dir, "xlog")
	os.MkdirAll(indexDir, 0755)

	idx, err := NewXLogIndex(indexDir)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	n := 100000
	for i := 0; i < n; i++ {
		idx.SetByTxid(int64(i+1000000), int64(i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.GetByTxid(int64(i%n + 1000000))
	}
}

func BenchmarkXLogIndex_TripleIndex_Write(b *testing.B) {
	dir := benchDir(b)
	indexDir := filepath.Join(dir, "xlog")
	os.MkdirAll(indexDir, 0755)

	idx, err := NewXLogIndex(indexDir)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dataPos := int64(i * 100)
		idx.SetByTime(baseTime+int64(i), dataPos)
		idx.SetByTxid(int64(i+1000000), dataPos)
		if i%3 == 0 { // ~33% have gxid
			idx.SetByGxid(int64(i/3+5000000), dataPos)
		}
	}
}

// ============================================================================
// XLogWR End-to-End Benchmarks
// ============================================================================

func BenchmarkXLogWR_Add(b *testing.B) {
	dir := benchDir(b)
	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)
	defer writer.Close()

	now := time.Now().UnixMilli()
	data := make([]byte, 256) // typical XLogPack size
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i),
			Txid:    int64(i + 1000000),
			Gxid:    int64(i/3 + 5000000),
			Elapsed: int32(i % 5000),
			Data:    data,
		})
	}
}

func BenchmarkXLogWR_AddAndWait(b *testing.B) {
	dir := benchDir(b)
	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	data := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i),
			Txid:    int64(i + 1000000),
			Gxid:    0,
			Elapsed: 100,
			Data:    data,
		})
	}
	b.StopTimer()
	// Wait for queue drain
	writer.Close()
}

func BenchmarkXLogWR_ConcurrentAdd(b *testing.B) {
	dir := benchDir(b)
	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	data := make([]byte, 256)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			writer.Add(&XLogEntry{
				Time:    now + int64(idx),
				Txid:    int64(idx + 1000000),
				Gxid:    0,
				Elapsed: 100,
				Data:    data,
			})
			idx++
		}
	})
	b.StopTimer()
	writer.Close()
}

// ============================================================================
// XLog ReadByTime End-to-End Benchmarks (the "past data query" hot path)
// ============================================================================

// setupXLogData populates XLog storage with N entries over a time range,
// returning the writer (with in-memory containers) and a reader for past data.
func setupXLogData(b *testing.B, dir string, n int) (*XLogWR, *XLogRD, int64) {
	b.Helper()

	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	data := makeTestData(256)

	for i := 0; i < n; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i)*10, // 10ms apart
			Txid:    int64(i + 1000000),
			Gxid:    int64(i/5 + 5000000), // 5 per gxid group
			Elapsed: int32(i % 5000),
			Data:    data,
		})
	}

	// Wait for queue to drain
	time.Sleep(500 * time.Millisecond)
	cancel()
	writer.Close()

	// Reopen as writer+reader
	writer2 := NewXLogWR(dir)
	ctx2, cancel2 := context.WithCancel(context.Background())
	writer2.Start(ctx2)
	b.Cleanup(func() {
		cancel2()
		writer2.Close()
	})

	reader := NewXLogRD(dir)
	b.Cleanup(func() { reader.Close() })

	return writer2, reader, now
}

func BenchmarkXLogRD_ReadByTime_1K(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 10000)
	date := time.UnixMilli(now).Format("20060102")

	// 1K entries range
	stime := now
	etime := now + 10000 // 10ms * 1000 = 10s

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		reader.ReadByTime(date, stime, etime, func(data []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkXLogRD_ReadByTime_10K(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 50000)
	date := time.UnixMilli(now).Format("20060102")

	// 10K entries range
	stime := now
	etime := now + 100000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		reader.ReadByTime(date, stime, etime, func(data []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkXLogRD_ReadByTime_50K(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 50000)
	date := time.UnixMilli(now).Format("20060102")

	stime := now
	etime := now + 500000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		reader.ReadByTime(date, stime, etime, func(data []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkXLogRD_ReadFromEndTime_10K(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 50000)
	date := time.UnixMilli(now).Format("20060102")

	stime := now
	etime := now + 100000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		reader.ReadFromEndTime(date, stime, etime, func(data []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkXLogRD_ReadByTime_EarlyStop(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 50000)
	date := time.UnixMilli(now).Format("20060102")

	stime := now
	etime := now + 500000 // full range but stop early

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		reader.ReadByTime(date, stime, etime, func(data []byte) bool {
			count++
			return count < 100 // stop after 100 entries
		})
	}
}

func BenchmarkXLogRD_GetByTxid(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 100000)
	date := time.UnixMilli(now).Format("20060102")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.GetByTxid(date, int64(i%100000+1000000))
	}
}

func BenchmarkXLogRD_GetByTxid_Concurrent(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 100000)
	date := time.UnixMilli(now).Format("20060102")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			reader.GetByTxid(date, int64(idx%100000+1000000))
			idx++
		}
	})
}

func BenchmarkXLogRD_ReadByGxid(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 50000)
	date := time.UnixMilli(now).Format("20060102")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.ReadByGxid(date, int64(i%10000+5000000), func(data []byte) {})
	}
}

func BenchmarkXLogRD_ReadByTime_Concurrent(b *testing.B) {
	dir := benchDir(b)
	_, reader, now := setupXLogData(b, dir, 50000)
	date := time.UnixMilli(now).Format("20060102")

	stime := now
	etime := now + 100000

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			count := 0
			reader.ReadByTime(date, stime, etime, func(data []byte) bool {
				count++
				return true
			})
		}
	})
}

// ============================================================================
// XLogWR ReadByTime (through writer's live containers)
// ============================================================================

func BenchmarkXLogWR_ReadByTime_10K(b *testing.B) {
	dir := benchDir(b)

	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	data := make([]byte, 256)
	n := 50000
	for i := 0; i < n; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i)*10,
			Txid:    int64(i + 1000000),
			Gxid:    0,
			Elapsed: int32(i % 5000),
			Data:    data,
		})
	}
	time.Sleep(500 * time.Millisecond) // let batch process

	date := time.UnixMilli(now).Format("20060102")
	stime := now
	etime := now + 100000

	b.Cleanup(func() {
		cancel()
		writer.Close()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		writer.ReadByTime(date, stime, etime, func(data []byte) bool {
			count++
			return true
		})
	}
}

// ============================================================================
// Bulk End-to-End Benchmarks
// ============================================================================

func BenchmarkXLog_EndToEnd_WriteAndRead_10K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dir := benchDir(b)
		writer := NewXLogWR(dir)
		ctx, cancel := context.WithCancel(context.Background())
		writer.Start(ctx)

		now := time.Now().UnixMilli()
		data := make([]byte, 256)
		n := 10000

		for j := 0; j < n; j++ {
			writer.Add(&XLogEntry{
				Time:    now + int64(j)*10,
				Txid:    int64(j + 1000000),
				Gxid:    0,
				Elapsed: int32(j % 5000),
				Data:    data,
			})
		}
		time.Sleep(300 * time.Millisecond)
		cancel()
		writer.Close()

		reader := NewXLogRD(dir)
		date := time.UnixMilli(now).Format("20060102")
		count := 0
		reader.ReadByTime(date, now, now+int64(n)*10, func(data []byte) bool {
			count++
			return true
		})
		reader.Close()
	}
}

// ============================================================================
// Simulating Real-World Load: Mixed Reads During Writes
// ============================================================================

func BenchmarkXLog_MixedReadWrite(b *testing.B) {
	dir := benchDir(b)

	writer := NewXLogWR(dir)
	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	now := time.Now().UnixMilli()
	data := make([]byte, 256)
	n := 50000
	for i := 0; i < n; i++ {
		writer.Add(&XLogEntry{
			Time:    now + int64(i)*10,
			Txid:    int64(i + 1000000),
			Gxid:    0,
			Elapsed: int32(i % 5000),
			Data:    data,
		})
	}
	time.Sleep(500 * time.Millisecond)

	date := time.UnixMilli(now).Format("20060102")

	b.Cleanup(func() {
		cancel()
		writer.Close()
	})

	// Concurrent: writers adding new entries while readers scan past data
	var writeCounter int64
	var mu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			if idx%10 == 0 {
				// 10% writes
				mu.Lock()
				writeCounter++
				wc := writeCounter
				mu.Unlock()
				writer.Add(&XLogEntry{
					Time:    now + int64(n)*10 + wc*10,
					Txid:    int64(n) + wc + 1000000,
					Gxid:    0,
					Elapsed: 100,
					Data:    data,
				})
			} else if idx%10 == 1 {
				// 10% txid lookups
				writer.GetByTxid(date, int64(idx%n+1000000))
			} else {
				// 80% time range reads (small window)
				stime := now + int64(idx%((n-100)*10))
				etime := stime + 1000
				count := 0
				writer.ReadByTime(date, stime, etime, func(data []byte) bool {
					count++
					return count < 50
				})
			}
			idx++
		}
	})
}
