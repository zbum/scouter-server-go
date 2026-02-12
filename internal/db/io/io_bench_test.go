package io

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

func benchDir(b *testing.B) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "scouter-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// ============================================================================
// MemHashBlock Benchmarks
// ============================================================================

func BenchmarkMemHashBlock_Put(b *testing.B) {
	dir := benchDir(b)
	m, err := NewMemHashBlock(filepath.Join(dir, "bench"), 1*MB)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(int32(i), int64(i*100))
	}
}

func BenchmarkMemHashBlock_Get(b *testing.B) {
	dir := benchDir(b)
	m, err := NewMemHashBlock(filepath.Join(dir, "bench"), 1*MB)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()

	// Pre-populate
	for i := 0; i < 100000; i++ {
		m.Put(int32(i), int64(i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get(int32(i % 100000))
	}
}

func BenchmarkMemHashBlock_PutGet_Mixed(b *testing.B) {
	dir := benchDir(b)
	m, err := NewMemHashBlock(filepath.Join(dir, "bench"), 1*MB)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			m.Put(int32(i), int64(i))
		} else {
			m.Get(int32(i / 2))
		}
	}
}

// ============================================================================
// RealDataFile Benchmarks
// ============================================================================

func BenchmarkRealDataFile_Write(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			dir := benchDir(b)
			df, err := NewRealDataFile(filepath.Join(dir, "data.dat"))
			if err != nil {
				b.Fatal(err)
			}
			defer df.Close()

			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i)
			}

			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := df.Write(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRealDataFile_WriteAndFlush(b *testing.B) {
	dir := benchDir(b)
	df, err := NewRealDataFile(filepath.Join(dir, "data.dat"))
	if err != nil {
		b.Fatal(err)
	}
	defer df.Close()

	data := make([]byte, 256)
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := df.Write(data); err != nil {
			b.Fatal(err)
		}
		if i%100 == 99 {
			df.Flush()
		}
	}
}

// ============================================================================
// RealKeyFile Benchmarks
// ============================================================================

func BenchmarkRealKeyFile_Append(b *testing.B) {
	dir := benchDir(b)
	kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer kf.Close()

	key := protocol.ToBytesLong(12345)
	val := protocol.ToBytes5(99999)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kf.Append(0, key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRealKeyFile_GetRecord_Sequential(b *testing.B) {
	dir := benchDir(b)
	kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer kf.Close()

	// Pre-populate with N records
	n := 100000
	positions := make([]int64, n)
	for i := 0; i < n; i++ {
		key := protocol.ToBytesLong(int64(i))
		val := protocol.ToBytes5(int64(i * 10))
		pos, err := kf.Append(0, key, val)
		if err != nil {
			b.Fatal(err)
		}
		positions[i] = pos
	}
	kf.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kf.GetRecord(positions[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRealKeyFile_GetRecord_Random(b *testing.B) {
	dir := benchDir(b)
	kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer kf.Close()

	n := 100000
	positions := make([]int64, n)
	for i := 0; i < n; i++ {
		key := protocol.ToBytesLong(int64(i))
		val := protocol.ToBytes5(int64(i * 10))
		pos, err := kf.Append(0, key, val)
		if err != nil {
			b.Fatal(err)
		}
		positions[i] = pos
	}
	kf.Flush()

	// Shuffle for random access
	rng := rand.New(rand.NewSource(42))
	shuffled := make([]int64, n)
	copy(shuffled, positions)
	rng.Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kf.GetRecord(shuffled[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRealKeyFile_GetRecord_Concurrent(b *testing.B) {
	dir := benchDir(b)
	kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer kf.Close()

	n := 100000
	positions := make([]int64, n)
	for i := 0; i < n; i++ {
		key := protocol.ToBytesLong(int64(i))
		val := protocol.ToBytes5(int64(i * 10))
		pos, err := kf.Append(0, key, val)
		if err != nil {
			b.Fatal(err)
		}
		positions[i] = pos
	}
	kf.Flush()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			if _, err := kf.GetRecord(positions[idx%n]); err != nil {
				b.Fatal(err)
			}
			idx++
		}
	})
}

func BenchmarkRealKeyFile_AppendAndRead_Mixed(b *testing.B) {
	dir := benchDir(b)
	kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer kf.Close()

	positions := make([]int64, 0, 10000)
	// Seed with some data
	for i := 0; i < 1000; i++ {
		key := protocol.ToBytesLong(int64(i))
		val := protocol.ToBytes5(int64(i))
		pos, _ := kf.Append(0, key, val)
		positions = append(positions, pos)
	}
	kf.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			// 10% writes
			key := protocol.ToBytesLong(int64(i))
			val := protocol.ToBytes5(int64(i))
			pos, err := kf.Append(0, key, val)
			if err != nil {
				b.Fatal(err)
			}
			positions = append(positions, pos)
		} else {
			// 90% reads
			idx := i % len(positions)
			if _, err := kf.GetRecord(positions[idx]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// ============================================================================
// IndexKeyFile Benchmarks
// ============================================================================

func BenchmarkIndexKeyFile_Put(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i * 100))
		if err := idx.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexKeyFile_Get(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	n := 100000
	for i := 0; i < n; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i * 100))
		idx.Put(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := protocol.ToBytesInt(int32(i % n))
		if _, err := idx.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexKeyFile_Get_Random(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	n := 100000
	keys := make([]int32, n)
	for i := 0; i < n; i++ {
		keys[i] = int32(i)
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i * 100))
		idx.Put(key, val)
	}

	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := protocol.ToBytesInt(keys[i%n])
		if _, err := idx.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexKeyFile_HasKey(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	n := 100000
	for i := 0; i < n; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i * 100))
		idx.Put(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := protocol.ToBytesInt(int32(i % n))
		if _, err := idx.HasKey(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexKeyFile_PutGet_HighCollision(b *testing.B) {
	// Use a small hash table to force many hash collisions (long chains)
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1) // 1MB hash = ~200K buckets
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	// Pre-populate: 500K entries in 200K buckets → avg chain depth ~2.5
	n := 500000
	for i := 0; i < n; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i))
		idx.Put(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := protocol.ToBytesInt(int32(i % n))
		if _, err := idx.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexKeyFile_Read_FullScan(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	n := 50000
	for i := 0; i < n; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i))
		idx.Put(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		idx.Read(func(key []byte, data []byte) {
			count++
		})
	}
}

// ============================================================================
// IndexTimeFile Benchmarks
// ============================================================================

func BenchmarkIndexTimeFile_Put(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timeMs := baseTime + int64(i) // 1ms apart → many entries per 500ms bucket
		dp := protocol.ToBytes5(int64(i * 10))
		if _, err := idx.Put(timeMs, dp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexTimeFile_Read_SmallRange(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	n := 100000
	for i := 0; i < n; i++ {
		timeMs := baseTime + int64(i)*10 // 10ms apart
		dp := protocol.ToBytes5(int64(i))
		idx.Put(timeMs, dp)
	}

	// 5-second range → ~500 entries
	stime := baseTime
	etime := baseTime + 5000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		idx.Read(stime, etime, func(time int64, dataPos []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkIndexTimeFile_Read_MediumRange(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	n := 100000
	for i := 0; i < n; i++ {
		timeMs := baseTime + int64(i)*10
		dp := protocol.ToBytes5(int64(i))
		idx.Put(timeMs, dp)
	}

	// 60-second range → ~6000 entries
	stime := baseTime
	etime := baseTime + 60000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		idx.Read(stime, etime, func(time int64, dataPos []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkIndexTimeFile_Read_LargeRange(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	n := 100000
	for i := 0; i < n; i++ {
		timeMs := baseTime + int64(i)*10
		dp := protocol.ToBytes5(int64(i))
		idx.Put(timeMs, dp)
	}

	// 10-minute range → ~60000 entries
	stime := baseTime
	etime := baseTime + 600000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		idx.Read(stime, etime, func(time int64, dataPos []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkIndexTimeFile_ReadFromEnd_MediumRange(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	n := 100000
	for i := 0; i < n; i++ {
		timeMs := baseTime + int64(i)*10
		dp := protocol.ToBytes5(int64(i))
		idx.Put(timeMs, dp)
	}

	stime := baseTime
	etime := baseTime + 60000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		idx.ReadFromEnd(stime, etime, func(time int64, dataPos []byte) bool {
			count++
			return true
		})
	}
}

func BenchmarkIndexTimeFile_Put_HighDensity(b *testing.B) {
	// Many entries in the same 500ms bucket (chain depth stress test)
	dir := benchDir(b)
	idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	baseTime := int64(1705312245000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Same bucket (within 500ms window) — chain grows long
		timeMs := baseTime + int64(i%500)
		dp := protocol.ToBytes5(int64(i))
		if _, err := idx.Put(timeMs, dp); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================================
// Concurrent Read/Write Benchmarks
// ============================================================================

func BenchmarkRealKeyFile_ConcurrentReadWrite(b *testing.B) {
	dir := benchDir(b)
	kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
	if err != nil {
		b.Fatal(err)
	}
	defer kf.Close()

	// Pre-populate
	n := 50000
	positions := make([]int64, n)
	for i := 0; i < n; i++ {
		key := protocol.ToBytesLong(int64(i))
		val := protocol.ToBytes5(int64(i))
		pos, _ := kf.Append(0, key, val)
		positions[i] = pos
	}
	kf.Flush()

	var mu sync.Mutex
	writeIdx := n

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localIdx := 0
		for pb.Next() {
			if localIdx%20 == 0 {
				// 5% writes
				mu.Lock()
				key := protocol.ToBytesLong(int64(writeIdx))
				val := protocol.ToBytes5(int64(writeIdx))
				kf.Append(0, key, val)
				writeIdx++
				mu.Unlock()
			} else {
				// 95% reads
				idx := localIdx % n
				kf.GetRecord(positions[idx])
			}
			localIdx++
		}
	})
}

func BenchmarkIndexKeyFile_ConcurrentGet(b *testing.B) {
	dir := benchDir(b)
	idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	n := 100000
	for i := 0; i < n; i++ {
		key := protocol.ToBytesInt(int32(i))
		val := protocol.ToBytes5(int64(i))
		idx.Put(key, val)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localIdx := 0
		for pb.Next() {
			key := protocol.ToBytesInt(int32(localIdx % n))
			idx.Get(key)
			localIdx++
		}
	})
}

// ============================================================================
// Bulk/Throughput Benchmarks
// ============================================================================

func BenchmarkIndexKeyFile_BulkPut_100K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dir := benchDir(b)
		idx, err := NewIndexKeyFile(filepath.Join(dir, "idx"), 1)
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 100000; j++ {
			key := protocol.ToBytesInt(int32(j))
			val := protocol.ToBytes5(int64(j))
			idx.Put(key, val)
		}
		idx.Close()
	}
}

func BenchmarkIndexTimeFile_BulkPut_100K(b *testing.B) {
	baseTime := int64(1705312245000)
	for i := 0; i < b.N; i++ {
		dir := benchDir(b)
		idx, err := NewIndexTimeFile(filepath.Join(dir, "tidx"))
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 100000; j++ {
			timeMs := baseTime + int64(j)*10
			dp := protocol.ToBytes5(int64(j))
			idx.Put(timeMs, dp)
		}
		idx.Close()
	}
}

func BenchmarkRealKeyFile_BulkAppend_100K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dir := benchDir(b)
		kf, err := NewRealKeyFile(filepath.Join(dir, "bench"))
		if err != nil {
			b.Fatal(err)
		}

		for j := 0; j < 100000; j++ {
			key := protocol.ToBytesLong(int64(j))
			val := protocol.ToBytes5(int64(j))
			kf.Append(0, key, val)
		}
		kf.Flush()
		kf.Close()
	}
}
