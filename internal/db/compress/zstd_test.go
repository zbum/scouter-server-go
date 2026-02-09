package compress

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sizes := []int{0, 1, 100, 1024, 64 * 1024}
	for _, size := range sizes {
		data := make([]byte, size)
		rand.Read(data)

		compressed := pool.Compress(data)
		decoded, err := pool.Decode(compressed)
		if err != nil {
			t.Fatalf("size=%d: Decode error: %v", size, err)
		}
		if !bytes.Equal(data, decoded) {
			t.Fatalf("size=%d: roundtrip mismatch", size)
		}
	}
}

func TestLegacyPassthrough(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Legacy data: first byte != 0x00 (e.g., a Pack type byte >= 10)
	legacy := []byte{10, 1, 2, 3, 4, 5}
	decoded, err := pool.Decode(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(legacy, decoded) {
		t.Fatal("legacy data should pass through unchanged")
	}
}

func TestDecodeEmpty(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	decoded, err := pool.Decode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != nil {
		t.Fatal("nil input should return nil")
	}

	decoded, err = pool.Decode([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Fatal("empty input should return empty")
	}
}

func TestNewFormatRaw(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Manually construct a new-format raw body: [0x00][0x00][payload]
	payload := []byte{1, 2, 3, 4, 5}
	body := append([]byte{flagNewFormat, compTypeRaw}, payload...)

	decoded, err := pool.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, decoded) {
		t.Fatal("new-format raw should strip 2-byte header")
	}
}

func TestConcurrency(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := make([]byte, 1024)
			rand.Read(data)
			compressed := pool.Compress(data)
			decoded, err := pool.Decode(compressed)
			if err != nil {
				t.Errorf("Decode error: %v", err)
				return
			}
			if !bytes.Equal(data, decoded) {
				t.Error("roundtrip mismatch in concurrent test")
			}
		}()
	}
	wg.Wait()
}
