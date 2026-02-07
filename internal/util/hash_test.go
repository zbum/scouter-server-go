package util

import "testing"

func TestHashBytes(t *testing.T) {
	// Verify deterministic behavior
	h1 := HashBytes([]byte("hello world"))
	h2 := HashBytes([]byte("hello world"))
	if h1 != h2 {
		t.Errorf("hash not deterministic: %d != %d", h1, h2)
	}

	// Different input should produce different hash
	h3 := HashBytes([]byte("hello world!"))
	if h1 == h3 {
		t.Error("different inputs produced same hash")
	}

	// Empty input should not panic
	_ = HashBytes([]byte{})
	_ = HashBytes(nil)
}

func TestHashString(t *testing.T) {
	h1 := HashString("hello world")
	h2 := HashBytes([]byte("hello world"))
	if h1 != h2 {
		t.Errorf("HashString != HashBytes: %d != %d", h1, h2)
	}
}
