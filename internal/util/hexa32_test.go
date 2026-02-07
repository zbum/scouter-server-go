package util

import "testing"

func TestHexa32ToString32(t *testing.T) {
	tests := []struct {
		input    int32
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{9, "9"},
		{10, "xa"},
		{100, "x34"},
		{-1, "z1"},
		{-100, "z34"},
		{-554939494, "zgh7d36"},
	}
	for _, tc := range tests {
		got := Hexa32ToString32(tc.input)
		if got != tc.expected {
			t.Errorf("Hexa32ToString32(%d) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}
