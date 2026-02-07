package util

import (
	"testing"
	"time"
)

func TestGetDateMillis(t *testing.T) {
	// Use local timezone since GetDateMillis works in local time
	tm := time.Date(2024, 1, 15, 10, 30, 45, 123000000, time.Local)
	ms := tm.UnixMilli()

	expected := 10*MillisPerHour + 30*MillisPerMinute + 45*MillisPerSecond + 123
	got := GetDateMillis(ms)
	if got != expected {
		t.Errorf("expected %d, got %d", expected, got)
	}
}

func TestGetDateMillisMidnight(t *testing.T) {
	tm := time.Date(2024, 1, 15, 0, 0, 0, 0, time.Local)
	ms := tm.UnixMilli()
	got := GetDateMillis(ms)
	if got != 0 {
		t.Errorf("expected 0 for midnight, got %d", got)
	}
}

func TestFormatDate(t *testing.T) {
	tm := time.Date(2024, 1, 15, 10, 30, 0, 0, time.Local)
	ms := tm.UnixMilli()
	got := FormatDate(ms)
	if got != "20240115" {
		t.Errorf("expected '20240115', got %q", got)
	}
}
