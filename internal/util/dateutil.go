package util

import "time"

const (
	MillisPerSecond     = 1000
	MillisPerMinute     = 60 * MillisPerSecond
	MillisPerFiveMinute = 5 * MillisPerMinute
	MillisPerHour       = 60 * MillisPerMinute
	MillisPerDay        = 24 * MillisPerHour
	SecondsPerDay       = 86400
	BucketsPerDay       = 288 // 24*60/5
)

// GetDateMillis returns the milliseconds elapsed since midnight (local time) for the given
// Unix timestamp in milliseconds. This matches Java's DateUtil.getDateMillis().
func GetDateMillis(timeMs int64) int {
	t := time.UnixMilli(timeMs)
	y, m, d := t.Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, t.Location())
	return int(t.Sub(midnight).Milliseconds())
}

// FormatDate returns the date part of a Unix timestamp in milliseconds as "YYYYMMDD".
func FormatDate(timeMs int64) string {
	t := time.UnixMilli(timeMs)
	return t.Format("20060102")
}

// HHMM returns the "HHmm" string for a Unix timestamp in milliseconds.
// For example, 14:30 → "1430", 09:05 → "0905".
func HHMM(timeMs int64) string {
	t := time.UnixMilli(timeMs)
	return t.Format("1504")
}

// DateToMillis converts a "YYYYMMDD" date string to Unix millis at midnight local time.
func DateToMillis(date string) int64 {
	t, err := time.ParseInLocation("20060102", date, time.Now().Location())
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}
