package util

import "strconv"

// Hexa32ToString32 converts an int32 hash to a Hexa32 string representation
// matching Java's scouter.util.Hexa32.toString32().
// - 0..9: returned as decimal string ("0" through "9")
// - positive >=10: "x" + base-32 representation
// - negative: "z" + base-32 of absolute value
func Hexa32ToString32(h int32) string {
	if h >= 0 && h < 10 {
		return strconv.Itoa(int(h))
	}
	if h >= 0 {
		return "x" + strconv.FormatInt(int64(h), 32)
	}
	// negative
	return "z" + strconv.FormatInt(-int64(h), 32)
}
