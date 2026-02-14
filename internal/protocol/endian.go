package protocol

import "encoding/binary"

// BigEndian provides big-endian byte conversions for Scouter's wire format.
// Extends encoding/binary.BigEndian with 3-byte and 5-byte signed integer support.
var BigEndian bigEndian

type bigEndian struct{}

// Int3 reads a signed int32 from 3 bytes (big-endian with sign extension).
func (bigEndian) Int3(b []byte) int32 {
	return ((int32(b[0]) << 24) + (int32(b[1]) << 16) + (int32(b[2]) << 8)) >> 8
}

// PutInt3 writes a signed int32 as 3 bytes (big-endian).
func (bigEndian) PutInt3(b []byte, v int32) {
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}

// Int5 reads a signed int64 from 5 bytes (big-endian with sign extension).
func (bigEndian) Int5(b []byte) int64 {
	return (int64(int8(b[0])) << 32) |
		(int64(b[1]) << 24) |
		(int64(b[2]) << 16) |
		(int64(b[3]) << 8) |
		int64(b[4])
}

// PutInt5 writes a signed int64 as 5 bytes (big-endian).
func (bigEndian) PutInt5(b []byte, v int64) {
	b[0] = byte(v >> 32)
	b[1] = byte(v >> 24)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 8)
	b[4] = byte(v)
}

// Int32 reads a signed int32 from 4 bytes (big-endian).
func (bigEndian) Int32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

// PutInt32 writes a signed int32 as 4 bytes (big-endian).
func (bigEndian) PutInt32(b []byte, v int32) {
	binary.BigEndian.PutUint32(b, uint32(v))
}

// Int64 reads a signed int64 from 8 bytes (big-endian).
func (bigEndian) Int64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// PutInt64 writes a signed int64 as 8 bytes (big-endian).
func (bigEndian) PutInt64(b []byte, v int64) {
	binary.BigEndian.PutUint64(b, uint64(v))
}

// Bytes5 returns a new 5-byte big-endian representation of v.
func (e bigEndian) Bytes5(v int64) []byte {
	b := make([]byte, 5)
	e.PutInt5(b, v)
	return b
}

// Bytes4 returns a new 4-byte big-endian representation of v.
func (e bigEndian) Bytes4(v int32) []byte {
	b := make([]byte, 4)
	e.PutInt32(b, v)
	return b
}

// Bytes8 returns a new 8-byte big-endian representation of v.
func (e bigEndian) Bytes8(v int64) []byte {
	b := make([]byte, 8)
	e.PutInt64(b, v)
	return b
}