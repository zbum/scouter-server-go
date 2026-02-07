package protocol

import (
	"encoding/binary"
	"io"
	"math"
)

const (
	INT3MinValue  = int32(-8388608)   // 0xff800000 sign-extended
	INT3MaxValue  = int32(8388607)    // 0x007fffff
	LONG5MinValue = int64(-549755813888) // 0xffffff8000000000 sign-extended
	LONG5MaxValue = int64(549755813887)  // 0x0000007fffffffff
)

type DataOutputX struct {
	buf     []byte
	written int
	writer  io.Writer // optional: when set, writes to stream instead of buffer
}

func NewDataOutputX() *DataOutputX {
	return &DataOutputX{buf: make([]byte, 0, 256)}
}

func NewDataOutputXWithSize(size int) *DataOutputX {
	return &DataOutputX{buf: make([]byte, 0, size)}
}

// NewDataOutputXStream creates a stream-based DataOutputX that writes to an io.Writer.
func NewDataOutputXStream(w io.Writer) *DataOutputX {
	return &DataOutputX{writer: w}
}

func (o *DataOutputX) ToByteArray() []byte {
	return o.buf
}

func (o *DataOutputX) Size() int {
	return o.written
}

// Flush writes any buffered data to the underlying writer (stream mode only).
func (o *DataOutputX) Flush() error {
	if o.writer == nil {
		return nil
	}
	if f, ok := o.writer.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

func (o *DataOutputX) Write(b []byte) *DataOutputX {
	if o.writer != nil {
		o.writer.Write(b)
		o.written += len(b)
		return o
	}
	o.buf = append(o.buf, b...)
	o.written += len(b)
	return o
}

func (o *DataOutputX) WriteByte(v byte) *DataOutputX {
	if o.writer != nil {
		o.writer.Write([]byte{v})
		o.written++
		return o
	}
	o.buf = append(o.buf, v)
	o.written++
	return o
}

func (o *DataOutputX) WriteBoolean(v bool) *DataOutputX {
	if v {
		o.WriteByte(1)
	} else {
		o.WriteByte(0)
	}
	return o
}

func (o *DataOutputX) WriteInt16(v int16) *DataOutputX {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return o.Write(b)
}

func (o *DataOutputX) WriteShort(v int) *DataOutputX {
	return o.WriteInt16(int16(v))
}

func (o *DataOutputX) WriteUint16(v uint16) *DataOutputX {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return o.Write(b)
}

func (o *DataOutputX) WriteInt32(v int32) *DataOutputX {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return o.Write(b)
}

func (o *DataOutputX) WriteInt(v int32) *DataOutputX {
	return o.WriteInt32(v)
}

func (o *DataOutputX) WriteInt64(v int64) *DataOutputX {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return o.Write(b)
}

func (o *DataOutputX) WriteLong(v int64) *DataOutputX {
	return o.WriteInt64(v)
}

func (o *DataOutputX) WriteFloat32(v float32) *DataOutputX {
	return o.WriteInt32(int32(math.Float32bits(v)))
}

func (o *DataOutputX) WriteFloat(v float32) *DataOutputX {
	return o.WriteFloat32(v)
}

func (o *DataOutputX) WriteFloat64(v float64) *DataOutputX {
	return o.WriteInt64(int64(math.Float64bits(v)))
}

func (o *DataOutputX) WriteDouble(v float64) *DataOutputX {
	return o.WriteFloat64(v)
}

func (o *DataOutputX) WriteInt3(v int32) *DataOutputX {
	b := make([]byte, 3)
	b[0] = byte((v >> 16) & 0xFF)
	b[1] = byte((v >> 8) & 0xFF)
	b[2] = byte(v & 0xFF)
	return o.Write(b)
}

func (o *DataOutputX) WriteLong5(v int64) *DataOutputX {
	b := make([]byte, 5)
	b[0] = byte(v >> 32)
	b[1] = byte(v >> 24)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 8)
	b[4] = byte(v)
	return o.Write(b)
}

func (o *DataOutputX) WriteDecimal(v int64) *DataOutputX {
	if v == 0 {
		o.WriteByte(0)
	} else if v >= -128 && v <= 127 {
		o.WriteByte(1)
		o.WriteByte(byte(int8(v)))
	} else if v >= -32768 && v <= 32767 {
		o.WriteByte(2)
		o.WriteInt16(int16(v))
	} else if int64(INT3MinValue) <= v && v <= int64(INT3MaxValue) {
		o.WriteByte(3)
		o.WriteInt3(int32(v))
	} else if v >= -2147483648 && v <= 2147483647 {
		o.WriteByte(4)
		o.WriteInt32(int32(v))
	} else if LONG5MinValue <= v && v <= LONG5MaxValue {
		o.WriteByte(5)
		o.WriteLong5(v)
	} else {
		o.WriteByte(8)
		o.WriteInt64(v)
	}
	return o
}

func (o *DataOutputX) WriteBlob(value []byte) *DataOutputX {
	if value == nil || len(value) == 0 {
		o.WriteByte(0)
	} else {
		length := len(value)
		if length <= 253 {
			o.WriteByte(byte(length))
			o.Write(value)
		} else if length <= 65535 {
			o.WriteByte(255)
			o.WriteUint16(uint16(length))
			o.Write(value)
		} else {
			o.WriteByte(254)
			o.WriteInt32(int32(length))
			o.Write(value)
		}
	}
	return o
}

func (o *DataOutputX) WriteText(s string) *DataOutputX {
	if s == "" {
		o.WriteByte(0)
	} else {
		o.WriteBlob([]byte(s))
	}
	return o
}

func (o *DataOutputX) WriteIntBytes(b []byte) *DataOutputX {
	o.WriteInt32(int32(len(b)))
	o.Write(b)
	return o
}

func (o *DataOutputX) WriteShortBytes(b []byte) *DataOutputX {
	o.WriteInt16(int16(len(b)))
	o.Write(b)
	return o
}

func (o *DataOutputX) WriteArrayInt(v []int32) *DataOutputX {
	if v == nil {
		o.WriteShort(0)
	} else {
		o.WriteShort(len(v))
		for _, val := range v {
			o.WriteInt32(val)
		}
	}
	return o
}

func (o *DataOutputX) WriteArrayLong(v []int64) *DataOutputX {
	if v == nil {
		o.WriteShort(0)
	} else {
		o.WriteShort(len(v))
		for _, val := range v {
			o.WriteInt64(val)
		}
	}
	return o
}

func (o *DataOutputX) WriteArrayFloat(v []float32) *DataOutputX {
	if v == nil {
		o.WriteShort(0)
	} else {
		o.WriteShort(len(v))
		for _, val := range v {
			o.WriteFloat32(val)
		}
	}
	return o
}

func (o *DataOutputX) WriteDecimalArray(v []int64) *DataOutputX {
	if v == nil {
		o.WriteDecimal(0)
	} else {
		o.WriteDecimal(int64(len(v)))
		for _, val := range v {
			o.WriteDecimal(val)
		}
	}
	return o
}

// ToBytes5 converts an int64 value to a 5-byte big-endian byte array.
func ToBytes5(v int64) []byte {
	b := make([]byte, 5)
	b[0] = byte(v >> 32)
	b[1] = byte(v >> 24)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 8)
	b[4] = byte(v)
	return b
}

// ToBytesInt converts an int32 value to a 4-byte big-endian byte array.
func ToBytesInt(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

// ToBytesLong converts an int64 value to an 8-byte big-endian byte array.
func ToBytesLong(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// SetBytes copies data into buf starting at position pos.
func SetBytes(buf []byte, pos int, data []byte) {
	copy(buf[pos:], data)
}

func (o *DataOutputX) WriteDecimalIntArray(v []int32) *DataOutputX {
	if v == nil {
		o.WriteDecimal(0)
	} else {
		o.WriteDecimal(int64(len(v)))
		for _, val := range v {
			o.WriteDecimal(int64(val))
		}
	}
	return o
}
