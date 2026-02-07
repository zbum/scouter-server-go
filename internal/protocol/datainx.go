package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var (
	ErrEOF           = errors.New("unexpected end of data")
	ErrUnknownType   = errors.New("unknown type code")
)

type DataInputX struct {
	buf    []byte
	offset int
	reader io.Reader // optional: when set, reads from stream instead of buffer
}

func NewDataInputX(buf []byte) *DataInputX {
	return &DataInputX{buf: buf, offset: 0}
}

func NewDataInputXOffset(buf []byte, offset int) *DataInputX {
	return &DataInputX{buf: buf, offset: offset}
}

// NewDataInputXStream creates a stream-based DataInputX that reads from an io.Reader.
func NewDataInputXStream(r io.Reader) *DataInputX {
	return &DataInputX{reader: r}
}

func (d *DataInputX) Available() int {
	if d.reader != nil {
		return math.MaxInt32 // stream mode: always "available"
	}
	return len(d.buf) - d.offset
}

func (d *DataInputX) Offset() int {
	return d.offset
}

func (d *DataInputX) Read(n int) ([]byte, error) {
	if d.reader != nil {
		b := make([]byte, n)
		_, err := io.ReadFull(d.reader, b)
		if err != nil {
			return nil, err
		}
		d.offset += n
		return b, nil
	}
	if d.offset+n > len(d.buf) {
		return nil, ErrEOF
	}
	b := make([]byte, n)
	copy(b, d.buf[d.offset:d.offset+n])
	d.offset += n
	return b, nil
}

func (d *DataInputX) ReadByte() (byte, error) {
	if d.reader != nil {
		var buf [1]byte
		_, err := io.ReadFull(d.reader, buf[:])
		if err != nil {
			return 0, err
		}
		d.offset++
		return buf[0], nil
	}
	if d.offset >= len(d.buf) {
		return 0, ErrEOF
	}
	v := d.buf[d.offset]
	d.offset++
	return v, nil
}

func (d *DataInputX) ReadUnsignedByte() (int, error) {
	b, err := d.ReadByte()
	if err != nil {
		return 0, err
	}
	return int(b) & 0xFF, nil
}

func (d *DataInputX) ReadBoolean() (bool, error) {
	b, err := d.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

func (d *DataInputX) ReadInt16() (int16, error) {
	if d.reader != nil {
		b, err := d.Read(2)
		if err != nil {
			return 0, err
		}
		return int16(binary.BigEndian.Uint16(b)), nil
	}
	if d.offset+2 > len(d.buf) {
		return 0, ErrEOF
	}
	v := int16(binary.BigEndian.Uint16(d.buf[d.offset:]))
	d.offset += 2
	return v, nil
}

func (d *DataInputX) ReadShort() (int16, error) {
	return d.ReadInt16()
}

func (d *DataInputX) ReadUnsignedShort() (int, error) {
	if d.reader != nil {
		b, err := d.Read(2)
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint16(b)), nil
	}
	if d.offset+2 > len(d.buf) {
		return 0, ErrEOF
	}
	v := int(binary.BigEndian.Uint16(d.buf[d.offset:]))
	d.offset += 2
	return v, nil
}

func (d *DataInputX) ReadInt32() (int32, error) {
	if d.reader != nil {
		b, err := d.Read(4)
		if err != nil {
			return 0, err
		}
		return int32(binary.BigEndian.Uint32(b)), nil
	}
	if d.offset+4 > len(d.buf) {
		return 0, ErrEOF
	}
	v := int32(binary.BigEndian.Uint32(d.buf[d.offset:]))
	d.offset += 4
	return v, nil
}

func (d *DataInputX) ReadInt() (int32, error) {
	return d.ReadInt32()
}

func (d *DataInputX) ReadInt64() (int64, error) {
	if d.reader != nil {
		b, err := d.Read(8)
		if err != nil {
			return 0, err
		}
		return int64(binary.BigEndian.Uint64(b)), nil
	}
	if d.offset+8 > len(d.buf) {
		return 0, ErrEOF
	}
	v := int64(binary.BigEndian.Uint64(d.buf[d.offset:]))
	d.offset += 8
	return v, nil
}

func (d *DataInputX) ReadLong() (int64, error) {
	return d.ReadInt64()
}

func (d *DataInputX) ReadFloat32() (float32, error) {
	v, err := d.ReadInt32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(uint32(v)), nil
}

func (d *DataInputX) ReadFloat() (float32, error) {
	return d.ReadFloat32()
}

func (d *DataInputX) ReadFloat64() (float64, error) {
	v, err := d.ReadInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(v)), nil
}

func (d *DataInputX) ReadDouble() (float64, error) {
	return d.ReadFloat64()
}

func (d *DataInputX) ReadInt3() (int32, error) {
	b, err := d.Read(3)
	if err != nil {
		return 0, err
	}
	return ToInt3(b, 0), nil
}

func (d *DataInputX) ReadLong5() (int64, error) {
	b, err := d.Read(5)
	if err != nil {
		return 0, err
	}
	return ToLong5(b, 0), nil
}

func (d *DataInputX) ReadDecimal() (int64, error) {
	lenByte, err := d.ReadByte()
	if err != nil {
		return 0, err
	}
	switch int8(lenByte) {
	case 0:
		return 0, nil
	case 1:
		b, err := d.ReadByte()
		if err != nil {
			return 0, err
		}
		return int64(int8(b)), nil
	case 2:
		v, err := d.ReadInt16()
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	case 3:
		v, err := d.ReadInt3()
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	case 4:
		v, err := d.ReadInt32()
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	case 5:
		return d.ReadLong5()
	default:
		return d.ReadInt64()
	}
}

func (d *DataInputX) ReadBlob() ([]byte, error) {
	baseLen, err := d.ReadUnsignedByte()
	if err != nil {
		return nil, err
	}
	switch baseLen {
	case 0:
		return []byte{}, nil
	case 255:
		length, err := d.ReadUnsignedShort()
		if err != nil {
			return nil, err
		}
		return d.Read(length)
	case 254:
		length, err := d.ReadInt32()
		if err != nil {
			return nil, err
		}
		return d.Read(int(length))
	default:
		return d.Read(baseLen)
	}
}

func (d *DataInputX) ReadText() (string, error) {
	b, err := d.ReadBlob()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (d *DataInputX) ReadIntBytes() ([]byte, error) {
	length, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}
	return d.Read(int(length))
}

func (d *DataInputX) ReadShortBytes() ([]byte, error) {
	length, err := d.ReadUnsignedShort()
	if err != nil {
		return nil, err
	}
	return d.Read(length)
}

func (d *DataInputX) ReadArrayInt() ([]int32, error) {
	length, err := d.ReadShort()
	if err != nil {
		return nil, err
	}
	data := make([]int32, length)
	for i := int16(0); i < length; i++ {
		v, err := d.ReadInt32()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}
	return data, nil
}

func (d *DataInputX) ReadArrayLong() ([]int64, error) {
	length, err := d.ReadShort()
	if err != nil {
		return nil, err
	}
	data := make([]int64, length)
	for i := int16(0); i < length; i++ {
		v, err := d.ReadInt64()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}
	return data, nil
}

func (d *DataInputX) ReadArrayFloat() ([]float32, error) {
	length, err := d.ReadShort()
	if err != nil {
		return nil, err
	}
	data := make([]float32, length)
	for i := int16(0); i < length; i++ {
		v, err := d.ReadFloat32()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}
	return data, nil
}

func (d *DataInputX) ReadDecimalArray() ([]int64, error) {
	length, err := d.ReadDecimal()
	if err != nil {
		return nil, err
	}
	data := make([]int64, length)
	for i := int64(0); i < length; i++ {
		v, err := d.ReadDecimal()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}
	return data, nil
}

func (d *DataInputX) ReadDecimalIntArray() ([]int32, error) {
	length, err := d.ReadDecimal()
	if err != nil {
		return nil, err
	}
	data := make([]int32, length)
	for i := int64(0); i < length; i++ {
		v, err := d.ReadDecimal()
		if err != nil {
			return nil, err
		}
		data[i] = int32(v)
	}
	return data, nil
}

func (d *DataInputX) SkipBytes(n int) error {
	if d.reader != nil {
		_, err := d.Read(n)
		return err
	}
	if d.offset+n > len(d.buf) {
		return ErrEOF
	}
	d.offset += n
	return nil
}

// ToInt3 converts 3 bytes at position pos to a signed int32.
// Uses sign extension: ((ch1<<24 + ch2<<16 + ch3<<8) >> 8) in Java.
func ToInt3(buf []byte, pos int) int32 {
	ch1 := int32(buf[pos]) & 0xFF
	ch2 := int32(buf[pos+1]) & 0xFF
	ch3 := int32(buf[pos+2]) & 0xFF
	return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8)) >> 8
}

// ToLong5 converts 5 bytes at position pos to a signed int64.
func ToLong5(buf []byte, pos int) int64 {
	return (int64(int8(buf[pos])) << 32) |
		(int64(buf[pos+1]&0xFF) << 24) |
		(int64(buf[pos+2]&0xFF) << 16) |
		(int64(buf[pos+3]&0xFF) << 8) |
		int64(buf[pos+4]&0xFF)
}

// ToInt converts 4 bytes at position pos to a signed int32 (big-endian).
func ToInt(buf []byte, pos int) int32 {
	return int32(binary.BigEndian.Uint32(buf[pos:]))
}

// ToLong converts 8 bytes at position pos to a signed int64 (big-endian).
func ToLong(buf []byte, pos int) int64 {
	return int64(binary.BigEndian.Uint64(buf[pos:]))
}
