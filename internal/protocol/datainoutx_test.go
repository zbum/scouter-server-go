package protocol

import (
	"math"
	"testing"
)

func TestReadWriteByte(t *testing.T) {
	o := NewDataOutputX()
	o.WriteByte(0x00)
	o.WriteByte(0x7F)
	o.WriteByte(0xFF)

	d := NewDataInputX(o.ToByteArray())
	b, _ := d.ReadByte()
	if b != 0x00 {
		t.Errorf("expected 0x00, got 0x%02X", b)
	}
	b, _ = d.ReadByte()
	if b != 0x7F {
		t.Errorf("expected 0x7F, got 0x%02X", b)
	}
	b, _ = d.ReadByte()
	if b != 0xFF {
		t.Errorf("expected 0xFF, got 0x%02X", b)
	}
}

func TestReadWriteBoolean(t *testing.T) {
	o := NewDataOutputX()
	o.WriteBoolean(true)
	o.WriteBoolean(false)

	d := NewDataInputX(o.ToByteArray())
	v, _ := d.ReadBoolean()
	if !v {
		t.Error("expected true")
	}
	v, _ = d.ReadBoolean()
	if v {
		t.Error("expected false")
	}
}

func TestReadWriteInt16(t *testing.T) {
	cases := []int16{0, 1, -1, 32767, -32768, 256, -256}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteInt16(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadInt16()
		if err != nil {
			t.Fatalf("error reading int16 %d: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %d, got %d", c, v)
		}
	}
}

func TestReadWriteInt32(t *testing.T) {
	cases := []int32{0, 1, -1, 2147483647, -2147483648, 65536, -65536}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteInt32(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadInt32()
		if err != nil {
			t.Fatalf("error reading int32 %d: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %d, got %d", c, v)
		}
	}
}

func TestReadWriteInt64(t *testing.T) {
	cases := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 4294967296, -4294967296}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteInt64(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadInt64()
		if err != nil {
			t.Fatalf("error reading int64 %d: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %d, got %d", c, v)
		}
	}
}

func TestReadWriteFloat32(t *testing.T) {
	cases := []float32{0, 1.5, -1.5, math.MaxFloat32, math.SmallestNonzeroFloat32}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteFloat32(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadFloat32()
		if err != nil {
			t.Fatalf("error reading float32 %v: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %v, got %v", c, v)
		}
	}
}

func TestReadWriteFloat64(t *testing.T) {
	cases := []float64{0, 1.5, -1.5, math.MaxFloat64, math.SmallestNonzeroFloat64}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteFloat64(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadFloat64()
		if err != nil {
			t.Fatalf("error reading float64 %v: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %v, got %v", c, v)
		}
	}
}

func TestReadWriteInt3(t *testing.T) {
	cases := []int32{0, 1, -1, INT3MaxValue, INT3MinValue, 100000, -100000}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteInt3(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadInt3()
		if err != nil {
			t.Fatalf("error reading int3 %d: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %d, got %d", c, v)
		}
	}
}

func TestReadWriteLong5(t *testing.T) {
	cases := []int64{0, 1, -1, LONG5MaxValue, LONG5MinValue, 1000000000, -1000000000}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteLong5(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadLong5()
		if err != nil {
			t.Fatalf("error reading long5 %d: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %d, got %d", c, v)
		}
	}
}

func TestReadWriteDecimal(t *testing.T) {
	cases := []int64{
		0,                  // 0 bytes (code 0)
		1, -1, 127, -128,  // 1 byte
		128, -129, 32767, -32768, // 2 bytes
		32768, -32769, int64(INT3MaxValue), int64(INT3MinValue), // 3 bytes
		int64(INT3MaxValue) + 1, int64(INT3MinValue) - 1, 2147483647, -2147483648, // 4 bytes
		2147483648, -2147483649, LONG5MaxValue, LONG5MinValue, // 5 bytes
		LONG5MaxValue + 1, LONG5MinValue - 1, math.MaxInt64, math.MinInt64, // 8 bytes
	}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteDecimal(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadDecimal()
		if err != nil {
			t.Fatalf("error reading decimal %d: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %d, got %d", c, v)
		}
	}
}

func TestReadWriteBlob(t *testing.T) {
	// Empty blob
	o := NewDataOutputX()
	o.WriteBlob(nil)
	o.WriteBlob([]byte{})
	d := NewDataInputX(o.ToByteArray())
	b, _ := d.ReadBlob()
	if len(b) != 0 {
		t.Errorf("expected empty, got len %d", len(b))
	}
	b, _ = d.ReadBlob()
	if len(b) != 0 {
		t.Errorf("expected empty, got len %d", len(b))
	}

	// Short blob (len <= 253)
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i)
	}
	o = NewDataOutputX()
	o.WriteBlob(data)
	d = NewDataInputX(o.ToByteArray())
	b, _ = d.ReadBlob()
	if len(b) != 100 {
		t.Fatalf("expected 100, got %d", len(b))
	}
	for i := range b {
		if b[i] != byte(i) {
			t.Errorf("byte %d: expected %d, got %d", i, byte(i), b[i])
		}
	}

	// Medium blob (254 <= len <= 65535)
	data = make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	o = NewDataOutputX()
	o.WriteBlob(data)
	d = NewDataInputX(o.ToByteArray())
	b, _ = d.ReadBlob()
	if len(b) != 1000 {
		t.Fatalf("expected 1000, got %d", len(b))
	}
}

func TestReadWriteText(t *testing.T) {
	cases := []string{"", "hello", "한글", "emoji🎉", "a longer string with spaces and 123"}
	for _, c := range cases {
		o := NewDataOutputX()
		o.WriteText(c)
		d := NewDataInputX(o.ToByteArray())
		v, err := d.ReadText()
		if err != nil {
			t.Fatalf("error reading text %q: %v", c, err)
		}
		if v != c {
			t.Errorf("expected %q, got %q", c, v)
		}
	}
}

func TestReadWriteArrayInt(t *testing.T) {
	arr := []int32{1, -1, 0, 2147483647, -2147483648}
	o := NewDataOutputX()
	o.WriteArrayInt(arr)
	d := NewDataInputX(o.ToByteArray())
	v, err := d.ReadArrayInt()
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(v) != len(arr) {
		t.Fatalf("length mismatch: %d != %d", len(v), len(arr))
	}
	for i := range arr {
		if v[i] != arr[i] {
			t.Errorf("index %d: %d != %d", i, v[i], arr[i])
		}
	}
}

func TestReadWriteArrayLong(t *testing.T) {
	arr := []int64{1, -1, 0, math.MaxInt64, math.MinInt64}
	o := NewDataOutputX()
	o.WriteArrayLong(arr)
	d := NewDataInputX(o.ToByteArray())
	v, err := d.ReadArrayLong()
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(v) != len(arr) {
		t.Fatalf("length mismatch: %d != %d", len(v), len(arr))
	}
	for i := range arr {
		if v[i] != arr[i] {
			t.Errorf("index %d: %d != %d", i, v[i], arr[i])
		}
	}
}

func TestReadWriteDecimalArray(t *testing.T) {
	arr := []int64{0, 1, -1, 127, 32768, 2147483648, math.MaxInt64}
	o := NewDataOutputX()
	o.WriteDecimalArray(arr)
	d := NewDataInputX(o.ToByteArray())
	v, err := d.ReadDecimalArray()
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(v) != len(arr) {
		t.Fatalf("length mismatch: %d != %d", len(v), len(arr))
	}
	for i := range arr {
		if v[i] != arr[i] {
			t.Errorf("index %d: %d != %d", i, v[i], arr[i])
		}
	}
}

func TestAvailableAndOffset(t *testing.T) {
	o := NewDataOutputX()
	o.WriteInt32(42)
	o.WriteInt64(100)

	d := NewDataInputX(o.ToByteArray())
	if d.Available() != 12 {
		t.Errorf("expected 12, got %d", d.Available())
	}
	if d.Offset() != 0 {
		t.Errorf("expected 0, got %d", d.Offset())
	}

	d.ReadInt32()
	if d.Available() != 8 {
		t.Errorf("expected 8, got %d", d.Available())
	}
	if d.Offset() != 4 {
		t.Errorf("expected 4, got %d", d.Offset())
	}
}

func TestEOFErrors(t *testing.T) {
	d := NewDataInputX([]byte{})
	_, err := d.ReadByte()
	if err != ErrEOF {
		t.Errorf("expected ErrEOF, got %v", err)
	}

	d = NewDataInputX([]byte{0x00})
	_, err = d.ReadInt16()
	if err != ErrEOF {
		t.Errorf("expected ErrEOF, got %v", err)
	}
}

func TestSkipBytes(t *testing.T) {
	o := NewDataOutputX()
	o.WriteInt32(1)
	o.WriteInt32(2)
	o.WriteInt32(3)

	d := NewDataInputX(o.ToByteArray())
	d.SkipBytes(4)
	v, _ := d.ReadInt32()
	if v != 2 {
		t.Errorf("expected 2, got %d", v)
	}
}

func TestDataOutputXSize(t *testing.T) {
	o := NewDataOutputX()
	if o.Size() != 0 {
		t.Errorf("expected 0, got %d", o.Size())
	}
	o.WriteInt32(1)
	if o.Size() != 4 {
		t.Errorf("expected 4, got %d", o.Size())
	}
	o.WriteInt64(1)
	if o.Size() != 12 {
		t.Errorf("expected 12, got %d", o.Size())
	}
}
