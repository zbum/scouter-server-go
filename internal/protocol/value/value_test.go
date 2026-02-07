package value

import (
	"testing"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

func roundTrip(t *testing.T, v Value) Value {
	t.Helper()
	o := protocol.NewDataOutputX()
	WriteValue(o, v)
	d := protocol.NewDataInputX(o.ToByteArray())
	result, err := ReadValue(d)
	if err != nil {
		t.Fatalf("ReadValue error: %v", err)
	}
	return result
}

func TestNullValue(t *testing.T) {
	o := protocol.NewDataOutputX()
	WriteValue(o, nil)
	d := protocol.NewDataInputX(o.ToByteArray())
	v, err := ReadValue(d)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if v.GetValueType() != TYPE_NULL {
		t.Errorf("expected NULL type, got %d", v.GetValueType())
	}
}

func TestBooleanValue(t *testing.T) {
	for _, b := range []bool{true, false} {
		v := &BooleanValue{Value: b}
		result := roundTrip(t, v)
		rv, ok := result.(*BooleanValue)
		if !ok {
			t.Fatalf("expected BooleanValue, got %T", result)
		}
		if rv.Value != b {
			t.Errorf("expected %v, got %v", b, rv.Value)
		}
	}
}

func TestDecimalValue(t *testing.T) {
	cases := []int64{0, 1, -1, 127, -128, 32767, -32768, 2147483647, -2147483648, 9223372036854775807}
	for _, c := range cases {
		v := NewDecimalValue(c)
		result := roundTrip(t, v)
		rv, ok := result.(*DecimalValue)
		if !ok {
			t.Fatalf("expected DecimalValue, got %T", result)
		}
		if rv.Value != c {
			t.Errorf("expected %d, got %d", c, rv.Value)
		}
	}
}

func TestFloatValue(t *testing.T) {
	cases := []float32{0, 1.5, -1.5, 3.14}
	for _, c := range cases {
		v := &FloatValue{Value: c}
		result := roundTrip(t, v)
		rv, ok := result.(*FloatValue)
		if !ok {
			t.Fatalf("expected FloatValue, got %T", result)
		}
		if rv.Value != c {
			t.Errorf("expected %v, got %v", c, rv.Value)
		}
	}
}

func TestDoubleValue(t *testing.T) {
	cases := []float64{0, 1.5, -1.5, 3.14159265358979}
	for _, c := range cases {
		v := &DoubleValue{Value: c}
		result := roundTrip(t, v)
		rv, ok := result.(*DoubleValue)
		if !ok {
			t.Fatalf("expected DoubleValue, got %T", result)
		}
		if rv.Value != c {
			t.Errorf("expected %v, got %v", c, rv.Value)
		}
	}
}

func TestTextValue(t *testing.T) {
	cases := []string{"", "hello", "한글", "emoji🎉"}
	for _, c := range cases {
		v := NewTextValue(c)
		result := roundTrip(t, v)
		rv, ok := result.(*TextValue)
		if !ok {
			t.Fatalf("expected TextValue, got %T", result)
		}
		if rv.Value != c {
			t.Errorf("expected %q, got %q", c, rv.Value)
		}
	}
}

func TestTextHashValue(t *testing.T) {
	v := &TextHashValue{Value: 12345}
	result := roundTrip(t, v)
	rv, ok := result.(*TextHashValue)
	if !ok {
		t.Fatalf("expected TextHashValue, got %T", result)
	}
	if rv.Value != 12345 {
		t.Errorf("expected 12345, got %d", rv.Value)
	}
}

func TestBlobValue(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	v := &BlobValue{Value: data}
	result := roundTrip(t, v)
	rv, ok := result.(*BlobValue)
	if !ok {
		t.Fatalf("expected BlobValue, got %T", result)
	}
	if len(rv.Value) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(rv.Value))
	}
	for i := range data {
		if rv.Value[i] != data[i] {
			t.Errorf("byte %d: expected %d, got %d", i, data[i], rv.Value[i])
		}
	}
}

func TestIP4Value(t *testing.T) {
	v := &IP4Value{Value: [4]byte{10, 0, 0, 1}}
	result := roundTrip(t, v)
	rv, ok := result.(*IP4Value)
	if !ok {
		t.Fatalf("expected IP4Value, got %T", result)
	}
	if rv.Value != [4]byte{10, 0, 0, 1} {
		t.Errorf("expected [10,0,0,1], got %v", rv.Value)
	}
}

func TestListValue(t *testing.T) {
	list := NewListValue()
	list.Value = append(list.Value, NewTextValue("hello"))
	list.Value = append(list.Value, NewDecimalValue(42))

	result := roundTrip(t, list)
	rv, ok := result.(*ListValue)
	if !ok {
		t.Fatalf("expected ListValue, got %T", result)
	}
	if len(rv.Value) != 2 {
		t.Fatalf("expected 2 items, got %d", len(rv.Value))
	}
	tv, ok := rv.Value[0].(*TextValue)
	if !ok {
		t.Fatalf("expected TextValue, got %T", rv.Value[0])
	}
	if tv.Value != "hello" {
		t.Errorf("expected 'hello', got %q", tv.Value)
	}
}

func TestMapValue(t *testing.T) {
	m := NewMapValue()
	m.Put("key1", NewTextValue("value1"))
	m.Put("key2", NewDecimalValue(100))

	result := roundTrip(t, m)
	rv, ok := result.(*MapValue)
	if !ok {
		t.Fatalf("expected MapValue, got %T", result)
	}

	v1, found := rv.Get("key1")
	if !found {
		t.Fatal("key1 not found")
	}
	tv, ok := v1.(*TextValue)
	if !ok {
		t.Fatalf("expected TextValue, got %T", v1)
	}
	if tv.Value != "value1" {
		t.Errorf("expected 'value1', got %q", tv.Value)
	}

	v2, found := rv.Get("key2")
	if !found {
		t.Fatal("key2 not found")
	}
	dv, ok := v2.(*DecimalValue)
	if !ok {
		t.Fatalf("expected DecimalValue, got %T", v2)
	}
	if dv.Value != 100 {
		t.Errorf("expected 100, got %d", dv.Value)
	}
}

func TestIntArray(t *testing.T) {
	data := []int32{1, -1, 0, 2147483647}
	v := &IntArray{Value: data}
	result := roundTrip(t, v)
	rv, ok := result.(*IntArray)
	if !ok {
		t.Fatalf("expected IntArray, got %T", result)
	}
	if len(rv.Value) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(rv.Value))
	}
	for i := range data {
		if rv.Value[i] != data[i] {
			t.Errorf("index %d: expected %d, got %d", i, data[i], rv.Value[i])
		}
	}
}

func TestFloatArray(t *testing.T) {
	data := []float32{1.5, -1.5, 0, 3.14}
	v := &FloatArray{Value: data}
	result := roundTrip(t, v)
	rv, ok := result.(*FloatArray)
	if !ok {
		t.Fatalf("expected FloatArray, got %T", result)
	}
	if len(rv.Value) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(rv.Value))
	}
	for i := range data {
		if rv.Value[i] != data[i] {
			t.Errorf("index %d: expected %v, got %v", i, data[i], rv.Value[i])
		}
	}
}

func TestLongArray(t *testing.T) {
	data := []int64{1, -1, 0, 9223372036854775807}
	v := &LongArray{Value: data}
	result := roundTrip(t, v)
	rv, ok := result.(*LongArray)
	if !ok {
		t.Fatalf("expected LongArray, got %T", result)
	}
	if len(rv.Value) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(rv.Value))
	}
	for i := range data {
		if rv.Value[i] != data[i] {
			t.Errorf("index %d: expected %d, got %d", i, data[i], rv.Value[i])
		}
	}
}

func TestTextArray(t *testing.T) {
	data := []string{"hello", "world", "한글"}
	v := &TextArray{Value: data}
	result := roundTrip(t, v)
	rv, ok := result.(*TextArray)
	if !ok {
		t.Fatalf("expected TextArray, got %T", result)
	}
	if len(rv.Value) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(rv.Value))
	}
	for i := range data {
		if rv.Value[i] != data[i] {
			t.Errorf("index %d: expected %q, got %q", i, data[i], rv.Value[i])
		}
	}
}

func TestDoubleSummary(t *testing.T) {
	v := &DoubleSummary{Count: 10, Sum: 100.5, Min: 1.0, Max: 20.0}
	result := roundTrip(t, v)
	rv, ok := result.(*DoubleSummary)
	if !ok {
		t.Fatalf("expected DoubleSummary, got %T", result)
	}
	if rv.Count != 10 {
		t.Errorf("Count: expected 10, got %d", rv.Count)
	}
	if rv.Sum != 100.5 {
		t.Errorf("Sum: expected 100.5, got %v", rv.Sum)
	}
	if rv.Min != 1.0 {
		t.Errorf("Min: expected 1.0, got %v", rv.Min)
	}
	if rv.Max != 20.0 {
		t.Errorf("Max: expected 20.0, got %v", rv.Max)
	}
}

func TestLongSummary(t *testing.T) {
	v := &LongSummary{Count: 5, Sum: 500, Min: 10, Max: 200}
	result := roundTrip(t, v)
	rv, ok := result.(*LongSummary)
	if !ok {
		t.Fatalf("expected LongSummary, got %T", result)
	}
	if rv.Count != 5 {
		t.Errorf("Count: expected 5, got %d", rv.Count)
	}
	if rv.Sum != 500 {
		t.Errorf("Sum: expected 500, got %d", rv.Sum)
	}
}

func TestAllValueTypes(t *testing.T) {
	types := []byte{
		TYPE_NULL, TYPE_BOOLEAN, TYPE_DECIMAL, TYPE_FLOAT, TYPE_DOUBLE,
		TYPE_DOUBLE_SUMMARY, TYPE_LONG_SUMMARY, TYPE_TEXT, TYPE_TEXT_HASH,
		TYPE_BLOB, TYPE_IP4ADDR, TYPE_LIST, TYPE_ARRAY_INT, TYPE_ARRAY_FLOAT,
		TYPE_ARRAY_TEXT, TYPE_ARRAY_LONG, TYPE_MAP,
	}
	for _, tc := range types {
		v, err := CreateValue(tc)
		if err != nil {
			t.Errorf("CreateValue(%d) error: %v", tc, err)
			continue
		}
		if v.GetValueType() != tc {
			t.Errorf("type mismatch: expected %d, got %d", tc, v.GetValueType())
		}
	}
}
