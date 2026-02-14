package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type ListValue struct {
	Value []Value
}

func NewListValue() *ListValue {
	return &ListValue{Value: make([]Value, 0)}
}

func (v *ListValue) ValueType() byte {
	return TYPE_LIST
}

func (v *ListValue) Write(o *protocol.DataOutputX) {
	if v.Value == nil {
		o.WriteDecimal(0)
	} else {
		o.WriteDecimal(int64(len(v.Value)))
		for _, element := range v.Value {
			WriteValue(o, element)
		}
	}
}

// GetInt returns the int32 value at the given index.
func (v *ListValue) GetInt(idx int) int32 {
	if idx < 0 || idx >= len(v.Value) {
		return 0
	}
	if dv, ok := v.Value[idx].(*DecimalValue); ok {
		return int32(dv.Value)
	}
	return 0
}

// GetString returns the string value at the given index.
func (v *ListValue) GetString(idx int) string {
	if idx < 0 || idx >= len(v.Value) {
		return ""
	}
	if tv, ok := v.Value[idx].(*TextValue); ok {
		return tv.Value
	}
	return ""
}

// GetLong returns the int64 value at the given index.
func (v *ListValue) GetLong(idx int) int64 {
	if idx < 0 || idx >= len(v.Value) {
		return 0
	}
	if dv, ok := v.Value[idx].(*DecimalValue); ok {
		return dv.Value
	}
	return 0
}

func (v *ListValue) Read(d *protocol.DataInputX) error {
	count, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	v.Value = make([]Value, count)
	for i := int64(0); i < count; i++ {
		element, err := ReadValue(d)
		if err != nil {
			return err
		}
		v.Value[i] = element
	}
	return nil
}
