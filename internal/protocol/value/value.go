package value

import (
	"fmt"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

// Value type codes (must match Java ValueEnum exactly)
const (
	TYPE_NULL           byte = 0
	TYPE_BOOLEAN        byte = 10
	TYPE_DECIMAL        byte = 20
	TYPE_FLOAT          byte = 30
	TYPE_DOUBLE         byte = 40
	TYPE_DOUBLE_SUMMARY byte = 45
	TYPE_LONG_SUMMARY   byte = 46
	TYPE_TEXT           byte = 50
	TYPE_TEXT_HASH      byte = 51
	TYPE_BLOB           byte = 60
	TYPE_IP4ADDR        byte = 61
	TYPE_LIST           byte = 70
	TYPE_ARRAY_INT      byte = 71
	TYPE_ARRAY_FLOAT    byte = 72
	TYPE_ARRAY_TEXT     byte = 73
	TYPE_ARRAY_LONG     byte = 74
	TYPE_MAP            byte = 80
)

type Value interface {
	ValueType() byte
	Write(o *protocol.DataOutputX)
	Read(d *protocol.DataInputX) error
}

func CreateValue(typeCode byte) (Value, error) {
	switch typeCode {
	case TYPE_NULL:
		return &NullValue{}, nil
	case TYPE_BOOLEAN:
		return &BooleanValue{}, nil
	case TYPE_DECIMAL:
		return &DecimalValue{}, nil
	case TYPE_FLOAT:
		return &FloatValue{}, nil
	case TYPE_DOUBLE:
		return &DoubleValue{}, nil
	case TYPE_DOUBLE_SUMMARY:
		return &DoubleSummary{}, nil
	case TYPE_LONG_SUMMARY:
		return &LongSummary{}, nil
	case TYPE_TEXT:
		return &TextValue{}, nil
	case TYPE_TEXT_HASH:
		return &TextHashValue{}, nil
	case TYPE_BLOB:
		return &BlobValue{}, nil
	case TYPE_IP4ADDR:
		return &IP4Value{}, nil
	case TYPE_LIST:
		return &ListValue{}, nil
	case TYPE_ARRAY_INT:
		return &IntArray{}, nil
	case TYPE_ARRAY_FLOAT:
		return &FloatArray{}, nil
	case TYPE_ARRAY_TEXT:
		return &TextArray{}, nil
	case TYPE_ARRAY_LONG:
		return &LongArray{}, nil
	case TYPE_MAP:
		return &MapValue{}, nil
	default:
		return nil, fmt.Errorf("unknown value type code: %d", typeCode)
	}
}

func WriteValue(o *protocol.DataOutputX, v Value) {
	if v == nil {
		v = &NullValue{}
	}
	o.WriteByte(v.ValueType())
	v.Write(o)
}

func ReadValue(d *protocol.DataInputX) (Value, error) {
	typeByte, err := d.ReadByte()
	if err != nil {
		return nil, err
	}
	v, err := CreateValue(typeByte)
	if err != nil {
		return nil, err
	}
	err = v.Read(d)
	return v, err
}
