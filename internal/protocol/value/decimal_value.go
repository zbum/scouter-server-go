package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type DecimalValue struct {
	Value int64
}

func NewDecimalValue(val int64) *DecimalValue {
	return &DecimalValue{Value: val}
}

func (v *DecimalValue) ValueType() byte {
	return TYPE_DECIMAL
}

func (v *DecimalValue) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(v.Value)
}

func (v *DecimalValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
