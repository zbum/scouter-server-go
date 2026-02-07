package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type DoubleValue struct {
	Value float64
}

func (v *DoubleValue) GetValueType() byte {
	return TYPE_DOUBLE
}

func (v *DoubleValue) Write(o *protocol.DataOutputX) {
	o.WriteDouble(v.Value)
}

func (v *DoubleValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadDouble()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
