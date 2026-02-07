package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type FloatValue struct {
	Value float32
}

func (v *FloatValue) GetValueType() byte {
	return TYPE_FLOAT
}

func (v *FloatValue) Write(o *protocol.DataOutputX) {
	o.WriteFloat(v.Value)
}

func (v *FloatValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadFloat()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
