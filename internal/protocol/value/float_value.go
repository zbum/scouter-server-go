package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type FloatValue struct {
	Value float32
}

func (v *FloatValue) ValueType() byte {
	return TYPE_FLOAT
}

func (v *FloatValue) Write(o *protocol.DataOutputX) {
	o.WriteFloat32(v.Value)
}

func (v *FloatValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadFloat32()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
