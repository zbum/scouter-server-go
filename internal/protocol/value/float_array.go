package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type FloatArray struct {
	Value []float32
}

func (v *FloatArray) GetValueType() byte {
	return TYPE_ARRAY_FLOAT
}

func (v *FloatArray) Write(o *protocol.DataOutputX) {
	o.WriteArrayFloat(v.Value)
}

func (v *FloatArray) Read(d *protocol.DataInputX) error {
	val, err := d.ReadArrayFloat()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
