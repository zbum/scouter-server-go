package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type IntArray struct {
	Value []int32
}

func (v *IntArray) ValueType() byte {
	return TYPE_ARRAY_INT
}

func (v *IntArray) Write(o *protocol.DataOutputX) {
	o.WriteArrayInt(v.Value)
}

func (v *IntArray) Read(d *protocol.DataInputX) error {
	val, err := d.ReadArrayInt()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
