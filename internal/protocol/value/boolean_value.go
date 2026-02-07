package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type BooleanValue struct {
	Value bool
}

func (v *BooleanValue) GetValueType() byte {
	return TYPE_BOOLEAN
}

func (v *BooleanValue) Write(o *protocol.DataOutputX) {
	o.WriteBoolean(v.Value)
}

func (v *BooleanValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadBoolean()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
