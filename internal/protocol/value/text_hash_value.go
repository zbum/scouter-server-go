package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type TextHashValue struct {
	Value int32
}

func (v *TextHashValue) GetValueType() byte {
	return TYPE_TEXT_HASH
}

func (v *TextHashValue) Write(o *protocol.DataOutputX) {
	o.WriteInt(v.Value)
}

func (v *TextHashValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadInt()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
