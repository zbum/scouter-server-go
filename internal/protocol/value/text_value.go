package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type TextValue struct {
	Value string
}

func NewTextValue(val string) *TextValue {
	return &TextValue{Value: val}
}

func (v *TextValue) ValueType() byte {
	return TYPE_TEXT
}

func (v *TextValue) Write(o *protocol.DataOutputX) {
	o.WriteText(v.Value)
}

func (v *TextValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadText()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
