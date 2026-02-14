package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type TextArray struct {
	Value []string
}

func (v *TextArray) ValueType() byte {
	return TYPE_ARRAY_TEXT
}

func (v *TextArray) Write(o *protocol.DataOutputX) {
	if v.Value == nil {
		o.WriteInt16(0)
	} else {
		o.WriteInt16(int16(len(v.Value)))
		for _, text := range v.Value {
			o.WriteText(text)
		}
	}
}

func (v *TextArray) Read(d *protocol.DataInputX) error {
	length, err := d.ReadInt16()
	if err != nil {
		return err
	}
	v.Value = make([]string, length)
	for i := int16(0); i < length; i++ {
		text, err := d.ReadText()
		if err != nil {
			return err
		}
		v.Value[i] = text
	}
	return nil
}
