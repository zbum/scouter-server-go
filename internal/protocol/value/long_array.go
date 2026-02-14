package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type LongArray struct {
	Value []int64
}

func (v *LongArray) ValueType() byte {
	return TYPE_ARRAY_LONG
}

func (v *LongArray) Write(o *protocol.DataOutputX) {
	o.WriteArrayLong(v.Value)
}

func (v *LongArray) Read(d *protocol.DataInputX) error {
	val, err := d.ReadArrayLong()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
