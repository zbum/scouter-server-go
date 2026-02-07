package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type IP4Value struct {
	Value [4]byte
}

func (v *IP4Value) GetValueType() byte {
	return TYPE_IP4ADDR
}

func (v *IP4Value) Write(o *protocol.DataOutputX) {
	o.Write(v.Value[:])
}

func (v *IP4Value) Read(d *protocol.DataInputX) error {
	bytes, err := d.Read(4)
	if err != nil {
		return err
	}
	copy(v.Value[:], bytes)
	return nil
}
