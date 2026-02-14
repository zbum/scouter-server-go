package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type NullValue struct{}

func (v *NullValue) ValueType() byte {
	return TYPE_NULL
}

func (v *NullValue) Write(o *protocol.DataOutputX) {
	// No-op: NullValue has no body
}

func (v *NullValue) Read(d *protocol.DataInputX) error {
	// No-op: NullValue has no body
	return nil
}
