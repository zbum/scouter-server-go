package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// StackPack represents stack trace data.
type StackPack struct {
	Time    int64
	ObjHash int32
	Data    []byte
}

// PackType returns the pack type code.
func (p *StackPack) PackType() byte {
	return PackTypeStack
}

// Write serializes the StackPack to the output stream.
func (p *StackPack) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(p.Time)
	o.WriteDecimal(int64(p.ObjHash))
	o.WriteBlob(p.Data)
}

// Read deserializes the StackPack from the input stream.
func (p *StackPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadDecimal(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ObjHash = int32(val)
	}
	if p.Data, err = d.ReadBlob(); err != nil {
		return err
	}
	return nil
}
