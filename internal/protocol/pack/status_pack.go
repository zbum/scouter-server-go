package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// StatusPack represents status data.
type StatusPack struct {
	Time    int64
	ObjType string
	ObjHash int32
	Key     string
	Data    *value.MapValue
}

// PackType returns the pack type code.
func (p *StatusPack) PackType() byte {
	return PackTypePerfStatus
}

// Write serializes the StatusPack to the output stream.
func (p *StatusPack) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(p.Time)
	o.WriteText(p.ObjType)
	o.WriteDecimal(int64(p.ObjHash))
	o.WriteText(p.Key)
	value.WriteValue(o, p.Data)
}

// Read deserializes the StatusPack from the input stream.
func (p *StatusPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadDecimal(); err != nil {
		return err
	}
	if p.ObjType, err = d.ReadText(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ObjHash = int32(val)
	}
	if p.Key, err = d.ReadText(); err != nil {
		return err
	}

	val, err := value.ReadValue(d)
	if err != nil {
		return err
	}

	var ok bool
	if p.Data, ok = val.(*value.MapValue); !ok && val != nil {
		// If not nil but wrong type, create empty MapValue
		p.Data = value.NewMapValue()
	}

	return nil
}
