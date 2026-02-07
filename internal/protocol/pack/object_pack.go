package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// ObjectPack represents object/agent data.
type ObjectPack struct {
	ObjType string
	ObjHash int32
	ObjName string
	Address string
	Version string
	Alive   bool
	Wakeup  int64
	Tags    *value.MapValue
}

// GetPackType returns the pack type code.
func (p *ObjectPack) GetPackType() byte {
	return PackTypeObject
}

// Write serializes the ObjectPack to the output stream.
func (p *ObjectPack) Write(o *protocol.DataOutputX) {
	o.WriteText(p.ObjType)
	o.WriteDecimal(int64(p.ObjHash))
	o.WriteText(p.ObjName)
	o.WriteText(p.Address)
	o.WriteText(p.Version)
	o.WriteBoolean(p.Alive)
	o.WriteDecimal(p.Wakeup)
	value.WriteValue(o, p.Tags)
}

// Read deserializes the ObjectPack from the input stream.
func (p *ObjectPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.ObjType, err = d.ReadText(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ObjHash = int32(val)
	}
	if p.ObjName, err = d.ReadText(); err != nil {
		return err
	}
	if p.Address, err = d.ReadText(); err != nil {
		return err
	}
	if p.Version, err = d.ReadText(); err != nil {
		return err
	}
	if p.Alive, err = d.ReadBoolean(); err != nil {
		return err
	}
	if p.Wakeup, err = d.ReadDecimal(); err != nil {
		return err
	}

	val, err := value.ReadValue(d)
	if err != nil {
		return err
	}

	var ok bool
	if p.Tags, ok = val.(*value.MapValue); !ok && val != nil {
		// If not nil but wrong type, create empty MapValue
		p.Tags = value.NewMapValue()
	}

	return nil
}
