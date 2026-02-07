package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// PerfCounterPack represents performance counter data.
type PerfCounterPack struct {
	Time     int64
	ObjName  string
	TimeType byte
	Data     *value.MapValue
}

// GetPackType returns the pack type code.
func (p *PerfCounterPack) GetPackType() byte {
	return PackTypePerfCounter
}

// Write serializes the PerfCounterPack to the output stream.
func (p *PerfCounterPack) Write(o *protocol.DataOutputX) {
	o.WriteLong(p.Time)
	o.WriteText(p.ObjName)
	o.WriteByte(p.TimeType)
	value.WriteValue(o, p.Data)
}

// Read deserializes the PerfCounterPack from the input stream.
func (p *PerfCounterPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadLong(); err != nil {
		return err
	}
	if p.ObjName, err = d.ReadText(); err != nil {
		return err
	}
	if p.TimeType, err = d.ReadByte(); err != nil {
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
