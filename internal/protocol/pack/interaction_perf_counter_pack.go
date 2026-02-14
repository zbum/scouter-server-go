package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// InteractionPerfCounterPack represents interaction performance counter data.
type InteractionPerfCounterPack struct {
	Time            int64
	ObjName         string
	InteractionType string
	FromHash        int32
	ToHash          int32
	Period          int32
	Count           int32
	ErrorCount      int32
	TotalElapsed    int64
	CustomData      *value.MapValue
}

// PackType returns the pack type code.
func (p *InteractionPerfCounterPack) PackType() byte {
	return PackTypePerfInteractionCounter
}

// Write serializes the InteractionPerfCounterPack to the output stream.
func (p *InteractionPerfCounterPack) Write(o *protocol.DataOutputX) {
	o.WriteInt64(p.Time)
	o.WriteText(p.ObjName)
	o.WriteText(p.InteractionType)
	o.WriteInt32(p.FromHash)
	o.WriteInt32(p.ToHash)
	o.WriteInt32(p.Period)
	o.WriteInt32(p.Count)
	o.WriteInt32(p.ErrorCount)
	o.WriteInt64(p.TotalElapsed)
	value.WriteValue(o, p.CustomData)
}

// Read deserializes the InteractionPerfCounterPack from the input stream.
func (p *InteractionPerfCounterPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadInt64(); err != nil {
		return err
	}
	if p.ObjName, err = d.ReadText(); err != nil {
		return err
	}
	if p.InteractionType, err = d.ReadText(); err != nil {
		return err
	}
	if p.FromHash, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.ToHash, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.Period, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.Count, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.ErrorCount, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.TotalElapsed, err = d.ReadInt64(); err != nil {
		return err
	}

	val, err := value.ReadValue(d)
	if err != nil {
		return err
	}

	var ok bool
	if p.CustomData, ok = val.(*value.MapValue); !ok && val != nil {
		// If not nil but wrong type, create empty MapValue
		p.CustomData = value.NewMapValue()
	}

	return nil
}
