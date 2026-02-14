package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// SummaryPack represents summary data.
type SummaryPack struct {
	Time    int64
	ObjHash int32
	ObjType string
	SType   byte
	Table   *value.MapValue
}

// PackType returns the pack type code.
func (p *SummaryPack) PackType() byte {
	return PackTypeSummary
}

// Write serializes the SummaryPack to the output stream.
func (p *SummaryPack) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(p.Time)
	o.WriteInt32(p.ObjHash)
	o.WriteText(p.ObjType)
	o.WriteByte(p.SType)
	value.WriteValue(o, p.Table)
}

// Read deserializes the SummaryPack from the input stream.
func (p *SummaryPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadDecimal(); err != nil {
		return err
	}
	if p.ObjHash, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.ObjType, err = d.ReadText(); err != nil {
		return err
	}
	if p.SType, err = d.ReadByte(); err != nil {
		return err
	}

	val, err := value.ReadValue(d)
	if err != nil {
		return err
	}

	var ok bool
	if p.Table, ok = val.(*value.MapValue); !ok && val != nil {
		// If not nil but wrong type, create empty MapValue
		p.Table = value.NewMapValue()
	}

	return nil
}
