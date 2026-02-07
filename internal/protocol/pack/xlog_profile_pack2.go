package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// XLogProfilePack2 extends XLogProfilePack with additional fields.
type XLogProfilePack2 struct {
	XLogProfilePack
	Gxid                           int64
	XType                          byte
	DiscardType                    byte
	IgnoreGlobalConsequentSampling bool
}

// GetPackType returns the pack type code.
func (p *XLogProfilePack2) GetPackType() byte {
	return PackTypeXLogProfile2
}

// Write serializes the XLogProfilePack2 to the output stream.
func (p *XLogProfilePack2) Write(o *protocol.DataOutputX) {
	// Write base XLogProfilePack fields
	p.XLogProfilePack.Write(o)

	// Write additional fields
	o.WriteLong(p.Gxid)
	o.WriteByte(p.XType)
	o.WriteByte(p.DiscardType)
	o.WriteBoolean(p.IgnoreGlobalConsequentSampling)
}

// Read deserializes the XLogProfilePack2 from the input stream.
func (p *XLogProfilePack2) Read(d *protocol.DataInputX) error {
	// Read base XLogProfilePack fields
	if err := p.XLogProfilePack.Read(d); err != nil {
		return err
	}

	// Read additional fields
	var err error
	if p.Gxid, err = d.ReadLong(); err != nil {
		return err
	}
	if p.XType, err = d.ReadByte(); err != nil {
		return err
	}
	if p.DiscardType, err = d.ReadByte(); err != nil {
		return err
	}
	if p.IgnoreGlobalConsequentSampling, err = d.ReadBoolean(); err != nil {
		return err
	}

	return nil
}
