package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// XLogProfilePack represents transaction profile data.
type XLogProfilePack struct {
	Time    int64
	ObjHash int32
	Service int32
	Txid    int64
	Profile []byte
}

// GetPackType returns the pack type code.
func (p *XLogProfilePack) GetPackType() byte {
	return PackTypeXLogProfile
}

// Write serializes the XLogProfilePack to the output stream.
func (p *XLogProfilePack) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(p.Time)
	o.WriteDecimal(int64(p.ObjHash))
	o.WriteDecimal(int64(p.Service))
	o.WriteLong(p.Txid)
	o.WriteBlob(p.Profile)
}

// Read deserializes the XLogProfilePack from the input stream.
func (p *XLogProfilePack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadDecimal(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ObjHash = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Service = int32(val)
	}
	if p.Txid, err = d.ReadLong(); err != nil {
		return err
	}
	if p.Profile, err = d.ReadBlob(); err != nil {
		return err
	}
	return nil
}
