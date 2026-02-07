package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// DroppedXLogPack represents a dropped transaction log.
type DroppedXLogPack struct {
	Gxid int64
	Txid int64
}

// GetPackType returns the pack type code.
func (p *DroppedXLogPack) GetPackType() byte {
	return PackTypeDroppedXLog
}

// Write serializes the DroppedXLogPack to the output stream.
func (p *DroppedXLogPack) Write(o *protocol.DataOutputX) {
	o.WriteLong(p.Gxid)
	o.WriteLong(p.Txid)
}

// Read deserializes the DroppedXLogPack from the input stream.
func (p *DroppedXLogPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Gxid, err = d.ReadLong(); err != nil {
		return err
	}
	if p.Txid, err = d.ReadLong(); err != nil {
		return err
	}
	return nil
}
