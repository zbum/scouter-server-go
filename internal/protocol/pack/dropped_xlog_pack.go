package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// DroppedXLogPack represents a dropped transaction log.
type DroppedXLogPack struct {
	Gxid int64
	Txid int64
}

// PackType returns the pack type code.
func (p *DroppedXLogPack) PackType() byte {
	return PackTypeDroppedXLog
}

// Write serializes the DroppedXLogPack to the output stream.
func (p *DroppedXLogPack) Write(o *protocol.DataOutputX) {
	o.WriteInt64(p.Gxid)
	o.WriteInt64(p.Txid)
}

// Read deserializes the DroppedXLogPack from the input stream.
func (p *DroppedXLogPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Gxid, err = d.ReadInt64(); err != nil {
		return err
	}
	if p.Txid, err = d.ReadInt64(); err != nil {
		return err
	}
	return nil
}
