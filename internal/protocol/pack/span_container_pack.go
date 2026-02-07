package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// SpanContainerPack represents a container for multiple spans.
type SpanContainerPack struct {
	Gxid      int64
	SpanCount int32
	Timestamp int64
	Spans     []byte
}

// GetPackType returns the pack type code.
func (p *SpanContainerPack) GetPackType() byte {
	return PackTypeSpanContainer
}

// Write serializes the SpanContainerPack to the output stream.
func (p *SpanContainerPack) Write(o *protocol.DataOutputX) {
	o.WriteLong(p.Gxid)
	o.WriteDecimal(int64(p.SpanCount))
	o.WriteLong(p.Timestamp)
	o.WriteBlob(p.Spans)
}

// Read deserializes the SpanContainerPack from the input stream.
func (p *SpanContainerPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Gxid, err = d.ReadLong(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.SpanCount = int32(val)
	}
	if p.Timestamp, err = d.ReadLong(); err != nil {
		return err
	}
	if p.Spans, err = d.ReadBlob(); err != nil {
		return err
	}
	return nil
}
