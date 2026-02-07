package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// TextPack represents text data with hash.
type TextPack struct {
	XType string
	Hash  int32
	Text  string
}

// GetPackType returns the pack type code.
func (p *TextPack) GetPackType() byte {
	return PackTypeText
}

// Write serializes the TextPack to the output stream.
func (p *TextPack) Write(o *protocol.DataOutputX) {
	o.WriteText(p.XType)
	o.WriteInt(p.Hash)
	o.WriteText(p.Text)
}

// Read deserializes the TextPack from the input stream.
func (p *TextPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.XType, err = d.ReadText(); err != nil {
		return err
	}
	if p.Hash, err = d.ReadInt(); err != nil {
		return err
	}
	if p.Text, err = d.ReadText(); err != nil {
		return err
	}
	return nil
}
