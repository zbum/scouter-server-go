package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// AlertPack represents alert data.
type AlertPack struct {
	Time    int64
	Level   byte
	ObjType string
	ObjHash int32
	Title   string
	Message string
	Tags    *value.MapValue
}

// PackType returns the pack type code.
func (p *AlertPack) PackType() byte {
	return PackTypeAlert
}

// Write serializes the AlertPack to the output stream.
func (p *AlertPack) Write(o *protocol.DataOutputX) {
	o.WriteInt64(p.Time)
	o.WriteByte(p.Level)
	o.WriteText(p.ObjType)
	o.WriteInt32(p.ObjHash)
	o.WriteText(p.Title)
	o.WriteText(p.Message)
	// Java initializes tags = new MapValue(), so always write a MapValue (never NullValue).
	tags := p.Tags
	if tags == nil {
		tags = value.NewMapValue()
	}
	value.WriteValue(o, tags)
}

// Read deserializes the AlertPack from the input stream.
func (p *AlertPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Time, err = d.ReadInt64(); err != nil {
		return err
	}
	if p.Level, err = d.ReadByte(); err != nil {
		return err
	}
	if p.ObjType, err = d.ReadText(); err != nil {
		return err
	}
	if p.ObjHash, err = d.ReadInt32(); err != nil {
		return err
	}
	if p.Title, err = d.ReadText(); err != nil {
		return err
	}
	if p.Message, err = d.ReadText(); err != nil {
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
