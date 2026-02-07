package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// SpanPack represents distributed tracing span data.
type SpanPack struct {
	Gxid                       int64
	Txid                       int64
	Caller                     int64
	Timestamp                  int64
	Elapsed                    int32
	SpanType                   byte
	Name                       int32
	ObjHash                    int32
	Error                      int32
	LocalEndpointServiceName   int32
	LocalEndpointIp            []byte
	LocalEndpointPort          int16
	RemoteEndpointServiceName  int32
	RemoteEndpointIp           []byte
	RemoteEndpointPort         int16
	Debug                      bool
	Shared                     bool
	AnnotationTimestamps       *value.ListValue
	AnnotationValues           *value.ListValue
	Tags                       *value.MapValue
}

// GetPackType returns the pack type code.
func (p *SpanPack) GetPackType() byte {
	return PackTypeSpan
}

// Write serializes the SpanPack to the output stream.
func (p *SpanPack) Write(o *protocol.DataOutputX) {
	o.WriteLong(p.Gxid)
	o.WriteLong(p.Txid)
	o.WriteLong(p.Caller)
	o.WriteLong(p.Timestamp)
	o.WriteDecimal(int64(p.Elapsed))
	o.WriteByte(p.SpanType)
	o.WriteDecimal(int64(p.Name))
	o.WriteDecimal(int64(p.ObjHash))
	o.WriteDecimal(int64(p.Error))
	o.WriteDecimal(int64(p.LocalEndpointServiceName))
	o.WriteBlob(p.LocalEndpointIp)
	o.WriteShort(int(p.LocalEndpointPort))
	o.WriteDecimal(int64(p.RemoteEndpointServiceName))
	o.WriteBlob(p.RemoteEndpointIp)
	o.WriteShort(int(p.RemoteEndpointPort))
	o.WriteBoolean(p.Debug)
	o.WriteBoolean(p.Shared)
	value.WriteValue(o, p.AnnotationTimestamps)
	value.WriteValue(o, p.AnnotationValues)
	value.WriteValue(o, p.Tags)
}

// Read deserializes the SpanPack from the input stream.
func (p *SpanPack) Read(d *protocol.DataInputX) error {
	var err error
	if p.Gxid, err = d.ReadLong(); err != nil {
		return err
	}
	if p.Txid, err = d.ReadLong(); err != nil {
		return err
	}
	if p.Caller, err = d.ReadLong(); err != nil {
		return err
	}
	if p.Timestamp, err = d.ReadLong(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Elapsed = int32(val)
	}
	if p.SpanType, err = d.ReadByte(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Name = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ObjHash = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Error = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.LocalEndpointServiceName = int32(val)
	}
	if p.LocalEndpointIp, err = d.ReadBlob(); err != nil {
		return err
	}
	if p.LocalEndpointPort, err = d.ReadShort(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.RemoteEndpointServiceName = int32(val)
	}
	if p.RemoteEndpointIp, err = d.ReadBlob(); err != nil {
		return err
	}
	if p.RemoteEndpointPort, err = d.ReadShort(); err != nil {
		return err
	}
	if p.Debug, err = d.ReadBoolean(); err != nil {
		return err
	}
	if p.Shared, err = d.ReadBoolean(); err != nil {
		return err
	}

	// Read annotation timestamps
	val, err := value.ReadValue(d)
	if err != nil {
		return err
	}
	var ok bool
	if p.AnnotationTimestamps, ok = val.(*value.ListValue); !ok && val != nil {
		p.AnnotationTimestamps = value.NewListValue()
	}

	// Read annotation values
	val, err = value.ReadValue(d)
	if err != nil {
		return err
	}
	if p.AnnotationValues, ok = val.(*value.ListValue); !ok && val != nil {
		p.AnnotationValues = value.NewListValue()
	}

	// Read tags
	val, err = value.ReadValue(d)
	if err != nil {
		return err
	}
	if p.Tags, ok = val.(*value.MapValue); !ok && val != nil {
		p.Tags = value.NewMapValue()
	}

	return nil
}
