package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// XLogType constants matching Java's XLogTypes.
const (
	XLogTypeWebService byte = 0
	XLogTypeAppService byte = 1
)

// XLogPack represents transaction log data (most complex pack type with 40+ fields).
type XLogPack struct {
	EndTime                           int64
	ObjHash                           int32
	Service                           int32
	Txid                              int64
	ThreadNameHash                    int32
	Caller                            int64
	Gxid                              int64
	Elapsed                           int32
	Error                             int32
	Cpu                               int32
	SqlCount                          int32
	SqlTime                           int32
	IPAddr                            []byte
	Kbytes                            int32
	Status                            int32
	Userid                            int64
	UserAgent                         int32
	Referer                           int32
	Group                             int32
	ApicallCount                      int32
	ApicallTime                       int32
	CountryCode                       string
	City                              int32
	XType                             byte
	Login                             int32
	Desc                              int32
	WebHash                           int32
	WebTime                           int32
	HasDump                           byte
	Text1                             string
	Text2                             string
	QueuingHostHash                   int32
	QueuingTime                       int32
	Queuing2ndHostHash                int32
	Queuing2ndTime                    int32
	Text3                             string
	Text4                             string
	Text5                             string
	ProfileCount                      int32
	B3Mode                            bool
	ProfileSize                       int32
	DiscardType                       byte
	IgnoreGlobalConsequentSampling    bool
}

// PackType returns the pack type code.
func (p *XLogPack) PackType() byte {
	return PackTypeXLog
}

// ReadXLogFilterFields extracts only ObjHash and Elapsed from serialized XLogPack data
// by parsing just the first 7 fields instead of all 42+ fields.
// This avoids the cost of full deserialization when only filter fields are needed.
func ReadXLogFilterFields(data []byte) (objHash int32, elapsed int32, err error) {
	din := protocol.NewDataInputX(data)

	// Skip pack type byte
	if _, err = din.ReadByte(); err != nil {
		return
	}

	// Read blob to get inner buffer
	blob, err := din.ReadBlob()
	if err != nil {
		return
	}

	d := protocol.NewDataInputX(blob)

	// 1. Skip EndTime (WriteDecimal)
	if _, err = d.ReadDecimal(); err != nil {
		return
	}

	// 2. Read ObjHash (WriteDecimal)
	v, err := d.ReadDecimal()
	if err != nil {
		return
	}
	objHash = int32(v)

	// 3. Skip Service (WriteDecimal)
	if _, err = d.ReadDecimal(); err != nil {
		return
	}

	// 4-6. Skip Txid + Caller + Gxid (WriteLong × 3 = 24 bytes)
	if err = d.SkipBytes(24); err != nil {
		return
	}

	// 7. Read Elapsed (WriteDecimal)
	v, err = d.ReadDecimal()
	if err != nil {
		return
	}
	elapsed = int32(v)

	return
}

// Write serializes the XLogPack using blob wrapping.
func (p *XLogPack) Write(o *protocol.DataOutputX) {
	inner := protocol.NewDataOutputX()

	// Write all fields to inner buffer
	inner.WriteDecimal(p.EndTime)
	inner.WriteDecimal(int64(p.ObjHash))
	inner.WriteDecimal(int64(p.Service))
	inner.WriteInt64(p.Txid)
	inner.WriteInt64(p.Caller)
	inner.WriteInt64(p.Gxid)
	inner.WriteDecimal(int64(p.Elapsed))
	inner.WriteDecimal(int64(p.Error))
	inner.WriteDecimal(int64(p.Cpu))
	inner.WriteDecimal(int64(p.SqlCount))
	inner.WriteDecimal(int64(p.SqlTime))
	inner.WriteBlob(p.IPAddr)
	inner.WriteDecimal(int64(p.Kbytes))
	inner.WriteDecimal(int64(p.Status))
	inner.WriteDecimal(p.Userid)
	inner.WriteDecimal(int64(p.UserAgent))
	inner.WriteDecimal(int64(p.Referer))
	inner.WriteDecimal(int64(p.Group))
	inner.WriteDecimal(int64(p.ApicallCount))
	inner.WriteDecimal(int64(p.ApicallTime))
	inner.WriteText(p.CountryCode)
	inner.WriteDecimal(int64(p.City))
	inner.WriteByte(p.XType)
	inner.WriteDecimal(int64(p.Login))
	inner.WriteDecimal(int64(p.Desc))
	inner.WriteDecimal(int64(p.WebHash))
	inner.WriteDecimal(int64(p.WebTime))
	inner.WriteByte(p.HasDump)
	inner.WriteDecimal(int64(p.ThreadNameHash))
	inner.WriteText(p.Text1)
	inner.WriteText(p.Text2)
	inner.WriteDecimal(int64(p.QueuingHostHash))
	inner.WriteDecimal(int64(p.QueuingTime))
	inner.WriteDecimal(int64(p.Queuing2ndHostHash))
	inner.WriteDecimal(int64(p.Queuing2ndTime))
	inner.WriteText(p.Text3)
	inner.WriteText(p.Text4)
	inner.WriteText(p.Text5)
	inner.WriteDecimal(int64(p.ProfileCount))
	inner.WriteBoolean(p.B3Mode)
	inner.WriteDecimal(int64(p.ProfileSize))
	inner.WriteByte(p.DiscardType)
	inner.WriteBoolean(p.IgnoreGlobalConsequentSampling)

	// Write inner buffer as blob
	o.WriteBlob(inner.ToByteArray())
}

// Read deserializes the XLogPack from blob-wrapped data.
func (p *XLogPack) Read(din *protocol.DataInputX) error {
	blob, err := din.ReadBlob()
	if err != nil {
		return err
	}

	d := protocol.NewDataInputX(blob)

	// Read all fields from inner buffer
	if p.EndTime, err = d.ReadDecimal(); err != nil {
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
	if p.Txid, err = d.ReadInt64(); err != nil {
		return err
	}
	if p.Caller, err = d.ReadInt64(); err != nil {
		return err
	}
	if p.Gxid, err = d.ReadInt64(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Elapsed = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Error = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Cpu = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.SqlCount = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.SqlTime = int32(val)
	}
	if p.IPAddr, err = d.ReadBlob(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Kbytes = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Status = int32(val)
	}
	if p.Userid, err = d.ReadDecimal(); err != nil {
		return err
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.UserAgent = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Referer = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.Group = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ApicallCount = int32(val)
	}
	if val, err := d.ReadDecimal(); err != nil {
		return err
	} else {
		p.ApicallTime = int32(val)
	}

	// Backward compatibility: read optional fields only if available
	if d.Available() > 0 {
		if p.CountryCode, err = d.ReadText(); err != nil {
			return err
		}
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.City = int32(val)
		}
	}

	if d.Available() > 0 {
		if p.XType, err = d.ReadByte(); err != nil {
			return err
		}
	}

	if d.Available() > 0 {
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.Login = int32(val)
		}
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.Desc = int32(val)
		}
	}

	if d.Available() > 0 {
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.WebHash = int32(val)
		}
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.WebTime = int32(val)
		}
	}

	if d.Available() > 0 {
		if p.HasDump, err = d.ReadByte(); err != nil {
			return err
		}
	}

	if d.Available() > 0 {
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.ThreadNameHash = int32(val)
		}
	}

	if d.Available() > 0 {
		if p.Text1, err = d.ReadText(); err != nil {
			return err
		}
		if p.Text2, err = d.ReadText(); err != nil {
			return err
		}
	}

	if d.Available() > 0 {
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.QueuingHostHash = int32(val)
		}
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.QueuingTime = int32(val)
		}
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.Queuing2ndHostHash = int32(val)
		}
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.Queuing2ndTime = int32(val)
		}
	}

	if d.Available() > 0 {
		if p.Text3, err = d.ReadText(); err != nil {
			return err
		}
		if p.Text4, err = d.ReadText(); err != nil {
			return err
		}
		if p.Text5, err = d.ReadText(); err != nil {
			return err
		}
	}

	if d.Available() > 0 {
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.ProfileCount = int32(val)
		}
	}

	if d.Available() > 0 {
		if p.B3Mode, err = d.ReadBoolean(); err != nil {
			return err
		}
	}

	if d.Available() > 0 {
		if val, err := d.ReadDecimal(); err != nil {
			return err
		} else {
			p.ProfileSize = int32(val)
		}
		if p.DiscardType, err = d.ReadByte(); err != nil {
			return err
		}
		if p.IgnoreGlobalConsequentSampling, err = d.ReadBoolean(); err != nil {
			return err
		}
	}

	return nil
}
