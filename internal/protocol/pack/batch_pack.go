package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// BatchPack represents batch job data (uses blob wrapping).
type BatchPack struct {
	StartTime    int64
	ObjHash      int32
	BatchJobId   string
	Args         string
	PID          int32
	ElapsedTime  int64
	ThreadCnt    int32
	CpuTime      int64
	GcTime       int64
	GcCount      int64
	SqlTotalCnt  int32
	SqlTotalTime int64
	SqlTotalRows int64
	SqlTotalRuns int64
	IsStack      bool
	ObjName      string
	ObjType      string
	Position     int64
}

// GetPackType returns the pack type code.
func (p *BatchPack) GetPackType() byte {
	return PackTypeBatch
}

// Write serializes the BatchPack using blob wrapping.
func (p *BatchPack) Write(o *protocol.DataOutputX) {
	inner := protocol.NewDataOutputX()

	// Write all fields to inner buffer
	inner.WriteLong(p.StartTime)
	inner.WriteInt(p.ObjHash)
	inner.WriteText(p.BatchJobId)
	inner.WriteText(p.Args)
	inner.WriteInt(p.PID)
	inner.WriteLong(p.ElapsedTime)
	inner.WriteInt(p.ThreadCnt)
	inner.WriteLong(p.CpuTime)
	inner.WriteLong(p.GcTime)
	inner.WriteLong(p.GcCount)
	inner.WriteInt(p.SqlTotalCnt)
	inner.WriteLong(p.SqlTotalTime)
	inner.WriteLong(p.SqlTotalRows)
	inner.WriteLong(p.SqlTotalRuns)
	inner.WriteBoolean(p.IsStack)
	inner.WriteText(p.ObjName)
	inner.WriteText(p.ObjType)
	inner.WriteLong(p.Position)

	// Write inner buffer as blob
	o.WriteBlob(inner.ToByteArray())
}

// Read deserializes the BatchPack from blob-wrapped data.
func (p *BatchPack) Read(din *protocol.DataInputX) error {
	blob, err := din.ReadBlob()
	if err != nil {
		return err
	}

	d := protocol.NewDataInputX(blob)

	// Read all fields from inner buffer
	if p.StartTime, err = d.ReadLong(); err != nil {
		return err
	}
	if p.ObjHash, err = d.ReadInt(); err != nil {
		return err
	}
	if p.BatchJobId, err = d.ReadText(); err != nil {
		return err
	}
	if p.Args, err = d.ReadText(); err != nil {
		return err
	}
	if p.PID, err = d.ReadInt(); err != nil {
		return err
	}
	if p.ElapsedTime, err = d.ReadLong(); err != nil {
		return err
	}
	if p.ThreadCnt, err = d.ReadInt(); err != nil {
		return err
	}
	if p.CpuTime, err = d.ReadLong(); err != nil {
		return err
	}
	if p.GcTime, err = d.ReadLong(); err != nil {
		return err
	}
	if p.GcCount, err = d.ReadLong(); err != nil {
		return err
	}
	if p.SqlTotalCnt, err = d.ReadInt(); err != nil {
		return err
	}
	if p.SqlTotalTime, err = d.ReadLong(); err != nil {
		return err
	}
	if p.SqlTotalRows, err = d.ReadLong(); err != nil {
		return err
	}
	if p.SqlTotalRuns, err = d.ReadLong(); err != nil {
		return err
	}
	if p.IsStack, err = d.ReadBoolean(); err != nil {
		return err
	}
	if p.ObjName, err = d.ReadText(); err != nil {
		return err
	}
	if p.ObjType, err = d.ReadText(); err != nil {
		return err
	}
	if p.Position, err = d.ReadLong(); err != nil {
		return err
	}

	return nil
}
