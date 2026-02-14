package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// SqlStep3 extends SqlStep2 with Updated count
type SqlStep3 struct {
	SqlStep2
	Updated int32
}

func (s *SqlStep3) StepType() byte {
	return SQL3
}

func (s *SqlStep3) Write(o *protocol.DataOutputX) {
	s.SqlStep2.SqlStep.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteText(s.Param)
	o.WriteDecimal(int64(s.Error))
	o.WriteByte(s.XType)
	o.WriteDecimal(int64(s.Updated))
}

func (s *SqlStep3) Read(d *protocol.DataInputX) error {
	if err := s.SqlStep2.SqlStep.StepSingle.Read(d); err != nil {
		return err
	}

	hash, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Hash = int32(hash)

	elapsed, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Elapsed = int32(elapsed)

	cpuTime, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.CpuTime = int32(cpuTime)

	param, err := d.ReadText()
	if err != nil {
		return err
	}
	s.Param = param

	errorVal, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Error = int32(errorVal)

	xtype, err := d.ReadByte()
	if err != nil {
		return err
	}
	s.XType = xtype

	updated, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Updated = int32(updated)

	return nil
}
