package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// SqlStep2 extends SqlStep with XType
type SqlStep2 struct {
	SqlStep
	XType byte
}

func (s *SqlStep2) GetStepType() byte {
	return SQL2
}

func (s *SqlStep2) Write(o *protocol.DataOutputX) {
	s.SqlStep.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteText(s.Param)
	o.WriteDecimal(int64(s.Error))
	o.WriteByte(s.XType)
}

func (s *SqlStep2) Read(d *protocol.DataInputX) error {
	if err := s.SqlStep.StepSingle.Read(d); err != nil {
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

	return nil
}
