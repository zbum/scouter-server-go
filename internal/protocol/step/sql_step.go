package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// SqlStep represents a SQL execution step
type SqlStep struct {
	StepSingle
	Hash    int32
	Elapsed int32
	CpuTime int32
	Param   string
	Error   int32
}

func (s *SqlStep) StepType() byte {
	return SQL
}

func (s *SqlStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteText(s.Param)
	o.WriteDecimal(int64(s.Error))
}

func (s *SqlStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
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

	return nil
}
