package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// SqlSum represents a SQL summary step
type SqlSum struct {
	StepSummary
	Hash       int32
	Count      int32
	Elapsed    int32
	CpuTime    int32
	Error      int32
	Param      string
	ParamError string
}

func (s *SqlSum) GetStepType() byte {
	return SQL_SUM
}

func (s *SqlSum) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Count))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteDecimal(int64(s.Error))
	o.WriteText(s.Param)
	o.WriteText(s.ParamError)
}

func (s *SqlSum) Read(d *protocol.DataInputX) error {
	hash, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Hash = int32(hash)

	count, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Count = int32(count)

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

	errorVal, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Error = int32(errorVal)

	param, err := d.ReadText()
	if err != nil {
		return err
	}
	s.Param = param

	paramError, err := d.ReadText()
	if err != nil {
		return err
	}
	s.ParamError = paramError

	return nil
}
