package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// MethodStep2 extends MethodStep with error information
type MethodStep2 struct {
	MethodStep
	Error int32
}

func (s *MethodStep2) StepType() byte {
	return METHOD2
}

func (s *MethodStep2) Write(o *protocol.DataOutputX) {
	s.MethodStep.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteDecimal(int64(s.Error))
}

func (s *MethodStep2) Read(d *protocol.DataInputX) error {
	if err := s.MethodStep.StepSingle.Read(d); err != nil {
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

	errorVal, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Error = int32(errorVal)

	return nil
}
