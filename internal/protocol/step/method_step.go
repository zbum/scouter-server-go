package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// MethodStep represents a method execution step
type MethodStep struct {
	StepSingle
	Hash     int32
	Elapsed  int32
	CpuTime  int32
}

func (s *MethodStep) StepType() byte {
	return METHOD
}

func (s *MethodStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
}

func (s *MethodStep) Read(d *protocol.DataInputX) error {
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

	return nil
}
