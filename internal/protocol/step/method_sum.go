package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// MethodSum represents a method summary step
type MethodSum struct {
	StepSummary
	Hash    int32
	Count   int32
	Elapsed int32
	CpuTime int32
}

func (s *MethodSum) GetStepType() byte {
	return METHOD_SUM
}

func (s *MethodSum) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Count))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
}

func (s *MethodSum) Read(d *protocol.DataInputX) error {
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

	return nil
}
