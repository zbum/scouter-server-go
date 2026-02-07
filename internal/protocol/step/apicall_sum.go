package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// ApiCallSum represents an API call summary step
type ApiCallSum struct {
	StepSummary
	Hash    int32
	Count   int32
	Elapsed int32
	CpuTime int32
	Error   int32
}

func (s *ApiCallSum) GetStepType() byte {
	return APICALL_SUM
}

func (s *ApiCallSum) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Count))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteDecimal(int64(s.Error))
}

func (s *ApiCallSum) Read(d *protocol.DataInputX) error {
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

	return nil
}
