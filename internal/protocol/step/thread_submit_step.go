package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// ThreadSubmitStep represents a thread submit step
type ThreadSubmitStep struct {
	StepSingle
	Txid    int64
	Hash    int32
	Elapsed int32
	CpuTime int32
	Error   int32
}

func (s *ThreadSubmitStep) GetStepType() byte {
	return THREAD_SUBMIT
}

func (s *ThreadSubmitStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteLong(s.Txid)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteDecimal(int64(s.Error))
}

func (s *ThreadSubmitStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	txid, err := d.ReadLong()
	if err != nil {
		return err
	}
	s.Txid = txid

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
