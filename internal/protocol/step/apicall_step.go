package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// ApiCallStep represents an API call step
type ApiCallStep struct {
	StepSingle
	Txid    int64
	Hash    int32
	Elapsed int32
	CpuTime int32
	Error   int32
	Opt     byte
	Address string
}

func (s *ApiCallStep) GetStepType() byte {
	return APICALL
}

func (s *ApiCallStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteLong(s.Txid)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.CpuTime))
	o.WriteDecimal(int64(s.Error))
	o.WriteByte(s.Opt)
	if s.Opt == 1 {
		o.WriteText(s.Address)
	}
}

func (s *ApiCallStep) Read(d *protocol.DataInputX) error {
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

	opt, err := d.ReadByte()
	if err != nil {
		return err
	}
	s.Opt = opt

	if s.Opt == 1 {
		address, err := d.ReadText()
		if err != nil {
			return err
		}
		s.Address = address
	}

	return nil
}
