package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// ThreadCallPossibleStep represents a possible thread call step
type ThreadCallPossibleStep struct {
	StepSingle
	Txid     int64
	Hash     int32
	Elapsed  int32
	Threaded byte
}

func (s *ThreadCallPossibleStep) StepType() byte {
	return THREAD_CALL_POSSIBLE
}

func (s *ThreadCallPossibleStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteInt64(s.Txid)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteByte(s.Threaded)
}

func (s *ThreadCallPossibleStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	txid, err := d.ReadInt64()
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

	threaded, err := d.ReadByte()
	if err != nil {
		return err
	}
	s.Threaded = threaded

	return nil
}
