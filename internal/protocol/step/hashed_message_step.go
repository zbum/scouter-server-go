package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// HashedMessageStep represents a hashed message step
type HashedMessageStep struct {
	StepSingle
	Hash  int32
	Time  int32
	Value int32
}

func (s *HashedMessageStep) StepType() byte {
	return HASHED_MESSAGE
}

func (s *HashedMessageStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Time))
	o.WriteDecimal(int64(s.Value))
}

func (s *HashedMessageStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	hash, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Hash = int32(hash)

	time, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Time = int32(time)

	value, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Value = int32(value)

	return nil
}
