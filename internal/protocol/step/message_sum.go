package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// MessageSum represents a message summary step
type MessageSum struct {
	StepSummary
	Hash  int32
	Count int32
}

func (s *MessageSum) GetStepType() byte {
	return MESSAGE_SUM
}

func (s *MessageSum) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Count))
}

func (s *MessageSum) Read(d *protocol.DataInputX) error {
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

	return nil
}
