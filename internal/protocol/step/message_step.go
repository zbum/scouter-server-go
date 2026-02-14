package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// MessageStep represents a message step
type MessageStep struct {
	StepSingle
	Message string
}

func (s *MessageStep) StepType() byte {
	return MESSAGE
}

func (s *MessageStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteText(s.Message)
}

func (s *MessageStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	message, err := d.ReadText()
	if err != nil {
		return err
	}
	s.Message = message

	return nil
}
