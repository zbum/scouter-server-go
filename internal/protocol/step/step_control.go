package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// StepControl represents a control step
type StepControl struct {
	StepSingle
	Code    byte
	Message string
}

func (s *StepControl) GetStepType() byte {
	return CONTROL
}

func (s *StepControl) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteByte(s.Code)
	o.WriteText(s.Message)
}

func (s *StepControl) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	code, err := d.ReadByte()
	if err != nil {
		return err
	}
	s.Code = code

	message, err := d.ReadText()
	if err != nil {
		return err
	}
	s.Message = message

	return nil
}
