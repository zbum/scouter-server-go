package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// ParameterizedMessageStep represents a parameterized message step
type ParameterizedMessageStep struct {
	StepSingle
	Hash        int32
	Elapsed     int32
	Level       byte
	ParamString string
}

func (s *ParameterizedMessageStep) StepType() byte {
	return PARAMETERIZED_MESSAGE
}

func (s *ParameterizedMessageStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteDecimal(int64(s.Hash))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteByte(s.Level)
	o.WriteText(s.ParamString)
}

func (s *ParameterizedMessageStep) Read(d *protocol.DataInputX) error {
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

	level, err := d.ReadByte()
	if err != nil {
		return err
	}
	s.Level = level

	paramString, err := d.ReadText()
	if err != nil {
		return err
	}
	s.ParamString = paramString

	return nil
}
