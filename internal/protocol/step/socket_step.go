package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// SocketStep represents a socket connection step
type SocketStep struct {
	StepSingle
	IPAddr  []byte
	Port    int32
	Elapsed int32
	Error   int32
}

func (s *SocketStep) GetStepType() byte {
	return SOCKET
}

func (s *SocketStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteBlob(s.IPAddr)
	o.WriteDecimal(int64(s.Port))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.Error))
}

func (s *SocketStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	ipAddr, err := d.ReadBlob()
	if err != nil {
		return err
	}
	s.IPAddr = ipAddr

	port, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Port = int32(port)

	elapsed, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Elapsed = int32(elapsed)

	errorVal, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Error = int32(errorVal)

	return nil
}
