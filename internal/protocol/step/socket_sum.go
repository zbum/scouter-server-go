package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// SocketSum represents a socket summary step
type SocketSum struct {
	StepSummary
	IPAddr  []byte
	Port    int32
	Count   int32
	Elapsed int32
	Error   int32
}

func (s *SocketSum) GetStepType() byte {
	return SOCKET_SUM
}

func (s *SocketSum) Write(o *protocol.DataOutputX) {
	o.WriteBlob(s.IPAddr)
	o.WriteDecimal(int64(s.Port))
	o.WriteDecimal(int64(s.Count))
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.Error))
}

func (s *SocketSum) Read(d *protocol.DataInputX) error {
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

	errorVal, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Error = int32(errorVal)

	return nil
}
