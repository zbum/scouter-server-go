package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// CommonSpanStep base struct for span-related steps
type CommonSpanStep struct {
	StepSingle
	LocalEndpointServiceName  int32
	LocalEndpointIp           []byte
	LocalEndpointPort         int16
	RemoteEndpointServiceName int32
	RemoteEndpointIp          []byte
	RemoteEndpointPort        int16
	Debug                     bool
	Shared                    bool
	Timestamp                 int64
	Elapsed                   int32
	Error                     int32
	AnnotationTimestamps      []int64
	AnnotationValues          []int32
}

func (s *CommonSpanStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteDecimal(int64(s.LocalEndpointServiceName))
	o.WriteBlob(s.LocalEndpointIp)
	o.WriteInt16(s.LocalEndpointPort)
	o.WriteDecimal(int64(s.RemoteEndpointServiceName))
	o.WriteBlob(s.RemoteEndpointIp)
	o.WriteInt16(s.RemoteEndpointPort)
	o.WriteBoolean(s.Debug)
	o.WriteBoolean(s.Shared)
	o.WriteInt64(s.Timestamp)
	o.WriteDecimal(int64(s.Elapsed))
	o.WriteDecimal(int64(s.Error))
	o.WriteDecimalArray(s.AnnotationTimestamps)
	o.WriteDecimalIntArray(s.AnnotationValues)
}

func (s *CommonSpanStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	localEndpointServiceName, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.LocalEndpointServiceName = int32(localEndpointServiceName)

	localEndpointIp, err := d.ReadBlob()
	if err != nil {
		return err
	}
	s.LocalEndpointIp = localEndpointIp

	localEndpointPort, err := d.ReadInt16()
	if err != nil {
		return err
	}
	s.LocalEndpointPort = localEndpointPort

	remoteEndpointServiceName, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.RemoteEndpointServiceName = int32(remoteEndpointServiceName)

	remoteEndpointIp, err := d.ReadBlob()
	if err != nil {
		return err
	}
	s.RemoteEndpointIp = remoteEndpointIp

	remoteEndpointPort, err := d.ReadInt16()
	if err != nil {
		return err
	}
	s.RemoteEndpointPort = remoteEndpointPort

	debug, err := d.ReadBoolean()
	if err != nil {
		return err
	}
	s.Debug = debug

	shared, err := d.ReadBoolean()
	if err != nil {
		return err
	}
	s.Shared = shared

	timestamp, err := d.ReadInt64()
	if err != nil {
		return err
	}
	s.Timestamp = timestamp

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

	annotationTimestamps, err := d.ReadDecimalArray()
	if err != nil {
		return err
	}
	s.AnnotationTimestamps = annotationTimestamps

	annotationValues, err := d.ReadDecimalIntArray()
	if err != nil {
		return err
	}
	s.AnnotationValues = annotationValues

	return nil
}

// SpanStep represents a span step
type SpanStep struct {
	CommonSpanStep
}

func (s *SpanStep) StepType() byte {
	return SPAN
}

func (s *SpanStep) Write(o *protocol.DataOutputX) {
	s.CommonSpanStep.Write(o)
}

func (s *SpanStep) Read(d *protocol.DataInputX) error {
	return s.CommonSpanStep.Read(d)
}

// SpanCallStep extends CommonSpanStep with additional fields
type SpanCallStep struct {
	CommonSpanStep
	Txid    int64
	Opt     byte
	Address string
	Async   byte
}

func (s *SpanCallStep) StepType() byte {
	return SPANCALL
}

func (s *SpanCallStep) Write(o *protocol.DataOutputX) {
	s.CommonSpanStep.Write(o)
	o.WriteInt64(s.Txid)
	o.WriteByte(s.Opt)
	if s.Opt == 1 {
		o.WriteText(s.Address)
	}
	o.WriteByte(s.Async)
}

func (s *SpanCallStep) Read(d *protocol.DataInputX) error {
	if err := s.CommonSpanStep.Read(d); err != nil {
		return err
	}

	txid, err := d.ReadInt64()
	if err != nil {
		return err
	}
	s.Txid = txid

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

	async, err := d.ReadByte()
	if err != nil {
		return err
	}
	s.Async = async

	return nil
}
