package step

import (
	"fmt"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

// Step type codes from Java StepEnum
const (
	METHOD                  = byte(1)
	SQL                     = byte(2)
	MESSAGE                 = byte(3)
	SOCKET                  = byte(5)
	APICALL                 = byte(6)
	THREAD_SUBMIT           = byte(7)
	SQL2                    = byte(8)
	HASHED_MESSAGE          = byte(9)
	METHOD2                 = byte(10)
	METHOD_SUM              = byte(11)
	DUMP                    = byte(12)
	DISPATCH                = byte(13)
	THREAD_CALL_POSSIBLE    = byte(14)
	APICALL2                = byte(15)
	SQL3                    = byte(16)
	PARAMETERIZED_MESSAGE   = byte(17)
	SQL_SUM                 = byte(21)
	MESSAGE_SUM             = byte(31)
	SOCKET_SUM              = byte(42)
	APICALL_SUM             = byte(43)
	SPAN                    = byte(51)
	SPANCALL                = byte(52)
	CONTROL                 = byte(99)
)

// Step interface for all step types
type Step interface {
	StepType() byte
	Write(o *protocol.DataOutputX)
	Read(d *protocol.DataInputX) error
}

// CreateStep creates a step instance based on type code
func CreateStep(typeCode byte) (Step, error) {
	switch typeCode {
	case METHOD:
		return &MethodStep{}, nil
	case METHOD2:
		return &MethodStep2{}, nil
	case SQL:
		return &SqlStep{}, nil
	case SQL2:
		return &SqlStep2{}, nil
	case SQL3:
		return &SqlStep3{}, nil
	case MESSAGE:
		return &MessageStep{}, nil
	case SOCKET:
		return &SocketStep{}, nil
	case APICALL:
		return &ApiCallStep{}, nil
	case APICALL2:
		return &ApiCallStep2{}, nil
	case DISPATCH:
		return &DispatchStep{}, nil
	case THREAD_SUBMIT:
		return &ThreadSubmitStep{}, nil
	case THREAD_CALL_POSSIBLE:
		return &ThreadCallPossibleStep{}, nil
	case HASHED_MESSAGE:
		return &HashedMessageStep{}, nil
	case PARAMETERIZED_MESSAGE:
		return &ParameterizedMessageStep{}, nil
	case DUMP:
		return &DumpStep{}, nil
	case METHOD_SUM:
		return &MethodSum{}, nil
	case SQL_SUM:
		return &SqlSum{}, nil
	case MESSAGE_SUM:
		return &MessageSum{}, nil
	case SOCKET_SUM:
		return &SocketSum{}, nil
	case APICALL_SUM:
		return &ApiCallSum{}, nil
	case SPAN:
		return &SpanStep{}, nil
	case SPANCALL:
		return &SpanCallStep{}, nil
	case CONTROL:
		return &StepControl{}, nil
	default:
		return nil, fmt.Errorf("unknown step type: %d", typeCode)
	}
}

// WriteStep writes a step with its type code
func WriteStep(o *protocol.DataOutputX, s Step) {
	o.WriteByte(s.StepType())
	s.Write(o)
}

// ReadStep reads a step from the input
func ReadStep(d *protocol.DataInputX) (Step, error) {
	typeCode, err := d.ReadByte()
	if err != nil {
		return nil, err
	}
	step, err := CreateStep(typeCode)
	if err != nil {
		return nil, err
	}
	err = step.Read(d)
	if err != nil {
		return nil, err
	}
	return step, nil
}

// StepSingle base struct used by most steps
type StepSingle struct {
	Parent    int32
	Index     int32
	StartTime int32
	StartCpu  int32
}

func (s *StepSingle) Write(o *protocol.DataOutputX) {
	o.WriteDecimal(int64(s.Parent))
	o.WriteDecimal(int64(s.Index))
	o.WriteDecimal(int64(s.StartTime))
	o.WriteDecimal(int64(s.StartCpu))
}

func (s *StepSingle) Read(d *protocol.DataInputX) error {
	parent, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Parent = int32(parent)

	index, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.Index = int32(index)

	startTime, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.StartTime = int32(startTime)

	startCpu, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	s.StartCpu = int32(startCpu)

	return nil
}

// StepSummary base struct used by summary steps
type StepSummary struct {
	// No fields to serialize
}
