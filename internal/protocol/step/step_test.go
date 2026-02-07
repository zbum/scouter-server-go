package step

import (
	"testing"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

func roundTripStep(t *testing.T, s Step) Step {
	t.Helper()
	o := protocol.NewDataOutputX()
	WriteStep(o, s)
	d := protocol.NewDataInputX(o.ToByteArray())
	result, err := ReadStep(d)
	if err != nil {
		t.Fatalf("ReadStep error: %v", err)
	}
	return result
}

func TestMethodStep(t *testing.T) {
	s := &MethodStep{
		StepSingle: StepSingle{Parent: 0, Index: 1, StartTime: 100, StartCpu: 50},
		Hash:       12345,
		Elapsed:    200,
		CpuTime:    100,
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*MethodStep)
	if !ok {
		t.Fatalf("expected MethodStep, got %T", result)
	}
	if rs.Hash != 12345 {
		t.Errorf("Hash: expected 12345, got %d", rs.Hash)
	}
	if rs.Elapsed != 200 {
		t.Errorf("Elapsed: expected 200, got %d", rs.Elapsed)
	}
	if rs.Parent != 0 || rs.Index != 1 || rs.StartTime != 100 || rs.StartCpu != 50 {
		t.Error("StepSingle fields mismatch")
	}
}

func TestMethodStep2(t *testing.T) {
	s := &MethodStep2{
		MethodStep: MethodStep{
			StepSingle: StepSingle{Parent: 0, Index: 2, StartTime: 150, StartCpu: 75},
			Hash:       54321,
			Elapsed:    300,
			CpuTime:    150,
		},
		Error: 999,
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*MethodStep2)
	if !ok {
		t.Fatalf("expected MethodStep2, got %T", result)
	}
	if rs.Hash != 54321 {
		t.Errorf("Hash: expected 54321, got %d", rs.Hash)
	}
	if rs.Error != 999 {
		t.Errorf("Error: expected 999, got %d", rs.Error)
	}
}

func TestSqlStep(t *testing.T) {
	s := &SqlStep{
		StepSingle: StepSingle{Parent: 0, Index: 3, StartTime: 200, StartCpu: 100},
		Hash:       11111,
		Elapsed:    50,
		Error:      0,
		Param:      "param1",
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*SqlStep)
	if !ok {
		t.Fatalf("expected SqlStep, got %T", result)
	}
	if rs.Hash != 11111 {
		t.Errorf("Hash: expected 11111, got %d", rs.Hash)
	}
	if rs.Param != "param1" {
		t.Errorf("Param: expected 'param1', got %q", rs.Param)
	}
}

func TestMessageStep(t *testing.T) {
	s := &MessageStep{
		StepSingle: StepSingle{Parent: 0, Index: 4, StartTime: 250, StartCpu: 125},
		Message:    "test message",
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*MessageStep)
	if !ok {
		t.Fatalf("expected MessageStep, got %T", result)
	}
	if rs.Message != "test message" {
		t.Errorf("Message: expected 'test message', got %q", rs.Message)
	}
}

func TestApiCallStep(t *testing.T) {
	s := &ApiCallStep{
		StepSingle: StepSingle{Parent: 0, Index: 5, StartTime: 300, StartCpu: 150},
		Hash:       33333,
		Elapsed:    100,
		CpuTime:    50,
		Error:      0,
		Opt:        1,
		Address:    "http://example.com",
		Txid:       777888999,
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*ApiCallStep)
	if !ok {
		t.Fatalf("expected ApiCallStep, got %T", result)
	}
	if rs.Hash != 33333 {
		t.Errorf("Hash: expected 33333, got %d", rs.Hash)
	}
	if rs.Address != "http://example.com" {
		t.Errorf("Address: expected 'http://example.com', got %q", rs.Address)
	}
	if rs.Txid != 777888999 {
		t.Errorf("Txid: expected 777888999, got %d", rs.Txid)
	}
}

func TestSocketStep(t *testing.T) {
	s := &SocketStep{
		StepSingle: StepSingle{Parent: 0, Index: 6, StartTime: 350, StartCpu: 175},
		IPAddr:     []byte{10, 0, 0, 1},
		Port:       8080,
		Elapsed:    50,
		Error:      0,
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*SocketStep)
	if !ok {
		t.Fatalf("expected SocketStep, got %T", result)
	}
	if rs.Port != 8080 {
		t.Errorf("Port: expected 8080, got %d", rs.Port)
	}
	if len(rs.IPAddr) != 4 || rs.IPAddr[0] != 10 {
		t.Errorf("IPAddr: unexpected value %v", rs.IPAddr)
	}
}

func TestMethodSum(t *testing.T) {
	s := &MethodSum{
		Hash:    44444,
		Count:   10,
		Elapsed: 500,
		CpuTime: 250,
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*MethodSum)
	if !ok {
		t.Fatalf("expected MethodSum, got %T", result)
	}
	if rs.Hash != 44444 {
		t.Errorf("Hash: expected 44444, got %d", rs.Hash)
	}
	if rs.Count != 10 {
		t.Errorf("Count: expected 10, got %d", rs.Count)
	}
}

func TestSpanStep(t *testing.T) {
	s := &SpanStep{
		CommonSpanStep: CommonSpanStep{
			StepSingle:               StepSingle{Parent: 0, Index: 7, StartTime: 400, StartCpu: 200},
			LocalEndpointServiceName: 55555,
			LocalEndpointIp:          []byte{10, 0, 0, 1},
			LocalEndpointPort:        8080,
			Debug:                    true,
			Timestamp:                1234567890,
			Elapsed:                  100,
		},
	}
	result := roundTripStep(t, s)
	rs, ok := result.(*SpanStep)
	if !ok {
		t.Fatalf("expected SpanStep, got %T", result)
	}
	if rs.LocalEndpointServiceName != 55555 {
		t.Errorf("LocalEndpointServiceName: expected 55555, got %d", rs.LocalEndpointServiceName)
	}
	if !rs.Debug {
		t.Error("Debug: expected true")
	}
}

func TestAllStepTypes(t *testing.T) {
	types := []byte{
		METHOD, SQL, MESSAGE, SOCKET, APICALL,
		THREAD_SUBMIT, SQL2, HASHED_MESSAGE, METHOD2,
		METHOD_SUM, DUMP, DISPATCH, THREAD_CALL_POSSIBLE,
		APICALL2, SQL3, PARAMETERIZED_MESSAGE,
		SQL_SUM, MESSAGE_SUM, SOCKET_SUM, APICALL_SUM,
		SPAN, SPANCALL, CONTROL,
	}
	for _, tc := range types {
		s, err := CreateStep(tc)
		if err != nil {
			t.Errorf("CreateStep(%d) error: %v", tc, err)
			continue
		}
		if s.GetStepType() != tc {
			t.Errorf("type mismatch: expected %d, got %d", tc, s.GetStepType())
		}
	}
}

func TestAllStepRoundTrip(t *testing.T) {
	types := []byte{
		METHOD, SQL, MESSAGE, SOCKET, APICALL,
		THREAD_SUBMIT, SQL2, HASHED_MESSAGE, METHOD2,
		METHOD_SUM, DUMP, DISPATCH, THREAD_CALL_POSSIBLE,
		APICALL2, SQL3, PARAMETERIZED_MESSAGE,
		SQL_SUM, MESSAGE_SUM, SOCKET_SUM, APICALL_SUM,
		SPAN, SPANCALL, CONTROL,
	}
	for _, tc := range types {
		s, err := CreateStep(tc)
		if err != nil {
			t.Errorf("CreateStep(%d) error: %v", tc, err)
			continue
		}
		// Write and read back with default (zero) values
		o := protocol.NewDataOutputX()
		WriteStep(o, s)
		d := protocol.NewDataInputX(o.ToByteArray())
		result, err := ReadStep(d)
		if err != nil {
			t.Errorf("ReadStep(%d) error: %v", tc, err)
			continue
		}
		if result.GetStepType() != tc {
			t.Errorf("type %d: round-trip type mismatch, got %d", tc, result.GetStepType())
		}
	}
}
