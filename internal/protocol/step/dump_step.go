package step

import "github.com/zbum/scouter-server-go/internal/protocol"

// DumpStep represents a thread dump step
type DumpStep struct {
	StepSingle
	Stacks        []int32
	ThreadId      int64
	ThreadName    string
	ThreadState   string
	LockOwnerId   int64
	LockName      string
	LockOwnerName string
}

func (s *DumpStep) GetStepType() byte {
	return DUMP
}

func (s *DumpStep) Write(o *protocol.DataOutputX) {
	s.StepSingle.Write(o)
	o.WriteDecimalIntArray(s.Stacks)
	o.WriteLong(s.ThreadId)
	o.WriteText(s.ThreadName)
	o.WriteText(s.ThreadState)
	o.WriteLong(s.LockOwnerId)
	o.WriteText(s.LockName)
	o.WriteText(s.LockOwnerName)
}

func (s *DumpStep) Read(d *protocol.DataInputX) error {
	if err := s.StepSingle.Read(d); err != nil {
		return err
	}

	stacks, err := d.ReadDecimalIntArray()
	if err != nil {
		return err
	}
	s.Stacks = stacks

	threadId, err := d.ReadLong()
	if err != nil {
		return err
	}
	s.ThreadId = threadId

	threadName, err := d.ReadText()
	if err != nil {
		return err
	}
	s.ThreadName = threadName

	threadState, err := d.ReadText()
	if err != nil {
		return err
	}
	s.ThreadState = threadState

	lockOwnerId, err := d.ReadLong()
	if err != nil {
		return err
	}
	s.LockOwnerId = lockOwnerId

	lockName, err := d.ReadText()
	if err != nil {
		return err
	}
	s.LockName = lockName

	lockOwnerName, err := d.ReadText()
	if err != nil {
		return err
	}
	s.LockOwnerName = lockOwnerName

	return nil
}
