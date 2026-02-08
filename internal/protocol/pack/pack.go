package pack

import (
	"fmt"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

// Pack type codes
const (
	PackTypeMap                       = byte(10)
	PackTypeXLog                      = byte(21)
	PackTypeDroppedXLog               = byte(22)
	PackTypeXLogProfile               = byte(26)
	PackTypeXLogProfile2              = byte(27)
	PackTypeSpan                      = byte(31)
	PackTypeSpanContainer             = byte(32)
	PackTypeText                      = byte(50)
	PackTypePerfCounter               = byte(60)
	PackTypePerfStatus                = byte(61)
	PackTypeStack                     = byte(62)
	PackTypeSummary                   = byte(63)
	PackTypeBatch                     = byte(64)
	PackTypePerfInteractionCounter    = byte(65)
	PackTypeAlert                     = byte(70)
	PackTypeObject                    = byte(80)
)

// Pack is the interface that all pack types must implement.
type Pack interface {
	GetPackType() byte
	Write(o *protocol.DataOutputX)
	Read(d *protocol.DataInputX) error
}

// CreatePack creates a pack instance based on the type code.
func CreatePack(typeCode byte) (Pack, error) {
	switch typeCode {
	case PackTypeMap:
		return &MapPack{}, nil
	case PackTypeXLog:
		return &XLogPack{}, nil
	case PackTypeDroppedXLog:
		return &DroppedXLogPack{}, nil
	case PackTypeXLogProfile:
		return &XLogProfilePack{}, nil
	case PackTypeXLogProfile2:
		return &XLogProfilePack2{}, nil
	case PackTypeSpan:
		return &SpanPack{}, nil
	case PackTypeSpanContainer:
		return &SpanContainerPack{}, nil
	case PackTypeText:
		return &TextPack{}, nil
	case PackTypePerfCounter:
		return &PerfCounterPack{}, nil
	case PackTypePerfStatus:
		return &StatusPack{}, nil
	case PackTypeStack:
		return &StackPack{}, nil
	case PackTypeSummary:
		return &SummaryPack{}, nil
	case PackTypeBatch:
		return &BatchPack{}, nil
	case PackTypePerfInteractionCounter:
		return &InteractionPerfCounterPack{}, nil
	case PackTypeAlert:
		return &AlertPack{}, nil
	case PackTypeObject:
		return &ObjectPack{}, nil
	default:
		return nil, fmt.Errorf("unknown pack type: %d", typeCode)
	}
}

// WritePack writes a pack to the output stream with its type code.
func WritePack(o *protocol.DataOutputX, p Pack) {
	o.WriteByte(p.GetPackType())
	p.Write(o)
}

// ReadPack reads a pack from the input stream.
func ReadPack(d *protocol.DataInputX) (Pack, error) {
	typeCode, err := d.ReadByte()
	if err != nil {
		return nil, err
	}

	pack, err := CreatePack(typeCode)
	if err != nil {
		return nil, err
	}

	if err := pack.Read(d); err != nil {
		return nil, fmt.Errorf("packType=%d: %w", typeCode, err)
	}

	return pack, nil
}
