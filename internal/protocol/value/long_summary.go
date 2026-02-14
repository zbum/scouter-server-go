package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type LongSummary struct {
	Sum   int64
	Count int32
	Min   int64
	Max   int64
}

func (v *LongSummary) ValueType() byte {
	return TYPE_LONG_SUMMARY
}

func (v *LongSummary) Write(o *protocol.DataOutputX) {
	o.WriteInt64(v.Sum)
	o.WriteInt32(v.Count)
	o.WriteInt64(v.Min)
	o.WriteInt64(v.Max)
}

func (v *LongSummary) Read(d *protocol.DataInputX) error {
	sum, err := d.ReadInt64()
	if err != nil {
		return err
	}
	v.Sum = sum

	count, err := d.ReadInt32()
	if err != nil {
		return err
	}
	v.Count = count

	min, err := d.ReadInt64()
	if err != nil {
		return err
	}
	v.Min = min

	max, err := d.ReadInt64()
	if err != nil {
		return err
	}
	v.Max = max

	return nil
}
