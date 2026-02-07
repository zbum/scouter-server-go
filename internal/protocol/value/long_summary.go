package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type LongSummary struct {
	Sum   int64
	Count int32
	Min   int64
	Max   int64
}

func (v *LongSummary) GetValueType() byte {
	return TYPE_LONG_SUMMARY
}

func (v *LongSummary) Write(o *protocol.DataOutputX) {
	o.WriteLong(v.Sum)
	o.WriteInt(v.Count)
	o.WriteLong(v.Min)
	o.WriteLong(v.Max)
}

func (v *LongSummary) Read(d *protocol.DataInputX) error {
	sum, err := d.ReadLong()
	if err != nil {
		return err
	}
	v.Sum = sum

	count, err := d.ReadInt()
	if err != nil {
		return err
	}
	v.Count = count

	min, err := d.ReadLong()
	if err != nil {
		return err
	}
	v.Min = min

	max, err := d.ReadLong()
	if err != nil {
		return err
	}
	v.Max = max

	return nil
}
