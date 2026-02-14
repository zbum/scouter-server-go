package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type DoubleSummary struct {
	Sum   float64
	Count int32
	Min   float64
	Max   float64
}

func (v *DoubleSummary) ValueType() byte {
	return TYPE_DOUBLE_SUMMARY
}

func (v *DoubleSummary) Write(o *protocol.DataOutputX) {
	o.WriteFloat64(v.Sum)
	o.WriteInt32(v.Count)
	o.WriteFloat64(v.Min)
	o.WriteFloat64(v.Max)
}

func (v *DoubleSummary) Read(d *protocol.DataInputX) error {
	sum, err := d.ReadFloat64()
	if err != nil {
		return err
	}
	v.Sum = sum

	count, err := d.ReadInt32()
	if err != nil {
		return err
	}
	v.Count = count

	min, err := d.ReadFloat64()
	if err != nil {
		return err
	}
	v.Min = min

	max, err := d.ReadFloat64()
	if err != nil {
		return err
	}
	v.Max = max

	return nil
}
