package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type DoubleSummary struct {
	Sum   float64
	Count int32
	Min   float64
	Max   float64
}

func (v *DoubleSummary) GetValueType() byte {
	return TYPE_DOUBLE_SUMMARY
}

func (v *DoubleSummary) Write(o *protocol.DataOutputX) {
	o.WriteDouble(v.Sum)
	o.WriteInt(v.Count)
	o.WriteDouble(v.Min)
	o.WriteDouble(v.Max)
}

func (v *DoubleSummary) Read(d *protocol.DataInputX) error {
	sum, err := d.ReadDouble()
	if err != nil {
		return err
	}
	v.Sum = sum

	count, err := d.ReadInt()
	if err != nil {
		return err
	}
	v.Count = count

	min, err := d.ReadDouble()
	if err != nil {
		return err
	}
	v.Min = min

	max, err := d.ReadDouble()
	if err != nil {
		return err
	}
	v.Max = max

	return nil
}
