package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type MapEntry struct {
	Key   string
	Value Value
}

type MapValue struct {
	Entries []MapEntry
}

func NewMapValue() *MapValue {
	return &MapValue{Entries: make([]MapEntry, 0)}
}

func (v *MapValue) GetValueType() byte {
	return TYPE_MAP
}

func (v *MapValue) Write(o *protocol.DataOutputX) {
	if v.Entries == nil {
		o.WriteDecimal(0)
	} else {
		o.WriteDecimal(int64(len(v.Entries)))
		for _, entry := range v.Entries {
			o.WriteText(entry.Key)
			WriteValue(o, entry.Value)
		}
	}
}

func (v *MapValue) Read(d *protocol.DataInputX) error {
	count, err := d.ReadDecimal()
	if err != nil {
		return err
	}
	v.Entries = make([]MapEntry, count)
	for i := int64(0); i < count; i++ {
		key, err := d.ReadText()
		if err != nil {
			return err
		}
		val, err := ReadValue(d)
		if err != nil {
			return err
		}
		v.Entries[i] = MapEntry{Key: key, Value: val}
	}
	return nil
}

// Helper methods for map operations
func (v *MapValue) Put(key string, value Value) {
	for i := range v.Entries {
		if v.Entries[i].Key == key {
			v.Entries[i].Value = value
			return
		}
	}
	v.Entries = append(v.Entries, MapEntry{Key: key, Value: value})
}

func (v *MapValue) Get(key string) (Value, bool) {
	for _, entry := range v.Entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return nil, false
}

func (v *MapValue) Size() int {
	return len(v.Entries)
}
