package pack

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// MapEntry represents a key-value entry in the map.
type MapEntry struct {
	Key string
	Val value.Value
}

// MapPack represents a map data structure.
type MapPack struct {
	Table []MapEntry
}

// GetPackType returns the pack type code.
func (p *MapPack) GetPackType() byte {
	return PackTypeMap
}

// Write serializes the MapPack to the output stream.
func (p *MapPack) Write(o *protocol.DataOutputX) {
	count := int64(len(p.Table))
	o.WriteDecimal(count)
	for _, entry := range p.Table {
		o.WriteText(entry.Key)
		value.WriteValue(o, entry.Val)
	}
}

// Read deserializes the MapPack from the input stream.
func (p *MapPack) Read(d *protocol.DataInputX) error {
	count, err := d.ReadDecimal()
	if err != nil {
		return err
	}

	p.Table = make([]MapEntry, count)
	for i := int64(0); i < count; i++ {
		key, err := d.ReadText()
		if err != nil {
			return err
		}

		val, err := value.ReadValue(d)
		if err != nil {
			return err
		}

		p.Table[i] = MapEntry{Key: key, Val: val}
	}

	return nil
}

// Put adds or updates a key-value pair.
func (p *MapPack) Put(key string, v value.Value) {
	// Check if key exists
	for i := range p.Table {
		if p.Table[i].Key == key {
			p.Table[i].Val = v
			return
		}
	}
	// Add new entry
	p.Table = append(p.Table, MapEntry{Key: key, Val: v})
}

// Get retrieves a value by key.
func (p *MapPack) Get(key string) value.Value {
	for _, entry := range p.Table {
		if entry.Key == key {
			return entry.Val
		}
	}
	return nil
}

// PutStr adds a string value.
func (p *MapPack) PutStr(key, val string) {
	p.Put(key, value.NewTextValue(val))
}

// PutLong adds a long value.
func (p *MapPack) PutLong(key string, val int64) {
	p.Put(key, value.NewDecimalValue(val))
}

// GetText retrieves a string value by key.
func (p *MapPack) GetText(key string) string {
	v := p.Get(key)
	if v == nil {
		return ""
	}
	if tv, ok := v.(*value.TextValue); ok {
		return tv.Value
	}
	return ""
}

// GetInt retrieves an int32 value by key.
func (p *MapPack) GetInt(key string) int32 {
	v := p.Get(key)
	if v == nil {
		return 0
	}
	if dv, ok := v.(*value.DecimalValue); ok {
		return int32(dv.Value)
	}
	return 0
}

// GetLong retrieves an int64 value by key.
func (p *MapPack) GetLong(key string) int64 {
	v := p.Get(key)
	if v == nil {
		return 0
	}
	if dv, ok := v.(*value.DecimalValue); ok {
		return dv.Value
	}
	return 0
}

// GetList retrieves a ListValue by key.
func (p *MapPack) GetList(key string) *value.ListValue {
	v := p.Get(key)
	if v == nil {
		return nil
	}
	if lv, ok := v.(*value.ListValue); ok {
		return lv
	}
	return nil
}

// GetBoolean retrieves a boolean value by key.
func (p *MapPack) GetBoolean(key string) bool {
	v := p.Get(key)
	if v == nil {
		return false
	}
	if bv, ok := v.(*value.BooleanValue); ok {
		return bv.Value
	}
	return false
}

// Size returns the number of entries in the map.
func (p *MapPack) Size() int {
	return len(p.Table)
}
