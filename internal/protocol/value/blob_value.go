package value

import "github.com/zbum/scouter-server-go/internal/protocol"

type BlobValue struct {
	Value []byte
}

func (v *BlobValue) ValueType() byte {
	return TYPE_BLOB
}

func (v *BlobValue) Write(o *protocol.DataOutputX) {
	o.WriteBlob(v.Value)
}

func (v *BlobValue) Read(d *protocol.DataInputX) error {
	val, err := d.ReadBlob()
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}
