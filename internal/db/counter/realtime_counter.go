package counter

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RealtimeCounterData stores per-second counter values for a single day.
// Indexed by composite key: objHash(4B) + timeSec(4B) → data offset.
// Data file stores: [int32:length][byte:tagCount][{int32:counterIdx, Value:val}...]
type RealtimeCounterData struct {
	mu    sync.Mutex
	dir   string
	index *io.IndexKeyFile
	data  *io.RealDataFile
}

func NewRealtimeCounterData(dir string) (*RealtimeCounterData, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	index, err := io.NewIndexKeyFile(filepath.Join(dir, "real"), 1)
	if err != nil {
		return nil, err
	}

	data, err := io.NewRealDataFile(filepath.Join(dir, "real.data"))
	if err != nil {
		index.Close()
		return nil, err
	}

	return &RealtimeCounterData{
		dir:   dir,
		index: index,
		data:  data,
	}, nil
}

// makeKey builds the 8-byte composite key: objHash(4) + timeSec(4).
func makeKey(objHash int32, timeSec int32) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], uint32(objHash))
	binary.BigEndian.PutUint32(key[4:8], uint32(timeSec))
	return key
}

// Write stores counter values for an object at a specific second.
// counters is a map of counterName → Value.
func (r *RealtimeCounterData) Write(objHash int32, timeSec int32, counters map[string]value.Value) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Serialize: [byte:count][{text:name, valueType:byte, value:bytes}...]
	out := protocol.NewDataOutputX()
	out.WriteByte(byte(len(counters)))
	for name, val := range counters {
		out.WriteText(name)
		value.WriteValue(out, val)
	}
	blob := out.ToByteArray()

	// Write length-prefixed data
	dout := protocol.NewDataOutputX()
	dout.WriteInt32(int32(len(blob)))
	dout.Write(blob)

	offset, err := r.data.Write(dout.ToByteArray())
	if err != nil {
		return err
	}

	key := makeKey(objHash, timeSec)
	return r.index.Put(key, protocol.ToBytes5(offset))
}

// Read retrieves counter values for an object at a specific second.
func (r *RealtimeCounterData) Read(objHash int32, timeSec int32) (map[string]value.Value, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := makeKey(objHash, timeSec)
	posBytes, err := r.index.Get(key)
	if err != nil {
		return nil, err
	}
	if posBytes == nil {
		return nil, nil
	}

	offset := protocol.ToLong5(posBytes, 0)
	return r.readAtOffset(offset)
}

func (r *RealtimeCounterData) readAtOffset(offset int64) (map[string]value.Value, error) {
	f, err := os.Open(r.data.Filename())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Read length
	lenBuf := make([]byte, 4)
	if _, err := f.Read(lenBuf); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(lenBuf))

	// Read data
	blob := make([]byte, length)
	if _, err := f.Read(blob); err != nil {
		return nil, err
	}

	din := protocol.NewDataInputX(blob)
	count, err := din.ReadByte()
	if err != nil {
		return nil, err
	}

	result := make(map[string]value.Value, count)
	for i := byte(0); i < count; i++ {
		name, err := din.ReadText()
		if err != nil {
			return nil, err
		}
		val, err := value.ReadValue(din)
		if err != nil {
			return nil, err
		}
		result[name] = val
	}
	return result, nil
}

// ReadRange reads all counter entries for an object within a time range (seconds).
func (r *RealtimeCounterData) ReadRange(objHash int32, startSec, endSec int32, handler func(timeSec int32, counters map[string]value.Value)) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for sec := startSec; sec <= endSec; sec++ {
		key := makeKey(objHash, sec)
		posBytes, err := r.index.Get(key)
		if err != nil {
			continue
		}
		if posBytes == nil {
			continue
		}

		offset := protocol.ToLong5(posBytes, 0)
		counters, err := r.readAtOffset(offset)
		if err != nil {
			continue
		}
		handler(sec, counters)
	}
	return nil
}

func (r *RealtimeCounterData) Flush() error {
	return r.data.Flush()
}

func (r *RealtimeCounterData) Close() {
	r.data.Close()
	r.index.Close()
}
