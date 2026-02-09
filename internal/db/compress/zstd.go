package compress

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	flagNewFormat byte = 0x00
	compTypeRaw   byte = 0x00
	compTypeZstd  byte = 0x01
)

// Pool provides goroutine-safe zstd compression/decompression.
type Pool struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewPool creates a new compression pool with reusable encoder/decoder.
func NewPool() (*Pool, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
	)
	if err != nil {
		return nil, err
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		enc.Close()
		return nil, err
	}

	return &Pool{encoder: enc, decoder: dec}, nil
}

// Compress compresses data and returns [0x00][0x01][zstd payload].
func (p *Pool) Compress(data []byte) []byte {
	compressed := p.encoder.EncodeAll(data, nil)
	out := make([]byte, 2+len(compressed))
	out[0] = flagNewFormat
	out[1] = compTypeZstd
	copy(out[2:], compressed)
	return out
}

// Decode detects the format and decompresses if needed.
//   - body[0] == 0x00 → new format: body[1] selects codec
//   - body[0] != 0x00 → legacy raw data, returned as-is
func (p *Pool) Decode(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return body, nil
	}

	if body[0] != flagNewFormat {
		return body, nil
	}

	if len(body) < 2 {
		return body, nil
	}

	switch body[1] {
	case compTypeRaw:
		return body[2:], nil
	case compTypeZstd:
		return p.decoder.DecodeAll(body[2:], nil)
	default:
		return body[2:], nil
	}
}

// Close releases encoder and decoder resources.
// NOTE: Do not call Close on the shared singleton pool.
func (p *Pool) Close() {
	if p.encoder != nil {
		p.encoder.Close()
	}
	if p.decoder != nil {
		p.decoder.Close()
	}
}

var (
	sharedPool *Pool
	sharedOnce sync.Once
)

// SharedPool returns a process-wide singleton Pool.
// EncodeAll/DecodeAll are goroutine-safe, so a single Pool is sufficient.
func SharedPool() *Pool {
	sharedOnce.Do(func() {
		p, err := NewPool()
		if err != nil {
			panic("compress: failed to initialize shared pool: " + err.Error())
		}
		sharedPool = p
	})
	return sharedPool
}
