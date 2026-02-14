package xlog

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db/compress"
	"github.com/zbum/scouter-server-go/internal/db/io"
)

// XLogData manages the data file for XLog entries.
type XLogData struct {
	dataFile *io.RealDataFile
	path     string
	initOnce sync.Once // ensures raf is opened exactly once
	initErr  error     // error from lazy init, if any
	raf      *os.File  // read-only handle for ReadAt (pread), lazily initialized
}

// NewXLogData opens the XLog data file.
func NewXLogData(dir string) (*XLogData, error) {
	path := filepath.Join(dir, "xlog.data")
	dataFile, err := io.NewRealDataFile(path)
	if err != nil {
		return nil, err
	}

	return &XLogData{
		dataFile: dataFile,
		path:     path,
	}, nil
}

// Write writes an XLog entry as [short:length][bytes:body] and returns the start offset.
// If compression is enabled, body = [0x00][0x01][zstd payload]; otherwise body = raw data.
func (x *XLogData) Write(data []byte) (int64, error) {
	body := data
	if cfg := config.Get(); cfg != nil && cfg.CompressXLogEnabled() {
		body = compress.SharedPool().Compress(data)
	}
	buf := make([]byte, 2+len(body))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(body)))
	copy(buf[2:], body)
	return x.dataFile.Write(buf)
}

// bodyPool reuses read buffers for compressed data to reduce GC pressure.
// When compression is enabled, the read buffer is temporary (Decode produces
// a new decompressed buffer), so the read buffer can be recycled.
var bodyPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return b
	},
}

// Read reads an XLog entry from the given offset.
// Uses ReadAt (pread) for lock-free concurrent reads — multiple goroutines
// can read simultaneously without mutex serialization.
func (x *XLogData) Read(offset int64) ([]byte, error) {
	x.initOnce.Do(func() {
		f, err := os.Open(x.path)
		if err != nil {
			x.initErr = err
			return
		}
		x.raf = f
	})
	if x.initErr != nil {
		return nil, x.initErr
	}

	// Read length header (2 bytes) via pread — no seek, no lock needed
	var lenBuf [2]byte
	if _, err := x.raf.ReadAt(lenBuf[:], offset); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint16(lenBuf[:]))

	// Get read buffer from pool
	body := bodyPool.Get().([]byte)
	if cap(body) >= length {
		body = body[:length]
	} else {
		body = make([]byte, length)
	}

	// Read body via pread
	if _, err := x.raf.ReadAt(body, offset+2); err != nil {
		bodyPool.Put(body[:0])
		return nil, err
	}

	decoded, err := compress.SharedPool().Decode(body)
	if err != nil {
		bodyPool.Put(body[:0])
		return nil, err
	}

	// Recycle the read buffer only if Decode produced a new buffer (compressed case).
	// When uncompressed, decoded IS body — must not return it to the pool.
	if len(decoded) > 0 && len(body) > 0 && &decoded[0] != &body[0] {
		bodyPool.Put(body[:0])
	}

	return decoded, nil
}

// Flush flushes buffered data to disk.
func (x *XLogData) Flush() error {
	return x.dataFile.Flush()
}

// Close closes the data file and the read handle.
func (x *XLogData) Close() {
	if x.raf != nil {
		x.raf.Close()
		x.raf = nil
	}

	if x.dataFile != nil {
		x.dataFile.Close()
	}
}
