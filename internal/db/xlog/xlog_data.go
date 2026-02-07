package xlog

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/zbum/scouter-server-go/internal/db/io"
)

// XLogData manages the data file for XLog entries.
type XLogData struct {
	dataFile *io.RealDataFile
	path     string
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

// Write writes an XLog entry as [short:length][bytes:data] and returns the start offset.
func (x *XLogData) Write(data []byte) (int64, error) {
	buf := make([]byte, 2+len(data))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(data)))
	copy(buf[2:], data)
	return x.dataFile.Write(buf)
}

// Read reads an XLog entry from the given offset.
func (x *XLogData) Read(offset int64) ([]byte, error) {
	// Open file for random read access
	f, err := os.Open(x.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Seek to offset
	if _, err := f.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Read 2-byte length
	var lenBuf [2]byte
	if _, err := f.Read(lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint16(lenBuf[:])

	// Read data
	data := make([]byte, length)
	if _, err := f.Read(data); err != nil {
		return nil, err
	}

	return data, nil
}

// Flush flushes buffered data to disk.
func (x *XLogData) Flush() error {
	return x.dataFile.Flush()
}

// Close closes the data file.
func (x *XLogData) Close() {
	if x.dataFile != nil {
		x.dataFile.Close()
	}
}
