package text

import (
	"encoding/binary"
	"os"
	"sync"
)

// TextPermData manages a .data file for permanent text storage.
// Record format: [4B length (big-endian)][length bytes of text data]
// This matches Java's scouter.server.db.text.TextPermData.
type TextPermData struct {
	mu   sync.Mutex
	file *os.File
}

// NewTextPermData opens or creates a .data file at the given path.
func NewTextPermData(path string) (*TextPermData, error) {
	f, err := os.OpenFile(path+".data", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &TextPermData{file: f}, nil
}

// Write appends text data and returns the file position where it was written.
func (d *TextPermData) Write(data []byte) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	fi, err := d.file.Stat()
	if err != nil {
		return 0, err
	}
	pos := fi.Size()

	if _, err := d.file.Seek(pos, 0); err != nil {
		return 0, err
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := d.file.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	if _, err := d.file.Write(data); err != nil {
		return 0, err
	}

	return pos, nil
}

// Read reads text data at the given file position.
func (d *TextPermData) Read(pos int64) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, err := d.file.Seek(pos, 0); err != nil {
		return nil, err
	}

	var lenBuf [4]byte
	if _, err := d.file.Read(lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])

	buf := make([]byte, length)
	if _, err := d.file.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Close closes the data file.
func (d *TextPermData) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		d.file.Close()
		d.file = nil
	}
}
