package io

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

const (
	kfileHeaderSize    = 2     // 0xCA 0xFE
	appendBufThreshold = 16384 // 16KB - auto-flush threshold for buffered appends
)

// KeyRecord represents a single record in the key file chain.
type KeyRecord struct {
	Deleted bool
	PrevPos int64
	TimeKey []byte
	DataPos []byte
	Offset  int64 // file offset after this record
}

// RealKeyFile is a hash chain index file (.kfile) with sequential record storage.
// Record format: [1B deleted][5B prevPos][2B keyLen][keyLen B key][blob dataPos]
//
// Append operations are buffered in memory and flushed to disk either when the
// buffer exceeds appendBufThreshold or before any read/positional-write operation.
// This reduces disk I/O significantly under high write load.
type RealKeyFile struct {
	mu        sync.RWMutex
	path      string
	file      string
	raf       *os.File
	appendBuf []byte // buffered append data
	fileEnd   int64  // actual file size on disk (excludes buffered data)
}

func NewRealKeyFile(path string) (*RealKeyFile, error) {
	f := &RealKeyFile{
		path:      path,
		file:      path + ".kfile",
		appendBuf: make([]byte, 0, appendBufThreshold),
	}
	if err := f.open(); err != nil {
		return nil, err
	}
	GetFlushController().Register(f)
	return f, nil
}

func (f *RealKeyFile) open() error {
	raf, err := os.OpenFile(f.file, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	f.raf = raf

	fi, err := raf.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		_, err = raf.Write([]byte{0xCA, 0xFE})
		if err != nil {
			return err
		}
		f.fileEnd = kfileHeaderSize
	} else {
		f.fileEnd = fi.Size()
	}
	return nil
}

// flushAppendBuf writes buffered append data to the end of the file.
// Must be called with mu held.
func (f *RealKeyFile) flushAppendBuf() error {
	if len(f.appendBuf) == 0 {
		return nil
	}
	if _, err := f.raf.Seek(f.fileEnd, 0); err != nil {
		return err
	}
	n, err := f.raf.Write(f.appendBuf)
	if err != nil {
		return err
	}
	f.fileEnd += int64(n)
	f.appendBuf = f.appendBuf[:0]
	return nil
}

// Flush implements IFlushable. Writes buffered data to disk.
func (f *RealKeyFile) Flush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.flushAppendBuf()
}

// IsDirty implements IFlushable.
func (f *RealKeyFile) IsDirty() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.appendBuf) > 0
}

// Interval implements IFlushable.
func (f *RealKeyFile) Interval() time.Duration {
	return 2 * time.Second
}

// GetRecord reads a complete record at the given position.
// For on-disk positions (pos < fileEnd), uses ReadAt without flushing
// or exclusive locking, enabling concurrent reads from multiple goroutines.
// For buffered positions, flushes first under exclusive lock.
func (f *RealKeyFile) GetRecord(pos int64) (*KeyRecord, error) {
	f.mu.RLock()
	onDisk := pos < f.fileEnd
	f.mu.RUnlock()

	if onDisk {
		return f.getRecordReadAt(pos)
	}

	// Data might be in appendBuf — flush first under exclusive lock.
	f.mu.Lock()
	if err := f.flushAppendBuf(); err != nil {
		f.mu.Unlock()
		return nil, err
	}
	f.mu.Unlock()
	return f.getRecordReadAt(pos)
}

// getRecordReadAt reads a complete record using a single ReadAt call.
// Replaces the old getRecordInternal which used 7+ separate Seek+Read syscalls.
// ReadAt (pread) is thread-safe and doesn't require holding any lock.
func (f *RealKeyFile) getRecordReadAt(pos int64) (*KeyRecord, error) {
	// Read a generous buffer — covers 99%+ of records.
	// Typical XLog time record: 1+5+2+8+1+5 = 22 bytes.
	var buf [128]byte
	n, err := f.raf.ReadAt(buf[:], pos)
	if n < 8 { // minimum: 1(del) + 5(prevPos) + 2(keyLen)
		if err != nil {
			return nil, err
		}
		return nil, errors.New("record header too short")
	}

	r := &KeyRecord{}
	off := 0

	// deleted (1 byte)
	r.Deleted = buf[off] != 0
	off++

	// prevPos (5 bytes)
	r.PrevPos = protocol.ToLong5(buf[off:off+5], 0)
	off += 5

	// keyLen (2 bytes) + key
	keyLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
	off += 2

	if off+keyLen+1 > n { // key + at least blob prefix byte
		return nil, errors.New("record extends beyond read buffer")
	}
	r.TimeKey = make([]byte, keyLen)
	copy(r.TimeKey, buf[off:off+keyLen])
	off += keyLen

	// blob prefix (1 byte)
	blobPrefix := int(buf[off]) & 0xFF
	off++

	// Determine blob data length
	var blobLen int
	switch blobPrefix {
	case 0:
		r.DataPos = []byte{}
		r.Offset = pos + int64(off)
		return r, nil
	case 255:
		if off+2 > n {
			return nil, errors.New("blob length header truncated")
		}
		blobLen = int(binary.BigEndian.Uint16(buf[off : off+2]))
		off += 2
	case 254:
		if off+4 > n {
			return nil, errors.New("blob length header truncated")
		}
		blobLen = int(binary.BigEndian.Uint32(buf[off : off+4]))
		off += 4
	default:
		blobLen = blobPrefix
	}

	// Read blob data
	r.DataPos = make([]byte, blobLen)
	if off+blobLen <= n {
		// Common case: everything fits in the initial buffer read
		copy(r.DataPos, buf[off:off+blobLen])
	} else {
		// Rare case: blob extends beyond initial buffer (e.g. large text values)
		copied := 0
		if off < n {
			copied = n - off
			copy(r.DataPos[:copied], buf[off:n])
		}
		if copied < blobLen {
			if _, err := f.raf.ReadAt(r.DataPos[copied:], pos+int64(n)); err != nil {
				return nil, err
			}
		}
	}
	off += blobLen

	r.Offset = pos + int64(off)
	return r, nil
}

func (f *RealKeyFile) readBlob() ([]byte, error) {
	var b [1]byte
	if _, err := f.raf.Read(b[:]); err != nil {
		return nil, err
	}
	baseLen := int(b[0]) & 0xFF

	switch baseLen {
	case 0:
		return []byte{}, nil
	case 255:
		var lenBuf [2]byte
		if _, err := f.raf.Read(lenBuf[:]); err != nil {
			return nil, err
		}
		length := int(binary.BigEndian.Uint16(lenBuf[:]))
		data := make([]byte, length)
		if _, err := f.raf.Read(data); err != nil {
			return nil, err
		}
		return data, nil
	case 254:
		var lenBuf [4]byte
		if _, err := f.raf.Read(lenBuf[:]); err != nil {
			return nil, err
		}
		length := int(binary.BigEndian.Uint32(lenBuf[:]))
		data := make([]byte, length)
		if _, err := f.raf.Read(data); err != nil {
			return nil, err
		}
		return data, nil
	default:
		data := make([]byte, baseLen)
		if _, err := f.raf.Read(data); err != nil {
			return nil, err
		}
		return data, nil
	}
}

func (f *RealKeyFile) IsDeleted(pos int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return false, err
	}
	if _, err := f.raf.Seek(pos, 0); err != nil {
		return false, err
	}
	var b [1]byte
	if _, err := f.raf.Read(b[:]); err != nil {
		return false, err
	}
	return b[0] != 0, nil
}

func (f *RealKeyFile) GetPrevPos(pos int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return 0, err
	}
	if _, err := f.raf.Seek(pos+1, 0); err != nil {
		return 0, err
	}
	var buf [5]byte
	if _, err := f.raf.Read(buf[:]); err != nil {
		return 0, err
	}
	return protocol.ToLong5(buf[:], 0), nil
}

func (f *RealKeyFile) GetTimeKey(pos int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return nil, err
	}
	if _, err := f.raf.Seek(pos+1+5, 0); err != nil {
		return nil, err
	}
	var lenBuf [2]byte
	if _, err := f.raf.Read(lenBuf[:]); err != nil {
		return nil, err
	}
	keyLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	data := make([]byte, keyLen)
	if keyLen > 0 {
		if _, err := f.raf.Read(data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (f *RealKeyFile) GetDataPos(pos int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return nil, err
	}
	if _, err := f.raf.Seek(pos+1+5, 0); err != nil {
		return nil, err
	}
	// Skip key: read 2B length, skip keyLen bytes
	var lenBuf [2]byte
	if _, err := f.raf.Read(lenBuf[:]); err != nil {
		return nil, err
	}
	keyLen := int64(binary.BigEndian.Uint16(lenBuf[:]))
	if _, err := f.raf.Seek(keyLen, 1); err != nil {
		return nil, err
	}
	return f.readBlob()
}

func (f *RealKeyFile) SetDelete(pos int64, deleted bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	if _, err := f.raf.Seek(pos, 0); err != nil {
		return err
	}
	b := byte(0)
	if deleted {
		b = 1
	}
	_, err := f.raf.Write([]byte{b})
	return err
}

func (f *RealKeyFile) SetHashLink(pos int64, value int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	if _, err := f.raf.Seek(pos+1, 0); err != nil {
		return err
	}
	_, err := f.raf.Write(protocol.ToBytes5(value))
	return err
}

// Write writes a full record at the given position.
func (f *RealKeyFile) Write(pos int64, prevPos int64, indexKey []byte, dataPos []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	return f.writeInternal(pos, prevPos, indexKey, dataPos)
}

func (f *RealKeyFile) writeInternal(pos int64, prevPos int64, indexKey []byte, dataPos []byte) error {
	if _, err := f.raf.Seek(pos, 0); err != nil {
		return err
	}

	o := protocol.NewDataOutputX()
	o.WriteBoolean(false)
	o.WriteLong5(prevPos)
	o.WriteShortBytes(indexKey)
	o.WriteBlob(dataPos)

	_, err := f.raf.Write(o.ToByteArray())
	return err
}

// Update attempts to update the dataPos for a record at pos.
// Returns false if the new value is larger than the existing one.
func (f *RealKeyFile) Update(pos int64, key []byte, value []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return false, err
	}

	if _, err := f.raf.Seek(pos+1+5, 0); err != nil {
		return false, err
	}

	// Read key length and skip key
	var lenBuf [2]byte
	if _, err := f.raf.Read(lenBuf[:]); err != nil {
		return false, err
	}
	keyLen := int64(binary.BigEndian.Uint16(lenBuf[:]))
	if _, err := f.raf.Seek(keyLen, 1); err != nil {
		return false, err
	}

	// Read existing blob to check size
	org, err := f.readBlob()
	if err != nil {
		return false, err
	}
	if len(org) < len(value) {
		return false, nil
	}

	// Seek back to write position
	if _, err := f.raf.Seek(pos+1+5+2+keyLen, 0); err != nil {
		return false, err
	}

	o := protocol.NewDataOutputX()
	o.WriteBlob(value)
	_, err = f.raf.Write(o.ToByteArray())
	return err == nil, err
}

// Append writes a new record at the end of the file and returns the position.
// The data is buffered in memory and flushed when the buffer exceeds the threshold
// or before the next read/positional-write operation.
func (f *RealKeyFile) Append(prevPos int64, indexKey []byte, dataPos []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	pos := f.fileEnd + int64(len(f.appendBuf))

	o := protocol.NewDataOutputX()
	o.WriteBoolean(false)
	o.WriteLong5(prevPos)
	o.WriteShortBytes(indexKey)
	o.WriteBlob(dataPos)

	f.appendBuf = append(f.appendBuf, o.ToByteArray()...)

	if len(f.appendBuf) >= appendBufThreshold {
		if err := f.flushAppendBuf(); err != nil {
			return 0, err
		}
	}

	return pos, nil
}

func (f *RealKeyFile) GetFirstPos() int64 {
	return kfileHeaderSize
}

func (f *RealKeyFile) GetLength() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fileEnd + int64(len(f.appendBuf))
}

func (f *RealKeyFile) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.flushAppendBuf()
	GetFlushController().Unregister(f)
	if f.raf != nil {
		f.raf.Close()
		f.raf = nil
	}
}
