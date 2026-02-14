package io

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

// KeyRecord2 represents a single record in the v2 key file chain (with TTL support).
type KeyRecord2 struct {
	Deleted bool
	Expire  int64 // unix timestamp in seconds
	PrevPos int64
	TimeKey []byte
	DataPos []byte
	Offset  int64 // file offset after this record
}

// RealKeyFile2 is a hash chain index file (.k2file) with TTL support.
// Record format: [1B deleted][5B expire][5B prevPos][2B keyLen][keyLen B key][blob dataPos]
//
// Append operations are buffered in memory and flushed to disk either when the
// buffer exceeds appendBufThreshold or before any read/positional-write operation.
type RealKeyFile2 struct {
	mu        sync.RWMutex
	path      string
	file      string
	raf       *os.File
	appendBuf []byte
	fileEnd   int64
}

func NewRealKeyFile2(path string) (*RealKeyFile2, error) {
	f := &RealKeyFile2{
		path:      path,
		file:      path + ".k2file",
		appendBuf: make([]byte, 0, appendBufThreshold),
	}
	if err := f.open(); err != nil {
		return nil, err
	}
	GetFlushController().Register(f)
	return f, nil
}

func (f *RealKeyFile2) open() error {
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

func (f *RealKeyFile2) flushAppendBuf() error {
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

// Flush implements IFlushable.
func (f *RealKeyFile2) Flush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.flushAppendBuf()
}

// IsDirty implements IFlushable.
func (f *RealKeyFile2) IsDirty() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.appendBuf) > 0
}

// Interval implements IFlushable.
func (f *RealKeyFile2) Interval() time.Duration {
	return 2 * time.Second
}

// GetRecord reads a complete record at the given position.
func (f *RealKeyFile2) GetRecord(pos int64) (*KeyRecord2, error) {
	f.mu.RLock()
	onDisk := pos < f.fileEnd
	f.mu.RUnlock()

	if onDisk {
		return f.getRecordReadAt(pos)
	}

	f.mu.Lock()
	if err := f.flushAppendBuf(); err != nil {
		f.mu.Unlock()
		return nil, err
	}
	f.mu.Unlock()
	return f.getRecordReadAt(pos)
}

// getRecordReadAt reads a complete v2 record using a single ReadAt call.
func (f *RealKeyFile2) getRecordReadAt(pos int64) (*KeyRecord2, error) {
	var buf [128]byte
	n, err := f.raf.ReadAt(buf[:], pos)
	if n < 13 { // minimum: 1(del) + 5(expire) + 5(prevPos) + 2(keyLen)
		if err != nil {
			return nil, err
		}
		return nil, errors.New("record header too short")
	}

	r := &KeyRecord2{}
	off := 0

	// deleted (1 byte)
	r.Deleted = buf[off] != 0
	off++

	// expire (5 bytes)
	r.Expire = protocol.BigEndian.Int5(buf[off : off+5])
	off += 5

	// prevPos (5 bytes)
	r.PrevPos = protocol.BigEndian.Int5(buf[off : off+5])
	off += 5

	// keyLen (2 bytes) + key
	keyLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
	off += 2

	if off+keyLen+1 > n {
		return nil, errors.New("record extends beyond read buffer")
	}
	r.TimeKey = make([]byte, keyLen)
	copy(r.TimeKey, buf[off:off+keyLen])
	off += keyLen

	// blob prefix (1 byte)
	blobPrefix := int(buf[off])
	off++

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

	r.DataPos = make([]byte, blobLen)
	if off+blobLen <= n {
		copy(r.DataPos, buf[off:off+blobLen])
	} else {
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

func (f *RealKeyFile2) IsDeleted(pos int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return false, err
	}
	var b [1]byte
	if _, err := f.raf.ReadAt(b[:], pos); err != nil {
		return false, err
	}
	return b[0] != 0, nil
}

// IsExpired checks if the record at pos has expired.
func (f *RealKeyFile2) IsExpired(pos int64) (bool, error) {
	expire, err := f.GetExpire(pos)
	if err != nil {
		return false, err
	}
	return expire < time.Now().Unix(), nil
}

// IsDeletedOrExpired checks both deleted and expired flags in a single read.
func (f *RealKeyFile2) IsDeletedOrExpired(pos int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return false, err
	}
	var buf [6]byte // 1(deleted) + 5(expire)
	if _, err := f.raf.ReadAt(buf[:], pos); err != nil {
		return false, err
	}
	deleted := buf[0] != 0
	expire := protocol.BigEndian.Int5(buf[1:6])
	return deleted || expire < time.Now().Unix(), nil
}

// GetExpire reads the expire timestamp at pos+1.
func (f *RealKeyFile2) GetExpire(pos int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return 0, err
	}
	var buf [5]byte
	if _, err := f.raf.ReadAt(buf[:], pos+1); err != nil {
		return 0, err
	}
	return protocol.BigEndian.Int5(buf[:]), nil
}

// GetTTL returns the remaining TTL in seconds.
func (f *RealKeyFile2) GetTTL(pos int64) (int64, error) {
	expire, err := f.GetExpire(pos)
	if err != nil {
		return 0, err
	}
	return expire - time.Now().Unix(), nil
}

// SetTTL sets the TTL on a record. If ttl < 0, sets expire to LONG5MaxValue (infinite).
func (f *RealKeyFile2) SetTTL(pos int64, ttlSec int64) error {
	var expire int64
	if ttlSec < 0 {
		expire = protocol.LONG5MaxValue
	} else {
		expire = time.Now().Unix() + ttlSec
	}
	return f.SetExpire(pos, expire)
}

// SetExpire writes the expire unix timestamp at pos+1.
func (f *RealKeyFile2) SetExpire(pos int64, expireUnixTimestamp int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	if _, err := f.raf.Seek(pos+1, 0); err != nil {
		return err
	}
	_, err := f.raf.Write(protocol.BigEndian.Bytes5(expireUnixTimestamp))
	return err
}

func (f *RealKeyFile2) GetPrevPos(pos int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return 0, err
	}
	var buf [5]byte
	if _, err := f.raf.ReadAt(buf[:], pos+1+5); err != nil {
		return 0, err
	}
	return protocol.BigEndian.Int5(buf[:]), nil
}

func (f *RealKeyFile2) GetKey(pos int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return nil, err
	}
	if _, err := f.raf.Seek(pos+1+5+5, 0); err != nil {
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

func (f *RealKeyFile2) GetDataPos(pos int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return nil, err
	}
	if _, err := f.raf.Seek(pos+1+5+5, 0); err != nil {
		return nil, err
	}
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

func (f *RealKeyFile2) readBlob() ([]byte, error) {
	var b [1]byte
	if _, err := f.raf.Read(b[:]); err != nil {
		return nil, err
	}
	baseLen := int(b[0])

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

func (f *RealKeyFile2) SetDelete(pos int64, deleted bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	b := byte(0)
	if deleted {
		b = 1
	}
	if _, err := f.raf.Seek(pos, 0); err != nil {
		return err
	}
	_, err := f.raf.Write([]byte{b})
	return err
}

func (f *RealKeyFile2) SetHashLink(pos int64, value int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	if _, err := f.raf.Seek(pos+1+5, 0); err != nil {
		return err
	}
	_, err := f.raf.Write(protocol.BigEndian.Bytes5(value))
	return err
}

// Write writes a full record at the given position with infinite TTL.
func (f *RealKeyFile2) Write(pos int64, prevPos int64, indexKey []byte, dataPos []byte) error {
	return f.WriteTTL(pos, -1, prevPos, indexKey, dataPos)
}

// WriteTTL writes a full record at the given position with TTL.
func (f *RealKeyFile2) WriteTTL(pos int64, ttl int64, prevPos int64, indexKey []byte, dataPos []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return err
	}
	return f.writeInternalTTL(pos, ttl, prevPos, indexKey, dataPos)
}

func (f *RealKeyFile2) writeInternalTTL(pos int64, ttl int64, prevPos int64, indexKey []byte, dataPos []byte) error {
	if _, err := f.raf.Seek(pos, 0); err != nil {
		return err
	}

	var expire int64
	if ttl < 0 {
		expire = protocol.LONG5MaxValue
	} else {
		expire = time.Now().Unix() + ttl
	}

	o := protocol.NewDataOutputX()
	o.WriteBoolean(false)
	o.WriteLong5(expire)
	o.WriteLong5(prevPos)
	o.WriteShortBytes(indexKey)
	o.WriteBlob(dataPos)

	_, err := f.raf.Write(o.ToByteArray())
	return err
}

// Update attempts to update a record at pos with new TTL and value.
// Returns false if the new value is larger than the existing one.
func (f *RealKeyFile2) Update(pos int64, ttl int64, key []byte, value []byte) (bool, error) {
	if pos < 0 {
		return false, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.flushAppendBuf(); err != nil {
		return false, err
	}

	// Skip deleted(1) + expire(5), read prevPos and check blob size
	if _, err := f.raf.Seek(pos+1+5, 0); err != nil {
		return false, err
	}

	// Read prevPos
	var prevBuf [5]byte
	if _, err := f.raf.Read(prevBuf[:]); err != nil {
		return false, err
	}
	prevPos := protocol.BigEndian.Int5(prevBuf[:])

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

	// Rewrite the entire record with new TTL
	return true, f.writeInternalTTL(pos, ttl, prevPos, key, value)
}

// Append writes a new record at the end of the file with infinite TTL.
func (f *RealKeyFile2) Append(prevPos int64, indexKey []byte, dataPos []byte) (int64, error) {
	return f.AppendTTL(prevPos, -1, indexKey, dataPos)
}

// AppendTTL writes a new record at the end of the file with TTL.
func (f *RealKeyFile2) AppendTTL(prevPos int64, ttl int64, indexKey []byte, dataPos []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	pos := f.fileEnd + int64(len(f.appendBuf))

	var expire int64
	if ttl < 0 {
		expire = protocol.LONG5MaxValue
	} else {
		expire = time.Now().Unix() + ttl
	}

	o := protocol.NewDataOutputX()
	o.WriteBoolean(false)
	o.WriteLong5(expire)
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

func (f *RealKeyFile2) FirstPos() int64 {
	return kfileHeaderSize
}

func (f *RealKeyFile2) Length() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fileEnd + int64(len(f.appendBuf))
}

func (f *RealKeyFile2) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.flushAppendBuf()
	GetFlushController().Unregister(f)
	if f.raf != nil {
		f.raf.Close()
		f.raf = nil
	}
}
