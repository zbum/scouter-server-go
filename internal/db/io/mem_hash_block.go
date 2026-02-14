package io

import (
	"os"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

const (
	memHeadReserved = 1024
	keyLength       = 5
)

// MemHashBlock is an in-memory hash bucket table backed by a .hfile on disk.
// It stores 5-byte (long5) values in a hash-addressed bucket array.
type MemHashBlock struct {
	mu       sync.Mutex
	path     string
	file     string
	buf      []byte
	bufSize  int
	count    int
	capacity int
	dirty    bool
}

func NewMemHashBlock(path string, memSize int) (*MemHashBlock, error) {
	m := &MemHashBlock{
		path:    path,
		file:    path + ".hfile",
		bufSize: memSize,
	}
	if err := m.open(); err != nil {
		return nil, err
	}
	GetFlushController().Register(m)
	return m, nil
}

func (m *MemHashBlock) open() error {
	fi, err := os.Stat(m.file)
	isNew := os.IsNotExist(err) || (err == nil && fi.Size() < memHeadReserved)

	if isNew {
		m.buf = make([]byte, memHeadReserved+m.bufSize)
		m.buf[0] = 0xCA
		m.buf[1] = 0xFE
	} else {
		data, err := os.ReadFile(m.file)
		if err != nil {
			return err
		}
		m.bufSize = len(data) - memHeadReserved
		m.buf = data
		m.count = int(protocol.BigEndian.Int32(m.buf[4:]))
	}
	m.capacity = m.bufSize / keyLength
	return nil
}

func (m *MemHashBlock) offset(keyHash int32) int {
	bucketPos := int(keyHash&0x7FFFFFFF) % m.capacity
	return keyLength*bucketPos + memHeadReserved
}

func (m *MemHashBlock) Get(keyHash int32) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	pos := m.offset(keyHash)
	return protocol.BigEndian.Int5(m.buf[pos:])
}

func (m *MemHashBlock) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

func (m *MemHashBlock) addCount(n int) {
	m.count += n
	protocol.BigEndian.PutInt32(m.buf[4:], int32(m.count))
}

func (m *MemHashBlock) Put(keyHash int32, value int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := protocol.BigEndian.Bytes5(value)
	pos := m.offset(keyHash)

	if protocol.BigEndian.Int5(m.buf[pos:]) == 0 {
		m.addCount(1)
	}
	copy(m.buf[pos:], b)
	m.dirty = true
}

func (m *MemHashBlock) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirty {
		return
	}
	_ = os.WriteFile(m.file, m.buf, 0644)
	m.dirty = false
}

func (m *MemHashBlock) IsDirty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirty
}

func (m *MemHashBlock) Interval() time.Duration {
	return 4 * time.Second
}

func (m *MemHashBlock) Close() {
	m.Flush()
	GetFlushController().Unregister(m)
}
