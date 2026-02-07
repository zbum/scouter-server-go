package io

import (
	"os"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/util"
)

const (
	timeBlockCapacity  = 3600 * 24 * 2 // 172,800 buckets covering 48 hours at 500ms resolution
	timeBlockBufSize   = timeBlockCapacity * keyLength
)

// MemTimeBlock is a time-based bucket table with 500ms resolution, backed by a .hfile on disk.
type MemTimeBlock struct {
	mu       sync.Mutex
	path     string
	file     string
	buf      []byte
	bufSize  int
	count    int
	dirty    bool
}

func NewMemTimeBlock(path string) (*MemTimeBlock, error) {
	m := &MemTimeBlock{
		path:    path,
		file:    path + ".hfile",
		bufSize: timeBlockBufSize,
	}
	if err := m.open(); err != nil {
		return nil, err
	}
	GetFlushController().Register(m)
	return m, nil
}

func (m *MemTimeBlock) open() error {
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
		m.count = int(protocol.ToInt(m.buf, 4))
	}
	return nil
}

func (m *MemTimeBlock) offset(timeMs int64) int {
	seconds := util.GetDateMillis(timeMs) / 500
	hash := seconds % timeBlockCapacity
	return keyLength*hash + memHeadReserved
}

func (m *MemTimeBlock) Get(timeMs int64) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	pos := m.offset(timeMs)
	return protocol.ToLong5(m.buf, pos)
}

func (m *MemTimeBlock) GetCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

func (m *MemTimeBlock) AddCount(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addCountInternal(n)
}

func (m *MemTimeBlock) addCountInternal(n int) {
	m.count += n
	protocol.SetBytes(m.buf, 4, protocol.ToBytesInt(int32(m.count)))
}

func (m *MemTimeBlock) Put(timeMs int64, value int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := protocol.ToBytes5(value)
	pos := m.offset(timeMs)

	if protocol.ToLong5(m.buf, pos) == 0 {
		m.addCountInternal(1)
	}
	copy(m.buf[pos:], b)
	m.dirty = true
}

func (m *MemTimeBlock) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirty {
		return
	}
	_ = os.WriteFile(m.file, m.buf, 0644)
	m.dirty = false
}

func (m *MemTimeBlock) IsDirty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirty
}

func (m *MemTimeBlock) Interval() time.Duration {
	return 4 * time.Second
}

func (m *MemTimeBlock) Close() {
	m.Flush()
	GetFlushController().Unregister(m)
}
