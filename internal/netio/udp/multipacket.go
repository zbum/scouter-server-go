package udp

import (
	"log/slog"
	"sync"
	"time"
)

// multiPacket holds fragments of a split UDP packet awaiting reassembly.
type multiPacket struct {
	total     int
	objHash   int32
	fragments [][]byte
	received  int
	created   time.Time
}

func (mp *multiPacket) isDone() bool {
	return mp.received >= mp.total
}

func (mp *multiPacket) toBytes() []byte {
	size := 0
	for _, f := range mp.fragments {
		size += len(f)
	}
	buf := make([]byte, 0, size)
	for _, f := range mp.fragments {
		buf = append(buf, f...)
	}
	return buf
}

// MultiPacketProcessor reassembles MTU-split UDP packets.
// CAFM packet format: [magic:4][objHash:4][pkid:8][total:2][num:2][blob:var]
type MultiPacketProcessor struct {
	mu       sync.Mutex
	packets  map[int64]*multiPacket
	maxItems int
	expiry   time.Duration
}

func NewMultiPacketProcessor() *MultiPacketProcessor {
	mp := &MultiPacketProcessor{
		packets:  make(map[int64]*multiPacket),
		maxItems: 1000,
		expiry:   10 * time.Second,
	}
	go mp.cleanupLoop()
	return mp
}

// Add registers a fragment and returns the reassembled data when complete, or nil if incomplete.
func (p *MultiPacketProcessor) Add(pkid int64, total int16, num int16, data []byte, objHash int32) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	mp, ok := p.packets[pkid]
	if !ok {
		if len(p.packets) >= p.maxItems {
			slog.Warn("MultiPacketProcessor overflow, dropping old entries")
			// Drop oldest entries
			for k := range p.packets {
				delete(p.packets, k)
				break
			}
		}
		mp = &multiPacket{
			total:     int(total),
			objHash:   objHash,
			fragments: make([][]byte, total),
			created:   time.Now(),
		}
		p.packets[pkid] = mp
	}

	idx := int(num)
	if idx >= 0 && idx < mp.total && mp.fragments[idx] == nil {
		mp.fragments[idx] = data
		mp.received++
	}

	if mp.isDone() {
		result := mp.toBytes()
		delete(p.packets, pkid)
		return result
	}
	return nil
}

func (p *MultiPacketProcessor) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		now := time.Now()
		for k, mp := range p.packets {
			if now.Sub(mp.created) > p.expiry {
				slog.Debug("MultiPacket expired", "pkid", k, "received", mp.received, "total", mp.total)
				delete(p.packets, k)
			}
		}
		p.mu.Unlock()
	}
}
