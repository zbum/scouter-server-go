package visitor

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync"
)

const (
	hllPrecision = 14        // 2^14 = 16384 registers
	hllRegisters = 1 << hllPrecision
	hllAlpha     = 0.7213 / (1 + 1.079/float64(hllRegisters))
)

// HLL is a HyperLogLog counter for approximate cardinality estimation.
type HLL struct {
	mu        sync.Mutex
	registers [hllRegisters]uint8
	dirty     bool
}

// NewHLL creates a new HyperLogLog counter.
func NewHLL() *HLL {
	return &HLL{}
}

// Offer adds a value to the HLL counter.
func (h *HLL) Offer(value int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	hash := hashValue(value)
	idx := hash >> (64 - hllPrecision)
	w := hash << hllPrecision
	rho := uint8(leadingZeros(w) + 1)

	if rho > h.registers[idx] {
		h.registers[idx] = rho
		h.dirty = true
	}
}

// Count returns the estimated cardinality.
func (h *HLL) Count() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	sum := 0.0
	zeros := 0
	for _, val := range h.registers {
		sum += 1.0 / math.Pow(2, float64(val))
		if val == 0 {
			zeros++
		}
	}

	estimate := hllAlpha * float64(hllRegisters) * float64(hllRegisters) / sum

	// Small range correction
	if estimate <= 2.5*float64(hllRegisters) && zeros > 0 {
		estimate = float64(hllRegisters) * math.Log(float64(hllRegisters)/float64(zeros))
	}

	return int64(estimate + 0.5)
}

// IsDirty returns whether the HLL has been modified since last serialize.
func (h *HLL) IsDirty() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dirty
}

// Serialize serializes the HLL to bytes.
func (h *HLL) Serialize() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dirty = false
	data := make([]byte, hllRegisters)
	copy(data, h.registers[:])
	return data
}

// Deserialize loads HLL state from bytes.
func (h *HLL) Deserialize(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(data) >= hllRegisters {
		copy(h.registers[:], data[:hllRegisters])
	}
}

// Merge merges another HLL into this one (takes max of each register).
func (h *HLL) Merge(other *HLL) {
	h.mu.Lock()
	defer h.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()

	for i := 0; i < hllRegisters; i++ {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
			h.dirty = true
		}
	}
}

func hashValue(v int64) uint64 {
	h := fnv.New64a()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	h.Write(b)
	return h.Sum64()
}

func leadingZeros(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	n := uint8(0)
	for x&(1<<63) == 0 {
		n++
		x <<= 1
	}
	return n
}
