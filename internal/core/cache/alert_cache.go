package cache

import (
	"sync"
)

// AlertCache is a circular buffer for real-time alert delivery to clients.
// Matches Java's AlertCache with loop/index tracking.
type AlertCache struct {
	mu    sync.RWMutex
	buf   [][]byte // serialized AlertPack bytes
	size  int
	loop  int64 // increments each time we wrap around
	index int   // current write position
}

func NewAlertCache(size int) *AlertCache {
	return &AlertCache{
		buf:  make([][]byte, size),
		size: size,
	}
}

// Add adds a serialized AlertPack to the cache.
func (c *AlertCache) Add(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf[c.index] = data
	c.index++
	if c.index >= c.size {
		c.index = 0
		c.loop++
	}
}

// GetSince returns alerts added since the given (loop, index) position.
// Also returns the current (loop, index) for the client to use next time.
func (c *AlertCache) GetSince(clientLoop int64, clientIndex int) ([][]byte, int64, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	curLoop := c.loop
	curIndex := c.index

	// First call or client is too far behind - return nothing, just sync position
	if clientLoop < curLoop-1 {
		return nil, curLoop, curIndex
	}

	var result [][]byte

	if clientLoop == curLoop && clientIndex == curIndex {
		// No new data
		return nil, curLoop, curIndex
	}

	if clientLoop == curLoop {
		// Same loop, read from clientIndex to curIndex
		for i := clientIndex; i < curIndex; i++ {
			if c.buf[i] != nil {
				result = append(result, c.buf[i])
			}
		}
	} else if clientLoop == curLoop-1 && clientIndex > curIndex {
		// Previous loop, read from clientIndex to end, then 0 to curIndex
		for i := clientIndex; i < c.size; i++ {
			if c.buf[i] != nil {
				result = append(result, c.buf[i])
			}
		}
		for i := 0; i < curIndex; i++ {
			if c.buf[i] != nil {
				result = append(result, c.buf[i])
			}
		}
	} else {
		// Client is behind, just sync
		return nil, curLoop, curIndex
	}

	return result, curLoop, curIndex
}

// Position returns the current loop and index.
func (c *AlertCache) Position() (int64, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loop, c.index
}
