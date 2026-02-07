package cache

import "sync"

// XLogEntry is a serialized XLogPack stored in the cache ring buffer.
type XLogEntry struct {
	ObjHash int32
	Elapsed int32
	IsError bool
	Data    []byte
}

// XLogCache is a bounded ring buffer of recent XLog entries for real-time streaming.
type XLogCache struct {
	mu      sync.Mutex
	entries []XLogEntry
	size    int
	pos     int
	count   int
}

func NewXLogCache(size int) *XLogCache {
	return &XLogCache{
		entries: make([]XLogEntry, size),
		size:    size,
	}
}

func (c *XLogCache) Put(objHash int32, elapsed int32, isError bool, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[c.pos] = XLogEntry{
		ObjHash: objHash,
		Elapsed: elapsed,
		IsError: isError,
		Data:    data,
	}
	c.pos = (c.pos + 1) % c.size
	if c.count < c.size {
		c.count++
	}
}

// GetRecent returns up to maxCount recent entries.
func (c *XLogCache) GetRecent(maxCount int) []XLogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.count
	if n > maxCount {
		n = maxCount
	}
	result := make([]XLogEntry, n)
	start := (c.pos - n + c.size) % c.size
	for i := 0; i < n; i++ {
		result[i] = c.entries[(start+i)%c.size]
	}
	return result
}

// GetRecentByObjHash returns recent entries filtered by object hash.
func (c *XLogCache) GetRecentByObjHash(objHash int32, maxCount int) []XLogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []XLogEntry
	n := c.count
	start := (c.pos - n + c.size) % c.size
	for i := 0; i < n && len(result) < maxCount; i++ {
		entry := c.entries[(start+i)%c.size]
		if entry.ObjHash == objHash {
			result = append(result, entry)
		}
	}
	return result
}

func (c *XLogCache) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}
