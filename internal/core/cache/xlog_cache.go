package cache

import "sync"

// XLogEntry is a serialized XLogPack stored in the cache ring buffer.
type XLogEntry struct {
	ObjHash int32
	Elapsed int32
	IsError bool
	Data    []byte
}

// XLogCacheResult holds the result of a Get call with pagination state.
type XLogCacheResult struct {
	Loop  int64
	Index int
	Data  []XLogEntry
}

// XLogCache is a bounded ring buffer of recent XLog entries for real-time streaming.
// Implements loop/index pagination matching Java's XLogLoopCache.
type XLogCache struct {
	mu      sync.Mutex
	entries []XLogEntry
	size    int
	pos     int // current write index (0..size-1)
	loop    int64
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
	c.pos++
	if c.pos >= c.size {
		c.loop++
		c.pos = 0
	}
	if c.count < c.size {
		c.count++
	}
}

// Get returns entries added since (lastLoop, lastIndex), filtered by minElapsed.
// Entries with elapsed >= minElapsed OR isError are returned.
// If objHashSet is non-nil, entries are filtered to those object hashes.
// Returns the current (loop, index) for the client to use in the next request.
func (c *XLogCache) Get(lastLoop int64, lastIndex int, minElapsed int32, objHashSet map[int32]bool) *XLogCacheResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	endIndex := c.pos
	endLoop := c.loop

	result := &XLogCacheResult{
		Loop:  endLoop,
		Index: endIndex,
	}

	switch endLoop - lastLoop {
	case 0:
		if lastIndex < endIndex {
			c.copyFiltered(&result.Data, lastIndex, endIndex, minElapsed, objHashSet)
		}
	case 1:
		if lastIndex <= endIndex {
			// Gap too large for one loop, return what we can
			c.copyFiltered(&result.Data, endIndex, c.size, minElapsed, objHashSet)
			c.copyFiltered(&result.Data, 0, endIndex, minElapsed, objHashSet)
		} else {
			c.copyFiltered(&result.Data, lastIndex, c.size, minElapsed, objHashSet)
			c.copyFiltered(&result.Data, 0, endIndex, minElapsed, objHashSet)
		}
	default:
		// More than one loop behind, return entire current buffer
		c.copyFiltered(&result.Data, endIndex, c.size, minElapsed, objHashSet)
		c.copyFiltered(&result.Data, 0, endIndex, minElapsed, objHashSet)
	}

	return result
}

func (c *XLogCache) copyFiltered(buf *[]XLogEntry, from, to int, minElapsed int32, objHashSet map[int32]bool) {
	for i := from; i < to; i++ {
		e := c.entries[i]
		if e.Data == nil {
			continue
		}
		if e.Elapsed < minElapsed && !e.IsError {
			continue
		}
		if objHashSet != nil && !objHashSet[e.ObjHash] {
			continue
		}
		*buf = append(*buf, e)
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
