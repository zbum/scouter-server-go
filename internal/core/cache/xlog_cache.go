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
// objIndex provides O(k) lookup by objHash (k = entries for that hash)
// instead of O(n) full buffer scan when objHashSet filter is used.
type XLogCache struct {
	mu       sync.RWMutex
	entries  []XLogEntry
	size     int
	pos      int // current write index (0..size-1)
	loop     int64
	count    int
	objIndex map[int32][]int // objHash → positions in ring buffer (append order)
}

func NewXLogCache(size int) *XLogCache {
	return &XLogCache{
		entries:  make([]XLogEntry, size),
		size:     size,
		objIndex: make(map[int32][]int),
	}
}

func (c *XLogCache) Put(objHash int32, elapsed int32, isError bool, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove overwritten entry from index.
	// The overwritten position is always the oldest in its objHash list (index 0)
	// because ring buffer positions are overwritten in the same order they were written.
	old := c.entries[c.pos]
	if old.Data != nil {
		if list := c.objIndex[old.ObjHash]; len(list) > 0 && list[0] == c.pos {
			if len(list) == 1 {
				delete(c.objIndex, old.ObjHash)
			} else {
				c.objIndex[old.ObjHash] = list[1:]
			}
		}
	}

	c.entries[c.pos] = XLogEntry{
		ObjHash: objHash,
		Elapsed: elapsed,
		IsError: isError,
		Data:    data,
	}
	c.objIndex[objHash] = append(c.objIndex[objHash], c.pos)

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
	c.mu.RLock()
	defer c.mu.RUnlock()

	endIndex := c.pos
	endLoop := c.loop

	result := &XLogCacheResult{
		Loop:  endLoop,
		Index: endIndex,
	}

	if objHashSet != nil {
		// Index-based path: iterate only positions matching the requested objHashes
		c.copyFromIndex(&result.Data, lastLoop, lastIndex, endLoop, endIndex, minElapsed, objHashSet)
	} else {
		// Scan-based path: iterate the full range
		switch endLoop - lastLoop {
		case 0:
			if lastIndex < endIndex {
				c.copyFiltered(&result.Data, lastIndex, endIndex, minElapsed)
			}
		case 1:
			if lastIndex <= endIndex {
				c.copyFiltered(&result.Data, endIndex, c.size, minElapsed)
				c.copyFiltered(&result.Data, 0, endIndex, minElapsed)
			} else {
				c.copyFiltered(&result.Data, lastIndex, c.size, minElapsed)
				c.copyFiltered(&result.Data, 0, endIndex, minElapsed)
			}
		default:
			c.copyFiltered(&result.Data, endIndex, c.size, minElapsed)
			c.copyFiltered(&result.Data, 0, endIndex, minElapsed)
		}
	}

	return result
}

// copyFromIndex uses the objIndex to iterate only entries matching the requested objHashes.
func (c *XLogCache) copyFromIndex(buf *[]XLogEntry, lastLoop int64, lastIndex int, endLoop int64, endIndex int, minElapsed int32, objHashSet map[int32]bool) {
	for objHash := range objHashSet {
		for _, pos := range c.objIndex[objHash] {
			if !c.inRange(pos, lastLoop, lastIndex, endLoop, endIndex) {
				continue
			}
			e := c.entries[pos]
			if e.Data == nil {
				continue
			}
			if e.Elapsed < minElapsed && !e.IsError {
				continue
			}
			*buf = append(*buf, e)
		}
	}
}

// inRange checks if a ring buffer position falls within the requested (lastLoop, lastIndex) → (endLoop, endIndex) range.
func (c *XLogCache) inRange(pos int, lastLoop int64, lastIndex int, endLoop int64, endIndex int) bool {
	switch endLoop - lastLoop {
	case 0:
		return pos >= lastIndex && pos < endIndex
	case 1:
		if lastIndex <= endIndex {
			return true // entire buffer
		}
		return pos >= lastIndex || pos < endIndex
	default:
		return true // entire buffer
	}
}

func (c *XLogCache) copyFiltered(buf *[]XLogEntry, from, to int, minElapsed int32) {
	for i := from; i < to; i++ {
		e := &c.entries[i]
		if e.Data == nil {
			continue
		}
		if e.Elapsed < minElapsed && !e.IsError {
			continue
		}
		*buf = append(*buf, *e)
	}
}

// GetRecent returns up to maxCount recent entries.
func (c *XLogCache) GetRecent(maxCount int) []XLogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

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
	c.mu.RLock()
	defer c.mu.RUnlock()

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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}
