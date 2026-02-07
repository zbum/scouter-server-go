package cache

import (
	"container/list"
	"sync"
)

const defaultTextCacheMaxSize = 100000

// TextCache stores text hash-to-string mappings with a type prefix (e.g., "service", "sql").
// It uses an LRU eviction policy to bound memory usage.
type TextCache struct {
	mu      sync.Mutex
	maxSize int
	items   map[textKey]*list.Element
	evict   *list.List // front = most recently used
}

type textKey struct {
	div  string
	hash int32
}

type textEntry struct {
	key   textKey
	value string
}

func NewTextCache() *TextCache {
	return NewTextCacheWithSize(defaultTextCacheMaxSize)
}

func NewTextCacheWithSize(maxSize int) *TextCache {
	if maxSize <= 0 {
		maxSize = defaultTextCacheMaxSize
	}
	return &TextCache{
		maxSize: maxSize,
		items:   make(map[textKey]*list.Element, maxSize),
		evict:   list.New(),
	}
}

func (c *TextCache) Put(div string, hash int32, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := textKey{div: div, hash: hash}

	if elem, ok := c.items[key]; ok {
		// Update existing entry and move to front
		c.evict.MoveToFront(elem)
		elem.Value.(*textEntry).value = text
		return
	}

	// Evict oldest if at capacity
	for c.evict.Len() >= c.maxSize {
		back := c.evict.Back()
		if back == nil {
			break
		}
		c.removeElement(back)
	}

	// Add new entry at front
	entry := &textEntry{key: key, value: text}
	elem := c.evict.PushFront(entry)
	c.items[key] = elem
}

func (c *TextCache) Get(div string, hash int32) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := textKey{div: div, hash: hash}
	elem, ok := c.items[key]
	if !ok {
		return "", false
	}

	// Move to front on access
	c.evict.MoveToFront(elem)
	return elem.Value.(*textEntry).value, true
}

func (c *TextCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evict.Len()
}

func (c *TextCache) removeElement(elem *list.Element) {
	c.evict.Remove(elem)
	entry := elem.Value.(*textEntry)
	delete(c.items, entry.key)
}
