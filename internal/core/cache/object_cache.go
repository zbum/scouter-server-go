package cache

import (
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// ObjectInfo represents a monitored agent/object with its current state.
type ObjectInfo struct {
	Pack     *pack.ObjectPack
	LastSeen time.Time
}

// ObjectCache stores registered agents/objects keyed by object hash.
type ObjectCache struct {
	mu    sync.RWMutex
	store map[int32]*ObjectInfo
}

func NewObjectCache() *ObjectCache {
	return &ObjectCache{
		store: make(map[int32]*ObjectInfo),
	}
}

func (c *ObjectCache) Put(objHash int32, p *pack.ObjectPack) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[objHash] = &ObjectInfo{
		Pack:     p,
		LastSeen: time.Now(),
	}
}

func (c *ObjectCache) Get(objHash int32) (*ObjectInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.store[objHash]
	return v, ok
}

func (c *ObjectCache) GetAll() []*ObjectInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]*ObjectInfo, 0, len(c.store))
	for _, v := range c.store {
		result = append(result, v)
	}
	return result
}

// GetLive returns objects that have been seen within the given timeout duration.
func (c *ObjectCache) GetLive(timeout time.Duration) []*ObjectInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	var result []*ObjectInfo
	for _, v := range c.store {
		if now.Sub(v.LastSeen) < timeout {
			result = append(result, v)
		}
	}
	return result
}

// MarkDead marks objects that haven't been seen within the timeout as not alive.
// Returns the list of newly-dead objects.
func (c *ObjectCache) MarkDead(timeout time.Duration) []*ObjectInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	var dead []*ObjectInfo
	for _, v := range c.store {
		if v.Pack.Alive && now.Sub(v.LastSeen) >= timeout {
			v.Pack.Alive = false
			dead = append(dead, v)
		}
	}
	return dead
}

// Remove deletes an object from the cache by its hash.
func (c *ObjectCache) Remove(objHash int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, objHash)
}

func (c *ObjectCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.store)
}
