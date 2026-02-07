package cache

import (
	"sync"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// TimeType constants matching Java's TimeTypeEnum.
const (
	TimeTypeRealtime byte = 1
	TimeTypeOneMin   byte = 2
	TimeTypeFiveMin  byte = 3
)

// CounterKey identifies a specific counter for an object.
type CounterKey struct {
	ObjHash  int32
	Counter  string
	TimeType byte
}

// CounterCache stores the latest counter values per object.
type CounterCache struct {
	mu    sync.RWMutex
	store map[CounterKey]value.Value
}

func NewCounterCache() *CounterCache {
	return &CounterCache{
		store: make(map[CounterKey]value.Value),
	}
}

func (c *CounterCache) Put(key CounterKey, v value.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = v
}

func (c *CounterCache) Get(key CounterKey) (value.Value, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.store[key]
	return v, ok
}

// GetByObjHash returns all counter values for a given object hash.
func (c *CounterCache) GetByObjHash(objHash int32) map[string]value.Value {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]value.Value)
	for k, v := range c.store {
		if k.ObjHash == objHash {
			result[k.Counter] = v
		}
	}
	return result
}
