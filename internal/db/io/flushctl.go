package io

import (
	"sync"
	"time"
)

// IFlushable represents an object that can be periodically flushed to disk.
type IFlushable interface {
	Flush()
	IsDirty() bool
	Interval() time.Duration
}

// FlushController manages periodic flushing of registered IFlushable instances.
var flushCtl = &flushController{
	items: make(map[IFlushable]struct{}),
}

type flushController struct {
	mu      sync.Mutex
	items   map[IFlushable]struct{}
	started bool
}

func GetFlushController() *flushController {
	return flushCtl
}

func (fc *flushController) Register(f IFlushable) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.items[f] = struct{}{}
	if !fc.started {
		fc.started = true
		go fc.run()
	}
}

func (fc *flushController) Unregister(f IFlushable) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	delete(fc.items, f)
}

func (fc *flushController) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		fc.mu.Lock()
		items := make([]IFlushable, 0, len(fc.items))
		for f := range fc.items {
			items = append(items, f)
		}
		fc.mu.Unlock()

		for _, f := range items {
			if f.IsDirty() {
				f.Flush()
			}
		}
	}
}
