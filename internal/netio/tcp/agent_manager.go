package tcp

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultMaxAgents          = 5000
	defaultMaxConnsPerAgent   = 50
	defaultKeepaliveInterval  = 60 * time.Second
	defaultKeepaliveTimeout   = 3 * time.Second
	defaultGetConnWait        = 5 * time.Second
	keepaliveDaemonInterval   = 5 * time.Second
)

// AgentManager manages a pool of TCP agent connections.
// Each agent (identified by objHash) can have multiple pooled connections.
type AgentManager struct {
	mu                sync.Mutex
	agents            map[int32]*agentQueue
	maxConnsPerAgent  int
	keepaliveInterval time.Duration
	keepaliveTimeout  time.Duration
	getConnWait       time.Duration
}

type agentQueue struct {
	mu    sync.Mutex
	items []*AgentWorker
	cond  *sync.Cond
}

func newAgentQueue() *agentQueue {
	q := &agentQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// put adds a worker back to the queue.
func (q *agentQueue) put(w *AgentWorker, maxSize int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) < maxSize {
		q.items = append(q.items, w)
		q.cond.Signal()
	} else {
		w.Close()
	}
}

// getNoWait returns a worker without waiting, or nil if none available.
func (q *agentQueue) getNoWait() *AgentWorker {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	w := q.items[0]
	q.items = q.items[1:]
	return w
}

// get returns a worker, waiting up to timeout for one to become available.
func (q *agentQueue) get(timeout time.Duration) *AgentWorker {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) > 0 {
		w := q.items[0]
		q.items = q.items[1:]
		return w
	}

	if timeout <= 0 {
		return nil
	}

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		time.Sleep(timeout)
		q.cond.Broadcast()
		close(done)
	}()

	q.cond.Wait()

	if len(q.items) > 0 {
		w := q.items[0]
		q.items = q.items[1:]
		return w
	}
	return nil
}

func (q *agentQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// allWorkers returns a snapshot of all workers for keepalive processing.
func (q *agentQueue) drainAll() []*AgentWorker {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]*AgentWorker, len(q.items))
	copy(result, q.items)
	q.items = q.items[:0]
	return result
}

func NewAgentManager() *AgentManager {
	return &AgentManager{
		agents:            make(map[int32]*agentQueue),
		maxConnsPerAgent:  defaultMaxConnsPerAgent,
		keepaliveInterval: defaultKeepaliveInterval,
		keepaliveTimeout:  defaultKeepaliveTimeout,
		getConnWait:       defaultGetConnWait,
	}
}

// Add registers an agent connection for the given object hash.
func (m *AgentManager) Add(objHash int32, worker *AgentWorker) {
	m.mu.Lock()
	q, ok := m.agents[objHash]
	if !ok {
		if len(m.agents) >= defaultMaxAgents {
			m.mu.Unlock()
			worker.Close()
			return
		}
		q = newAgentQueue()
		m.agents[objHash] = q
	}
	m.mu.Unlock()

	q.put(worker, m.maxConnsPerAgent)
}

// Get retrieves an available agent connection, waiting if necessary.
func (m *AgentManager) Get(objHash int32) *AgentWorker {
	m.mu.Lock()
	q, ok := m.agents[objHash]
	m.mu.Unlock()

	if !ok {
		return nil
	}
	return q.get(m.getConnWait)
}

// HasAgent checks if there's at least one connection for the given objHash.
func (m *AgentManager) HasAgent(objHash int32) bool {
	m.mu.Lock()
	q, ok := m.agents[objHash]
	m.mu.Unlock()
	if !ok {
		return false
	}
	return q.size() > 0
}

// StartKeepalive runs the keepalive daemon that checks connections periodically.
func (m *AgentManager) StartKeepalive(ctx context.Context) {
	ticker := time.NewTicker(keepaliveDaemonInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runKeepalive()
		}
	}
}

func (m *AgentManager) runKeepalive() {
	m.mu.Lock()
	keys := make([]int32, 0, len(m.agents))
	queues := make([]*agentQueue, 0, len(m.agents))
	for k, q := range m.agents {
		keys = append(keys, k)
		queues = append(queues, q)
	}
	m.mu.Unlock()

	for i, q := range queues {
		workers := q.drainAll()
		for _, w := range workers {
			if w.IsClosed() {
				continue
			}
			if w.IsExpired(m.keepaliveInterval) {
				w.SendKeepAlive(m.keepaliveTimeout)
			}
			if !w.IsClosed() {
				q.put(w, m.maxConnsPerAgent)
			}
		}

		// Clean up empty queues
		if q.size() == 0 {
			m.mu.Lock()
			if q.size() == 0 {
				delete(m.agents, keys[i])
			}
			m.mu.Unlock()
		}
	}
}

// Size returns the total number of agent object hashes tracked.
func (m *AgentManager) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.agents)
}

// Close closes all agent connections.
func (m *AgentManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, q := range m.agents {
		workers := q.drainAll()
		for _, w := range workers {
			w.Close()
		}
	}
	m.agents = make(map[int32]*agentQueue)
	slog.Info("TCP agent manager closed")
}
