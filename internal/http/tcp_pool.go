package http

import (
	"net"
	"sync"
	"time"
)

// TCPClientPool manages a pool of TCP connections to a remote Scouter server.
// This is used by the webapp to proxy requests to the collector server.
// In a monolithic deployment (HTTP + TCP in same process), this pool is not needed.
// It is provided for future multi-server support.
type TCPClientPool struct {
	mu          sync.Mutex
	host        string
	port        int
	poolSize    int
	poolTimeout time.Duration
	soTimeout   time.Duration
	conns       chan net.Conn
}

// TCPClientPoolConfig holds configuration for the TCP client pool.
type TCPClientPoolConfig struct {
	Host        string
	Port        int
	PoolSize    int
	PoolTimeout time.Duration
	SoTimeout   time.Duration
}

// NewTCPClientPool creates a new TCP client connection pool.
func NewTCPClientPool(cfg TCPClientPoolConfig) *TCPClientPool {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 30
	}
	if cfg.PoolTimeout <= 0 {
		cfg.PoolTimeout = 60 * time.Second
	}
	if cfg.SoTimeout <= 0 {
		cfg.SoTimeout = 30 * time.Second
	}

	return &TCPClientPool{
		host:        cfg.Host,
		port:        cfg.Port,
		poolSize:    cfg.PoolSize,
		poolTimeout: cfg.PoolTimeout,
		soTimeout:   cfg.SoTimeout,
		conns:       make(chan net.Conn, cfg.PoolSize),
	}
}

// Get retrieves a connection from the pool or creates a new one.
func (p *TCPClientPool) Get() (net.Conn, error) {
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		return p.dial()
	}
}

// Put returns a connection to the pool. If the pool is full, the connection is closed.
func (p *TCPClientPool) Put(conn net.Conn) {
	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

// Close closes all pooled connections.
func (p *TCPClientPool) Close() {
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
}

func (p *TCPClientPool) dial() (net.Conn, error) {
	addr := net.JoinHostPort(p.host, itoa32(p.port))
	conn, err := net.DialTimeout("tcp", addr, p.poolTimeout)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func itoa32(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [10]byte
	pos := len(buf) - 1
	for i > 0 {
		buf[pos] = byte('0' + i%10)
		pos--
		i /= 10
	}
	return string(buf[pos+1:])
}
