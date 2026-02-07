package udp

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"time"
)

// ServerConfig holds UDP server configuration.
type ServerConfig struct {
	ListenIP   string
	ListenPort int
	BufSize    int
	RcvBufSize int
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ListenIP:   "0.0.0.0",
		ListenPort: 6100,
		BufSize:    65535,
		RcvBufSize: 8 * 1024 * 1024, // 8MB
	}
}

// Server is a UDP datagram listener that feeds received packets to a NetDataProcessor.
type Server struct {
	config    ServerConfig
	processor *NetDataProcessor
	conn      *net.UDPConn
}

func NewServer(config ServerConfig, processor *NetDataProcessor) *Server {
	return &Server{
		config:    config,
		processor: processor,
	}
}

// Start begins listening for UDP datagrams. It blocks until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	addr := net.JoinHostPort(s.config.ListenIP, strconv.Itoa(s.config.ListenPort))
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	s.conn = conn

	if s.config.RcvBufSize > 0 {
		if err := conn.SetReadBuffer(s.config.RcvBufSize); err != nil {
			slog.Warn("failed to set UDP receive buffer", "size", s.config.RcvBufSize, "error", err)
		}
	}

	slog.Info("UDP server started", "addr", addr)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	buf := make([]byte, s.config.BufSize)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				slog.Info("UDP server stopping")
				return nil
			default:
				slog.Error("UDP read error", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
		}

		data := make([]byte, n)
		copy(data, buf[:n])
		s.processor.Add(data, remoteAddr)
	}
}
