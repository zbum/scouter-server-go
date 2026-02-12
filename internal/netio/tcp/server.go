package tcp

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/netio/service"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// ServerConfig holds TCP server configuration.
type ServerConfig struct {
	ListenIP        string
	ListenPort      int
	ClientTimeout   time.Duration
	AgentSoTimeout  time.Duration
	ServicePoolSize int
	AgentConfig     AgentManagerConfig
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ListenIP:      "0.0.0.0",
		ListenPort:    6100,
		ClientTimeout: 60 * time.Second,
		AgentConfig:   DefaultAgentManagerConfig(),
	}
}

// Server is a TCP server that handles client and agent connections.
type Server struct {
	config       ServerConfig
	registry     *service.Registry
	sessions     *login.SessionManager
	agentManager *AgentManager
	agentCaller  *AgentCall
	listener     net.Listener
	wg           sync.WaitGroup
	sem          chan struct{} // semaphore for client connection limiting
}

func NewServer(config ServerConfig, registry *service.Registry, sessions *login.SessionManager) *Server {
	mgr := NewAgentManagerWithConfig(config.AgentConfig)
	poolSize := config.ServicePoolSize
	if poolSize <= 0 {
		poolSize = 100
	}
	return &Server{
		config:       config,
		registry:     registry,
		sessions:     sessions,
		agentManager: mgr,
		agentCaller:  NewAgentCall(mgr),
		sem:          make(chan struct{}, poolSize),
	}
}

// AgentMgr returns the server's agent connection manager.
func (s *Server) AgentMgr() *AgentManager {
	return s.agentManager
}

// AgentCallSingle sends a command to an agent and returns the response MapPack.
func (s *Server) AgentCallSingle(objHash int32, cmd string, param *pack.MapPack) *pack.MapPack {
	return s.agentCaller.Call(objHash, cmd, param)
}

// AgentCallStream sends a command and streams multi-pack responses via the handler.
func (s *Server) AgentCallStream(objHash int32, cmd string, param *pack.MapPack, handler func(pack.Pack)) {
	s.agentCaller.CallStream(objHash, cmd, param, handler)
}

// Start begins accepting TCP connections. Blocks until context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	addr := net.JoinHostPort(s.config.ListenIP, strconv.Itoa(s.config.ListenPort))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln
	slog.Info("TCP server started", "addr", addr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	// Start agent keepalive daemon
	go s.agentManager.StartKeepalive(ctx)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				slog.Info("TCP server stopping")
				s.wg.Wait()
				s.agentManager.Close()
				return nil
			default:
				slog.Error("TCP accept error", "error", err)
				continue
			}
		}

		// Acquire semaphore slot (limits concurrent client connections)
		select {
		case s.sem <- struct{}{}:
		case <-ctx.Done():
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go func() {
			defer func() { <-s.sem }()
			defer s.wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	reader := bufio.NewReaderSize(conn, 8192)
	writer := bufio.NewWriterSize(conn, 8192)

	din := protocol.NewDataInputXStream(reader)

	// Read initial 4-byte magic (unsigned comparison since values > 0x7FFFFFFF)
	cafeInt, err := din.ReadInt32()
	if err != nil {
		slog.Debug("TCP read magic failed", "addr", remoteAddr, "error", err)
		conn.Close()
		return
	}
	cafe := uint32(cafeInt)

	switch cafe {
	case uint32(protocol.TCP_CLIENT):
		defer conn.Close()
		slog.Debug("TCP client connected", "addr", remoteAddr)
		s.handleClient(ctx, reader, writer, remoteAddr)

	case uint32(protocol.TCP_AGENT), uint32(protocol.TCP_AGENT_V2):
		// Read objHash (4 bytes)
		objHashInt, err := din.ReadInt32()
		if err != nil {
			slog.Debug("TCP agent read objHash failed", "addr", remoteAddr, "error", err)
			conn.Close()
			return
		}
		slog.Info("TCP agent connected", "addr", remoteAddr, "objHash", objHashInt, "protocol", cafe)

		// Create worker and add to pool (connection is NOT closed here — it's pooled)
		worker := NewAgentWorker(conn, reader, writer, cafe, objHashInt, s.config.AgentSoTimeout)
		s.agentManager.Add(objHashInt, worker)

	default:
		slog.Debug("TCP unknown connection type", "addr", remoteAddr, "magic", cafe)
		conn.Close()
	}
}

func (s *Server) handleClient(ctx context.Context, reader io.Reader, writer *bufio.Writer, remoteAddr string) {
	din := protocol.NewDataInputXStream(reader)
	dout := protocol.NewDataOutputXStream(writer)

	sessionOk := false

	defer func() {
		if r := recover(); r != nil {
			slog.Error("TCP client handler panic", "addr", remoteAddr, "error", r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read command
		cmd, err := din.ReadText()
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				slog.Debug("TCP client read error", "addr", remoteAddr, "error", err)
			}
			return
		}

		if cmd == protocol.CLOSE {
			slog.Debug("TCP client closing", "addr", remoteAddr)
			return
		}

		// Read session
		session, err := din.ReadLong()
		if err != nil {
			slog.Debug("TCP client read session error", "addr", remoteAddr, "error", err)
			return
		}

		// Validate session for non-free commands
		if !sessionOk && !protocol.FreeCmds[cmd] {
			sessionOk = s.sessions.OkSession(session)
			if !sessionOk {
				dout.WriteByte(protocol.FLAG_INVALID_SESSION)
				dout.Flush()
				slog.Debug("TCP invalid session", "addr", remoteAddr, "cmd", cmd)
				return
			}
		}

		// log_tcp_action_enabled: log TCP command dispatch
		if cfg := config.Get(); cfg != nil && cfg.LogTcpActionEnabled() {
			slog.Info("TCP action", "cmd", cmd, "addr", remoteAddr)
		}

		// Dispatch to handler
		handler := s.registry.Get(cmd)
		if handler != nil {
			handler(din, dout, sessionOk)
		} else {
			// Consume the request pack to keep the stream in sync.
			// All Scouter TCP commands send a request pack after the
			// command text and session ID. If we don't consume it,
			// the leftover bytes corrupt the next command read.
			pack.ReadPack(din)
			slog.Warn("TCP unknown command", "addr", remoteAddr, "cmd", cmd)
		}

		// Write NoNEXT terminator and flush
		dout.WriteByte(protocol.FLAG_NO_NEXT)
		if err := dout.Flush(); err != nil {
			slog.Debug("TCP client write error", "addr", remoteAddr, "error", err)
			return
		}
	}
}
