package tcp

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// AgentWorker manages a single TCP connection to an agent.
// It supports both TCP_AGENT (v1) and TCP_AGENT_V2 (length-prefixed) protocols.
type AgentWorker struct {
	mu            sync.Mutex
	conn          net.Conn
	din           *protocol.DataInputX
	dout          *protocol.DataOutputX
	writer        *bufio.Writer
	protocolType  uint32
	objHash       int32
	lastWriteTime time.Time
	closed        bool
}

func NewAgentWorker(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer, protocolType uint32, objHash int32) *AgentWorker {
	return &AgentWorker{
		conn:          conn,
		din:           protocol.NewDataInputXStream(reader),
		dout:          protocol.NewDataOutputXStream(writer),
		writer:        writer,
		protocolType:  protocolType,
		objHash:       objHash,
		lastWriteTime: time.Now(),
	}
}

// Write sends a command and pack to the agent.
func (w *AgentWorker) Write(cmd string, p pack.Pack) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return io.ErrClosedPipe
	}

	switch w.protocolType {
	case uint32(protocol.TCP_AGENT):
		w.dout.WriteText(cmd)
		pack.WritePack(w.dout, p)
	case uint32(protocol.TCP_AGENT_V2):
		buf := protocol.NewDataOutputX()
		buf.WriteText(cmd)
		pack.WritePack(buf, p)
		w.dout.WriteIntBytes(buf.ToByteArray())
	}

	if err := w.dout.Flush(); err != nil {
		w.closeInternal()
		return err
	}

	w.lastWriteTime = time.Now()
	return nil
}

// ReadPack reads a pack response from the agent.
func (w *AgentWorker) ReadPack() (pack.Pack, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, io.ErrClosedPipe
	}

	switch w.protocolType {
	case uint32(protocol.TCP_AGENT):
		return pack.ReadPack(w.din)
	case uint32(protocol.TCP_AGENT_V2):
		buf, err := w.din.ReadIntBytes()
		if err != nil {
			w.closeInternal()
			return nil, err
		}
		return pack.ReadPack(protocol.NewDataInputX(buf))
	default:
		return nil, io.ErrUnexpectedEOF
	}
}

// ReadByte reads a single byte (flag) from the agent.
func (w *AgentWorker) ReadByte() (byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}

	b, err := w.din.ReadByte()
	if err != nil {
		w.closeInternal()
		return 0, err
	}
	return b, nil
}

// IsExpired checks if the connection has been idle longer than the keepalive interval.
func (w *AgentWorker) IsExpired(keepaliveInterval time.Duration) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Since(w.lastWriteTime) >= keepaliveInterval
}

// SendKeepAlive sends a KEEP_ALIVE command and drains any responses.
func (w *AgentWorker) SendKeepAlive(readTimeout time.Duration) {
	if w.IsClosed() {
		return
	}

	// Set a short read timeout for keepalive
	w.conn.SetReadDeadline(time.Now().Add(readTimeout))
	defer w.conn.SetReadDeadline(time.Time{}) // reset

	if err := w.Write("KEEP_ALIVE", &pack.MapPack{}); err != nil {
		return
	}

	// Drain any responses
	for {
		b, err := w.ReadByte()
		if err != nil || b != protocol.FLAG_HAS_NEXT {
			break
		}
		// Discard the response data
		w.mu.Lock()
		switch w.protocolType {
		case uint32(protocol.TCP_AGENT):
			pack.ReadPack(w.din)
		case uint32(protocol.TCP_AGENT_V2):
			w.din.ReadIntBytes()
		}
		w.mu.Unlock()
	}
}

// IsClosed returns whether the connection has been closed.
func (w *AgentWorker) IsClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// Close closes the connection.
func (w *AgentWorker) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeInternal()
}

func (w *AgentWorker) closeInternal() {
	if !w.closed {
		w.closed = true
		w.conn.Close()
		slog.Debug("TCP agent connection closed", "objHash", w.objHash, "addr", w.conn.RemoteAddr())
	}
}

// ObjHash returns the agent's object hash.
func (w *AgentWorker) ObjHash() int32 {
	return w.objHash
}

// ProtocolType returns the protocol type (TCP_AGENT or TCP_AGENT_V2).
func (w *AgentWorker) ProtocolType() uint32 {
	return w.protocolType
}
