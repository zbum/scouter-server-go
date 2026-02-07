package tcp

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// simulateAgent connects to the server as a TCP_AGENT and returns the connection for manual control.
func simulateAgent(t *testing.T, addr net.Addr, protocolType uint32, objHash int32) (net.Conn, *protocol.DataInputX, *protocol.DataOutputX) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr.String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	din := protocol.NewDataInputXStream(reader)
	dout := protocol.NewDataOutputXStream(writer)

	// Write magic (as raw 4 bytes)
	dout.WriteInt32(int32(protocolType))
	// Write objHash
	dout.WriteInt32(objHash)
	dout.Flush()

	return conn, din, dout
}

func TestTCP_AgentConnect(t *testing.T) {
	addr, cancel, _, _, _, _ := startTestServer(t)
	defer cancel()

	// Connect as agent
	conn, _, _ := simulateAgent(t, addr, uint32(protocol.TCP_AGENT), 100)
	defer conn.Close()

	// Wait for server to process
	time.Sleep(100 * time.Millisecond)

	// The server should have the agent in the manager
	// (We can't directly access the server's agentManager from here,
	// but the connection should still be open)
}

func TestTCP_AgentV2Connect(t *testing.T) {
	addr, cancel, _, _, _, _ := startTestServer(t)
	defer cancel()

	conn, _, _ := simulateAgent(t, addr, uint32(protocol.TCP_AGENT_V2), 200)
	defer conn.Close()

	time.Sleep(100 * time.Millisecond)
}

// TestAgentManager_AddGet tests the agent pool directly.
func TestAgentManager_AddGet(t *testing.T) {
	mgr := NewAgentManager()

	// Create a pair of connected sockets for testing
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		reader := bufio.NewReaderSize(conn, 4096)
		writer := bufio.NewWriterSize(conn, 4096)
		worker := NewAgentWorker(conn, reader, writer, uint32(protocol.TCP_AGENT), 100)
		mgr.Add(100, worker)
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	time.Sleep(50 * time.Millisecond)

	if !mgr.HasAgent(100) {
		t.Fatal("expected agent 100 to exist")
	}
	if mgr.HasAgent(999) {
		t.Fatal("expected agent 999 to not exist")
	}

	// Get the agent
	worker := mgr.Get(100)
	if worker == nil {
		t.Fatal("expected non-nil worker")
	}
	if worker.ObjHash() != 100 {
		t.Fatalf("expected objHash 100, got %d", worker.ObjHash())
	}

	// Return to pool
	mgr.Add(100, worker)
	if mgr.Size() != 1 {
		t.Fatalf("expected 1 agent, got %d", mgr.Size())
	}
}

// TestAgentWorker_WriteRead tests protocol v1 write/read over a pipe.
func TestAgentWorker_WriteRead_V1(t *testing.T) {
	// Create a connected pair
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	sReader := bufio.NewReaderSize(server, 4096)
	sWriter := bufio.NewWriterSize(server, 4096)

	worker := NewAgentWorker(server, sReader, sWriter, uint32(protocol.TCP_AGENT), 42)

	// Agent side: read command, respond
	go func() {
		cReader := bufio.NewReader(client)
		cWriter := bufio.NewWriter(client)
		din := protocol.NewDataInputXStream(cReader)
		dout := protocol.NewDataOutputXStream(cWriter)

		// Read command (v1: text + pack)
		cmd, err := din.ReadText()
		if err != nil {
			t.Error("agent read cmd error:", err)
			return
		}
		if cmd != "TEST_CMD" {
			t.Errorf("expected TEST_CMD, got %s", cmd)
			return
		}

		p, err := pack.ReadPack(din)
		if err != nil {
			t.Error("agent read pack error:", err)
			return
		}
		mp := p.(*pack.MapPack)
		if mp.GetText("key") != "value" {
			t.Errorf("expected value, got %s", mp.GetText("key"))
		}

		// Send response: HasNEXT + MapPack + NoNEXT
		resp := &pack.MapPack{}
		resp.PutStr("result", "ok")

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
		dout.WriteByte(protocol.FLAG_NO_NEXT)
		dout.Flush()
	}()

	// Server side: send command via worker
	param := &pack.MapPack{}
	param.PutStr("key", "value")
	if err := worker.Write("TEST_CMD", param); err != nil {
		t.Fatal("write error:", err)
	}

	// Read response
	flag, err := worker.ReadByte()
	if err != nil {
		t.Fatal("read flag error:", err)
	}
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, err := worker.ReadPack()
	if err != nil {
		t.Fatal("read pack error:", err)
	}
	mp := resp.(*pack.MapPack)
	if mp.GetText("result") != "ok" {
		t.Fatalf("expected ok, got %s", mp.GetText("result"))
	}

	flag, _ = worker.ReadByte()
	if flag != protocol.FLAG_NO_NEXT {
		t.Fatalf("expected NoNEXT, got %d", flag)
	}
}

// TestAgentWorker_WriteRead_V2 tests protocol v2 (length-prefixed) over a pipe.
func TestAgentWorker_WriteRead_V2(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	sReader := bufio.NewReaderSize(server, 4096)
	sWriter := bufio.NewWriterSize(server, 4096)

	worker := NewAgentWorker(server, sReader, sWriter, uint32(protocol.TCP_AGENT_V2), 42)

	// Agent side: read v2 frame, respond
	go func() {
		cReader := bufio.NewReader(client)
		cWriter := bufio.NewWriter(client)
		din := protocol.NewDataInputXStream(cReader)
		dout := protocol.NewDataOutputXStream(cWriter)

		// Read v2 frame: length-prefixed bytes
		buf, err := din.ReadIntBytes()
		if err != nil {
			t.Error("agent read frame error:", err)
			return
		}

		// Parse frame: text(cmd) + pack
		frameDin := protocol.NewDataInputX(buf)
		cmd, err := frameDin.ReadText()
		if err != nil {
			t.Error("agent parse cmd error:", err)
			return
		}
		if cmd != "TEST_V2" {
			t.Errorf("expected TEST_V2, got %s", cmd)
		}

		p, err := pack.ReadPack(frameDin)
		if err != nil {
			t.Error("agent parse pack error:", err)
			return
		}
		mp := p.(*pack.MapPack)
		if mp.GetText("k") != "v" {
			t.Errorf("expected v, got %s", mp.GetText("k"))
		}

		// Send v2 response: flag + length-prefixed pack
		resp := &pack.MapPack{}
		resp.PutStr("status", "done")

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		respBuf := protocol.NewDataOutputX()
		pack.WritePack(respBuf, resp)
		dout.WriteIntBytes(respBuf.ToByteArray())
		dout.WriteByte(protocol.FLAG_NO_NEXT)
		dout.Flush()
	}()

	param := &pack.MapPack{}
	param.PutStr("k", "v")
	if err := worker.Write("TEST_V2", param); err != nil {
		t.Fatal("write error:", err)
	}

	flag, err := worker.ReadByte()
	if err != nil {
		t.Fatal("read flag error:", err)
	}
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, err := worker.ReadPack()
	if err != nil {
		t.Fatal("read pack error:", err)
	}
	mp := resp.(*pack.MapPack)
	if mp.GetText("status") != "done" {
		t.Fatalf("expected done, got %s", mp.GetText("status"))
	}

	flag, _ = worker.ReadByte()
	if flag != protocol.FLAG_NO_NEXT {
		t.Fatalf("expected NoNEXT, got %d", flag)
	}
}

// TestAgentCall tests the RPC call pattern.
func TestAgentCall_Call(t *testing.T) {
	mgr := NewAgentManager()
	ac := NewAgentCall(mgr)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	sReader := bufio.NewReaderSize(server, 4096)
	sWriter := bufio.NewWriterSize(server, 4096)
	worker := NewAgentWorker(server, sReader, sWriter, uint32(protocol.TCP_AGENT), 50)
	mgr.Add(50, worker)

	// Agent side: handle command and respond
	go func() {
		cReader := bufio.NewReader(client)
		cWriter := bufio.NewWriter(client)
		din := protocol.NewDataInputXStream(cReader)
		dout := protocol.NewDataOutputXStream(cWriter)

		// Read command
		cmd, _ := din.ReadText()
		pack.ReadPack(din) // read param

		resp := &pack.MapPack{}
		resp.PutStr("cmd_received", cmd)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
		dout.WriteByte(protocol.FLAG_NO_NEXT)
		dout.Flush()
	}()

	param := &pack.MapPack{}
	param.PutStr("action", "dump")
	result := ac.Call(50, "THREAD_DUMP", param)

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.GetText("cmd_received") != "THREAD_DUMP" {
		t.Fatalf("expected THREAD_DUMP, got %s", result.GetText("cmd_received"))
	}

	// Worker should be back in the pool
	if !mgr.HasAgent(50) {
		t.Fatal("expected agent to be back in pool")
	}
}

// TestAgentCall_NoAgent tests calling when no agent is available.
func TestAgentCall_NoAgent(t *testing.T) {
	mgr := NewAgentManager()
	mgr.getConnWait = 100 * time.Millisecond // short timeout for testing
	ac := NewAgentCall(mgr)

	result := ac.Call(999, "SOME_CMD", nil)
	if result != nil {
		t.Fatal("expected nil result for non-existent agent")
	}
}
