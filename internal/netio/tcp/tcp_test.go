package tcp

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/counter"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/netio/service"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

const testVersion = "1.0.0-test"

func startTestServer(t *testing.T) (net.Addr, context.CancelFunc, *cache.ObjectCache, *cache.CounterCache, *cache.TextCache, *cache.XLogCache) {
	t.Helper()

	sessions := login.NewSessionManager(nil)
	textCache := cache.NewTextCache()
	xlogCache := cache.NewXLogCache(1000)
	counterCache := cache.NewCounterCache()
	objectCache := cache.NewObjectCache()

	registry := service.NewRegistry()
	service.RegisterLoginHandlers(registry, sessions, nil, testVersion)
	service.RegisterServerHandlers(registry, testVersion)
	service.RegisterObjectHandlers(registry, objectCache, 30*time.Second, counterCache, counter.NewObjectTypeManager())
	service.RegisterCounterHandlers(registry, counterCache, objectCache, 30*time.Second, nil)
	service.RegisterXLogHandlers(registry, xlogCache, nil)
	service.RegisterTextHandlers(registry, textCache, nil, nil)

	// Use OS-assigned port: bind a listener first, get the port, close it, then start server on that port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	config := ServerConfig{
		ListenIP:      "127.0.0.1",
		ListenPort:    port,
		ClientTimeout: 5 * time.Second,
	}

	server := NewServer(config, registry, sessions)
	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan struct{})
	go func() {
		// Signal ready once we start listening
		close(ready)
		server.Start(ctx)
	}()
	<-ready
	// Give the server goroutine time to actually start listening
	time.Sleep(50 * time.Millisecond)

	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}
	return addr, cancel, objectCache, counterCache, textCache, xlogCache
}

// clientConn opens a TCP connection to the server and sends the TCP_CLIENT magic.
func clientConn(t *testing.T, addr net.Addr) (*protocol.DataInputX, *protocol.DataOutputX, net.Conn) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr.String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	din := protocol.NewDataInputXStream(reader)
	dout := protocol.NewDataOutputXStream(writer)

	// Send TCP_CLIENT magic (0xCAFE2001 - write as raw 4 bytes)
	dout.Write([]byte{0xCA, 0xFE, 0x20, 0x01})
	dout.Flush()

	return din, dout, conn
}

func doLogin(t *testing.T, din *protocol.DataInputX, dout *protocol.DataOutputX) int64 {
	t.Helper()
	param := &pack.MapPack{}
	param.PutStr("id", "admin")
	param.PutStr("pass", "")

	dout.WriteText(protocol.LOGIN)
	dout.WriteLong(0)
	pack.WritePack(dout, param)
	dout.Flush()

	// Read response
	flag, err := din.ReadByte()
	if err != nil {
		t.Fatal("login read flag error:", err)
	}
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, err := pack.ReadPack(din)
	if err != nil {
		t.Fatal("login read pack error:", err)
	}
	mp := resp.(*pack.MapPack)
	session := mp.GetLong("session")
	if session == 0 {
		t.Fatal("login returned session 0")
	}

	// Read NoNEXT terminator
	flag, _ = din.ReadByte()
	if flag != protocol.FLAG_NO_NEXT {
		t.Fatalf("expected NoNEXT, got %d", flag)
	}

	return session
}

func TestTCP_ServerVersion(t *testing.T) {
	addr, cancel, _, _, _, _ := startTestServer(t)
	defer cancel()

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	// SERVER_VERSION is a free command (no login needed)
	param := &pack.MapPack{}
	dout.WriteText(protocol.SERVER_VERSION)
	dout.WriteLong(0)
	pack.WritePack(dout, param)
	dout.Flush()

	flag, err := din.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, err := pack.ReadPack(din)
	if err != nil {
		t.Fatal(err)
	}
	mp := resp.(*pack.MapPack)
	ver := mp.GetText("version")
	if ver != testVersion {
		t.Fatalf("expected version %s, got %s", testVersion, ver)
	}

	// Read NoNEXT
	flag, _ = din.ReadByte()
	if flag != protocol.FLAG_NO_NEXT {
		t.Fatalf("expected NoNEXT, got %d", flag)
	}
}

func TestTCP_ServerTime(t *testing.T) {
	addr, cancel, _, _, _, _ := startTestServer(t)
	defer cancel()

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	before := time.Now().UnixMilli()

	dout.WriteText(protocol.SERVER_TIME)
	dout.WriteLong(0)
	dout.Flush()

	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, _ := pack.ReadPack(din)
	mp := resp.(*pack.MapPack)
	serverTime := mp.GetLong("time")

	if serverTime < before {
		t.Fatalf("server time %d < before %d", serverTime, before)
	}

	din.ReadByte() // NoNEXT
}

func TestTCP_Login(t *testing.T) {
	addr, cancel, _, _, _, _ := startTestServer(t)
	defer cancel()

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)
	if session == 0 {
		t.Fatal("expected non-zero session")
	}
}

func TestTCP_InvalidSession(t *testing.T) {
	addr, cancel, _, _, _, _ := startTestServer(t)
	defer cancel()

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	// Try a non-free command without logging in
	dout.WriteText(protocol.OBJECT_LIST_REAL_TIME)
	dout.WriteLong(12345) // invalid session
	dout.Flush()

	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_INVALID_SESSION {
		t.Fatalf("expected INVALID_SESSION, got %d", flag)
	}
}

func TestTCP_ObjectListRealTime(t *testing.T) {
	addr, cancel, objectCache, _, _, _ := startTestServer(t)
	defer cancel()

	// Add some objects
	objectCache.Put(1, &pack.ObjectPack{ObjHash: 1, ObjName: "/app1", ObjType: "java", Alive: true})
	objectCache.Put(2, &pack.ObjectPack{ObjHash: 2, ObjName: "/app2", ObjType: "java", Alive: true})

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)

	dout.WriteText(protocol.OBJECT_LIST_REAL_TIME)
	dout.WriteLong(session)
	dout.Flush()

	// Read objects
	count := 0
	for {
		flag, err := din.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		if flag == protocol.FLAG_NO_NEXT {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Fatalf("unexpected flag %d", flag)
		}
		pk, err := pack.ReadPack(din)
		if err != nil {
			t.Fatal(err)
		}
		_ = pk.(*pack.ObjectPack)
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 objects, got %d", count)
	}
}

func TestTCP_CounterRealTime(t *testing.T) {
	addr, cancel, _, counterCache, _, _ := startTestServer(t)
	defer cancel()

	counterCache.Put(cache.CounterKey{ObjHash: 100, Counter: "TPS", TimeType: cache.TimeTypeRealtime}, value.NewDecimalValue(42))

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)

	param := &pack.MapPack{}
	param.PutLong("objHash", 100)
	param.PutStr("counter", "TPS")

	dout.WriteText(protocol.COUNTER_REAL_TIME)
	dout.WriteLong(session)
	pack.WritePack(dout, param)
	dout.Flush()

	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	v, err := value.ReadValue(din)
	if err != nil {
		t.Fatal(err)
	}
	dv := v.(*value.DecimalValue)
	if dv.Value != 42 {
		t.Fatalf("expected 42, got %d", dv.Value)
	}

	din.ReadByte() // NoNEXT
}

func TestTCP_CounterRealTimeAll(t *testing.T) {
	addr, cancel, objectCache, counterCache, _, _ := startTestServer(t)
	defer cancel()

	objectCache.Put(10, &pack.ObjectPack{ObjHash: 10, ObjName: "/a", ObjType: "java", Alive: true})
	objectCache.Put(20, &pack.ObjectPack{ObjHash: 20, ObjName: "/b", ObjType: "java", Alive: true})
	counterCache.Put(cache.CounterKey{ObjHash: 10, Counter: "TPS", TimeType: cache.TimeTypeRealtime}, value.NewDecimalValue(5))
	counterCache.Put(cache.CounterKey{ObjHash: 20, Counter: "TPS", TimeType: cache.TimeTypeRealtime}, value.NewDecimalValue(10))

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)

	param := &pack.MapPack{}
	param.PutStr("counter", "TPS")
	param.PutStr("objType", "java")

	dout.WriteText(protocol.COUNTER_REAL_TIME_ALL)
	dout.WriteLong(session)
	pack.WritePack(dout, param)
	dout.Flush()

	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, _ := pack.ReadPack(din)
	mp := resp.(*pack.MapPack)

	objHashVal := mp.Get("objHash")
	if objHashVal == nil {
		t.Fatal("missing objHash in response")
	}
	lv := objHashVal.(*value.ListValue)
	if len(lv.Value) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(lv.Value))
	}

	din.ReadByte() // NoNEXT
}

func TestTCP_GetText100(t *testing.T) {
	addr, cancel, _, _, textCache, _ := startTestServer(t)
	defer cancel()

	textCache.Put("service", 100, "/api/users")
	textCache.Put("service", 200, "/api/orders")

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)

	hashList := value.NewListValue()
	hashList.Value = append(hashList.Value, value.NewDecimalValue(100))
	hashList.Value = append(hashList.Value, value.NewDecimalValue(200))
	hashList.Value = append(hashList.Value, value.NewDecimalValue(999)) // doesn't exist

	param := &pack.MapPack{}
	param.PutStr("type", "service")
	param.Put("hash", hashList)

	dout.WriteText(protocol.GET_TEXT_100)
	dout.WriteLong(session)
	pack.WritePack(dout, param)
	dout.Flush()

	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}

	resp, _ := pack.ReadPack(din)
	mp := resp.(*pack.MapPack)

	if len(mp.Table) != 2 {
		t.Fatalf("expected 2 text entries, got %d", len(mp.Table))
	}

	din.ReadByte() // NoNEXT
}

func TestTCP_TransRealTimeGroup(t *testing.T) {
	addr, cancel, _, _, _, xlogCache := startTestServer(t)
	defer cancel()

	// Put some XLog entries
	o := protocol.NewDataOutputX()
	xp := &pack.XLogPack{ObjHash: 1, Elapsed: 100, Txid: 111, EndTime: 1000}
	pack.WritePack(o, xp)
	xlogCache.Put(1, 100, false, o.ToByteArray())

	o2 := protocol.NewDataOutputX()
	xp2 := &pack.XLogPack{ObjHash: 2, Elapsed: 200, Txid: 222, EndTime: 2000}
	pack.WritePack(o2, xp2)
	xlogCache.Put(2, 200, false, o2.ToByteArray())

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)

	param := &pack.MapPack{}
	param.PutLong("index", 0)
	param.PutLong("loop", 0)
	param.PutLong("limit", 100)

	dout.WriteText(protocol.TRANX_REAL_TIME_GROUP)
	dout.WriteLong(session)
	pack.WritePack(dout, param)
	dout.Flush()

	// First packet: metadata
	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatalf("expected HasNEXT, got %d", flag)
	}
	meta, _ := pack.ReadPack(din)
	mp := meta.(*pack.MapPack)
	_ = mp.GetLong("index") // should be 2

	// Read XLog entries
	count := 0
	for {
		flag, err := din.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		if flag == protocol.FLAG_NO_NEXT {
			break
		}
		if flag != protocol.FLAG_HAS_NEXT {
			t.Fatalf("unexpected flag %d", flag)
		}
		pk, err := pack.ReadPack(din)
		if err != nil {
			t.Fatal(err)
		}
		_ = pk.(*pack.XLogPack)
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 XLogs, got %d", count)
	}
}

func TestTCP_MultipleCommands(t *testing.T) {
	addr, cancel, objectCache, _, _, _ := startTestServer(t)
	defer cancel()

	objHash := util.HashString("/test")
	objectCache.Put(objHash, &pack.ObjectPack{ObjHash: objHash, ObjName: "/test", ObjType: "java", Alive: true})

	din, dout, conn := clientConn(t, addr)
	defer conn.Close()

	session := doLogin(t, din, dout)

	// Command 1: SERVER_VERSION
	dout.WriteText(protocol.SERVER_VERSION)
	dout.WriteLong(session)
	(&pack.MapPack{}).Write(dout)
	dout.Flush()

	flag, _ := din.ReadByte()
	if flag != protocol.FLAG_HAS_NEXT {
		t.Fatal("expected HasNEXT for version")
	}
	pack.ReadPack(din)
	din.ReadByte() // NoNEXT

	// Command 2: OBJECT_LIST_REAL_TIME
	dout.WriteText(protocol.OBJECT_LIST_REAL_TIME)
	dout.WriteLong(session)
	dout.Flush()

	count := 0
	for {
		flag, _ = din.ReadByte()
		if flag == protocol.FLAG_NO_NEXT {
			break
		}
		pack.ReadPack(din)
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 object, got %d", count)
	}
}
