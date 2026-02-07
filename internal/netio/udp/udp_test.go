package udp

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zbum/scouter-server-go/internal/core"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// --- MultiPacketProcessor tests ---

func TestMultiPacketReassembly(t *testing.T) {
	mp := NewMultiPacketProcessor()

	// Simulate 3-part packet
	result := mp.Add(12345, 3, 0, []byte("AAA"), 100)
	if result != nil {
		t.Error("expected nil before all fragments received")
	}

	result = mp.Add(12345, 3, 2, []byte("CCC"), 100)
	if result != nil {
		t.Error("expected nil before all fragments received")
	}

	result = mp.Add(12345, 3, 1, []byte("BBB"), 100)
	if result == nil {
		t.Fatal("expected reassembled data")
	}
	if string(result) != "AAABBBCCC" {
		t.Errorf("expected 'AAABBBCCC', got %q", string(result))
	}
}

func TestMultiPacketDuplicate(t *testing.T) {
	mp := NewMultiPacketProcessor()

	mp.Add(999, 2, 0, []byte("AA"), 1)
	mp.Add(999, 2, 0, []byte("XX"), 1) // duplicate fragment 0

	result := mp.Add(999, 2, 1, []byte("BB"), 1)
	if result == nil {
		t.Fatal("expected reassembled data")
	}
	// First fragment should win (duplicate ignored)
	if string(result) != "AABB" {
		t.Errorf("expected 'AABB', got %q", string(result))
	}
}

func TestMultiPacketSingleFragment(t *testing.T) {
	mp := NewMultiPacketProcessor()

	result := mp.Add(1, 1, 0, []byte("single"), 1)
	if result == nil {
		t.Fatal("expected immediate result for single fragment")
	}
	if string(result) != "single" {
		t.Errorf("expected 'single', got %q", string(result))
	}
}

// --- NetDataProcessor tests ---

func buildCafePacket(p pack.Pack) []byte {
	o := protocol.NewDataOutputX()
	o.WriteInt32(protocol.UDP_CAFE)
	pack.WritePack(o, p)
	return o.ToByteArray()
}

func buildCafeNPacket(packs []pack.Pack) []byte {
	o := protocol.NewDataOutputX()
	o.WriteInt32(protocol.UDP_CAFE_N)
	o.WriteInt16(int16(len(packs)))
	for _, p := range packs {
		pack.WritePack(o, p)
	}
	return o.ToByteArray()
}

func TestProcessorCafe(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var received atomic.Int32
	dispatcher.Register(pack.PackTypeText, func(p pack.Pack, addr *net.UDPAddr) {
		received.Add(1)
		tp, ok := p.(*pack.TextPack)
		if !ok {
			t.Errorf("expected TextPack, got %T", p)
			return
		}
		if tp.Text != "hello" {
			t.Errorf("expected 'hello', got %q", tp.Text)
		}
	})

	proc := NewNetDataProcessor(dispatcher, 1)
	defer proc.Close()

	tp := &pack.TextPack{XType: "service", Hash: 123, Text: "hello"}
	data := buildCafePacket(tp)

	proc.Add(data, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234})
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1 dispatch, got %d", received.Load())
	}
}

func TestProcessorCafeN(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var received atomic.Int32
	dispatcher.Register(pack.PackTypeText, func(p pack.Pack, addr *net.UDPAddr) {
		received.Add(1)
	})

	proc := NewNetDataProcessor(dispatcher, 1)
	defer proc.Close()

	packs := []pack.Pack{
		&pack.TextPack{XType: "a", Hash: 1, Text: "t1"},
		&pack.TextPack{XType: "b", Hash: 2, Text: "t2"},
		&pack.TextPack{XType: "c", Hash: 3, Text: "t3"},
	}
	data := buildCafeNPacket(packs)

	proc.Add(data, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234})
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 3 {
		t.Errorf("expected 3 dispatches, got %d", received.Load())
	}
}

func TestProcessorCafeMTU(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var received atomic.Int32
	dispatcher.Register(pack.PackTypeMap, func(p pack.Pack, addr *net.UDPAddr) {
		received.Add(1)
		mp, ok := p.(*pack.MapPack)
		if !ok {
			t.Errorf("expected MapPack, got %T", p)
			return
		}
		if mp.GetText("key") != "value" {
			t.Errorf("expected 'value', got %q", mp.GetText("key"))
		}
	})

	proc := NewNetDataProcessor(dispatcher, 1)
	defer proc.Close()

	// Build the pack payload
	mp := &pack.MapPack{}
	mp.PutStr("key", "value")
	packOut := protocol.NewDataOutputX()
	pack.WritePack(packOut, mp)
	packBytes := packOut.ToByteArray()

	// Split into 2 MTU fragments
	mid := len(packBytes) / 2
	frag0 := packBytes[:mid]
	frag1 := packBytes[mid:]

	pkid := int64(42)
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}

	// Build MTU packets
	buildMTU := func(num int16, fragData []byte) []byte {
		o := protocol.NewDataOutputX()
		o.WriteInt32(protocol.UDP_CAFE_MTU)
		o.WriteInt32(999) // objHash
		o.WriteInt64(pkid)
		o.WriteInt16(2) // total
		o.WriteInt16(num)
		o.WriteBlob(fragData)
		return o.ToByteArray()
	}

	proc.Add(buildMTU(0, frag0), addr)
	proc.Add(buildMTU(1, frag1), addr)
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1 dispatch after reassembly, got %d", received.Load())
	}
}

func TestProcessorMultipleHandlers(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var textCount, mapCount atomic.Int32
	dispatcher.Register(pack.PackTypeText, func(p pack.Pack, addr *net.UDPAddr) {
		textCount.Add(1)
	})
	dispatcher.Register(pack.PackTypeMap, func(p pack.Pack, addr *net.UDPAddr) {
		mapCount.Add(1)
	})

	proc := NewNetDataProcessor(dispatcher, 2)
	defer proc.Close()

	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}

	// Send a mix of TextPack and MapPack
	for i := 0; i < 10; i++ {
		tp := &pack.TextPack{XType: "svc", Hash: int32(i), Text: "t"}
		proc.Add(buildCafePacket(tp), addr)
	}
	for i := 0; i < 5; i++ {
		mp := &pack.MapPack{}
		mp.PutStr("k", "v")
		proc.Add(buildCafePacket(mp), addr)
	}
	time.Sleep(200 * time.Millisecond)

	if textCount.Load() != 10 {
		t.Errorf("expected 10 TextPacks, got %d", textCount.Load())
	}
	if mapCount.Load() != 5 {
		t.Errorf("expected 5 MapPacks, got %d", mapCount.Load())
	}
}

func TestProcessorUnknownMagic(t *testing.T) {
	dispatcher := core.NewDispatcher()
	proc := NewNetDataProcessor(dispatcher, 1)
	defer proc.Close()

	// Unknown magic should not panic
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	proc.Add(data, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234})
	time.Sleep(50 * time.Millisecond)
}

// --- Integration: concurrent writes ---

func TestProcessorConcurrent(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var received atomic.Int32
	dispatcher.Register(pack.PackTypeText, func(p pack.Pack, addr *net.UDPAddr) {
		received.Add(1)
	})

	proc := NewNetDataProcessor(dispatcher, 4)
	defer proc.Close()

	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
	total := 1000

	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func(idx int) {
			defer wg.Done()
			tp := &pack.TextPack{XType: "svc", Hash: int32(idx), Text: "test"}
			proc.Add(buildCafePacket(tp), addr)
		}(i)
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	r := received.Load()
	if r != int32(total) {
		t.Errorf("expected %d dispatches, got %d", total, r)
	}
}

// --- Dispatcher test ---

func TestDispatcherNoHandler(t *testing.T) {
	d := core.NewDispatcher()
	// Should not panic for unregistered type
	mp := &pack.MapPack{}
	d.Dispatch(mp, nil)
}

func TestDispatcherNilPack(t *testing.T) {
	d := core.NewDispatcher()
	// Should not panic for nil pack
	d.Dispatch(nil, nil)
}

// --- XLogPack round-trip through processor ---

func TestProcessorXLogPack(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var received atomic.Int32
	dispatcher.Register(pack.PackTypeXLog, func(p pack.Pack, addr *net.UDPAddr) {
		received.Add(1)
		xp, ok := p.(*pack.XLogPack)
		if !ok {
			t.Errorf("expected XLogPack, got %T", p)
			return
		}
		if xp.Elapsed != 1500 {
			t.Errorf("expected Elapsed=1500, got %d", xp.Elapsed)
		}
		if xp.Service != 200 {
			t.Errorf("expected Service=200, got %d", xp.Service)
		}
	})

	proc := NewNetDataProcessor(dispatcher, 1)
	defer proc.Close()

	xp := &pack.XLogPack{
		EndTime:  1234567890,
		ObjHash:  100,
		Service:  200,
		Txid:     999888777,
		Elapsed:  1500,
		Cpu:      100,
		SqlCount: 5,
		SqlTime:  200,
		Kbytes:   64,
		Status:   200,
	}

	proc.Add(buildCafePacket(xp), &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5555})
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1, got %d", received.Load())
	}
}

// --- ObjectPack through processor ---

func TestProcessorObjectPack(t *testing.T) {
	dispatcher := core.NewDispatcher()

	var received atomic.Int32
	dispatcher.Register(pack.PackTypeObject, func(p pack.Pack, addr *net.UDPAddr) {
		received.Add(1)
		op, ok := p.(*pack.ObjectPack)
		if !ok {
			t.Errorf("expected ObjectPack, got %T", p)
			return
		}
		if op.ObjName != "TestApp" {
			t.Errorf("expected 'TestApp', got %q", op.ObjName)
		}
	})

	proc := NewNetDataProcessor(dispatcher, 1)
	defer proc.Close()

	op := &pack.ObjectPack{
		ObjType: "java",
		ObjHash: 12345,
		ObjName: "TestApp",
		Address: "10.0.0.1",
		Alive:   true,
		Tags:    value.NewMapValue(),
	}

	proc.Add(buildCafePacket(op), &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5555})
	time.Sleep(100 * time.Millisecond)

	if received.Load() != 1 {
		t.Errorf("expected 1, got %d", received.Load())
	}
}
