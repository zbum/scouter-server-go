package udp

import (
	"log/slog"
	"net"

	"github.com/zbum/scouter-server-go/internal/core"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// NetDataProcessor handles incoming UDP data, parses frames, and dispatches packs.
type NetDataProcessor struct {
	multiPacket *MultiPacketProcessor
	dispatcher  *core.Dispatcher
	queue       chan netData
	workers     int
}

type netData struct {
	data []byte
	addr *net.UDPAddr
}

func NewNetDataProcessor(dispatcher *core.Dispatcher, workers int) *NetDataProcessor {
	if workers <= 0 {
		workers = 2
	}
	p := &NetDataProcessor{
		multiPacket: NewMultiPacketProcessor(),
		dispatcher:  dispatcher,
		queue:       make(chan netData, 2048),
		workers:     workers,
	}
	for i := 0; i < workers; i++ {
		go p.workerLoop()
	}
	return p
}

func (p *NetDataProcessor) Add(data []byte, addr *net.UDPAddr) {
	select {
	case p.queue <- netData{data: data, addr: addr}:
	default:
		slog.Warn("UDP receive queue overflow, dropping packet")
	}
}

func (p *NetDataProcessor) workerLoop() {
	for nd := range p.queue {
		p.process(nd)
	}
}

func (p *NetDataProcessor) process(nd netData) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in UDP processor", "error", r)
		}
	}()

	d := protocol.NewDataInputX(nd.data)
	cafe, err := d.ReadInt32()
	if err != nil {
		slog.Warn("failed to read UDP magic", "error", err)
		return
	}

	switch cafe {
	case protocol.UDP_CAFE, protocol.UDP_JAVA:
		p.processCafe(d, nd.addr)
	case protocol.UDP_CAFE_N, protocol.UDP_JAVA_N:
		p.processCafeN(d, nd.addr)
	case protocol.UDP_CAFE_MTU, protocol.UDP_JAVA_MTU:
		p.processCafeMTU(d, nd.addr)
	default:
		slog.Warn("unknown UDP magic", "magic", cafe, "len", len(nd.data), "addr", nd.addr)
	}
}

func (p *NetDataProcessor) processCafe(d *protocol.DataInputX, addr *net.UDPAddr) {
	pk, err := pack.ReadPack(d)
	if err != nil {
		slog.Warn("failed to read pack", "error", err)
		return
	}
	p.dispatcher.Dispatch(pk, addr)
}

func (p *NetDataProcessor) processCafeN(d *protocol.DataInputX, addr *net.UDPAddr) {
	n, err := d.ReadInt16()
	if err != nil {
		slog.Warn("failed to read pack count", "error", err)
		return
	}
	for i := int16(0); i < n; i++ {
		pk, err := pack.ReadPack(d)
		if err != nil {
			slog.Warn("failed to read pack in multi-frame", "index", i, "error", err)
			return
		}
		p.dispatcher.Dispatch(pk, addr)
	}
}

func (p *NetDataProcessor) processCafeMTU(d *protocol.DataInputX, addr *net.UDPAddr) {
	objHash, err := d.ReadInt32()
	if err != nil {
		return
	}
	pkid, err := d.ReadInt64()
	if err != nil {
		return
	}
	total, err := d.ReadInt16()
	if err != nil {
		return
	}
	num, err := d.ReadInt16()
	if err != nil {
		return
	}
	data, err := d.ReadBlob()
	if err != nil {
		return
	}

	done := p.multiPacket.Add(pkid, total, num, data, objHash)
	if done != nil {
		rd := protocol.NewDataInputX(done)
		pk, err := pack.ReadPack(rd)
		if err != nil {
			slog.Warn("failed to read reassembled pack", "error", err)
			return
		}
		p.dispatcher.Dispatch(pk, addr)
	}
}

func (p *NetDataProcessor) Close() {
	close(p.queue)
}
