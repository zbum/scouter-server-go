package core

import (
	"log/slog"
	"net"

	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// PackHandler processes a single pack received from the network.
type PackHandler func(p pack.Pack, addr *net.UDPAddr)

// Dispatcher routes incoming packs to registered handlers by pack type.
type Dispatcher struct {
	handlers map[byte]PackHandler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers: make(map[byte]PackHandler),
	}
}

// Register associates a handler with a pack type code.
func (d *Dispatcher) Register(packType byte, handler PackHandler) {
	d.handlers[packType] = handler
}

// Dispatch routes a pack to its registered handler.
func (d *Dispatcher) Dispatch(p pack.Pack, addr *net.UDPAddr) {
	if p == nil {
		return
	}
	h, ok := d.handlers[p.GetPackType()]
	if ok {
		h(p, addr)
	} else {
		slog.Debug("no handler for pack type", "type", p.GetPackType())
	}
}
