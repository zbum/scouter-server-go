package core

import (
	"log/slog"
	"net"

	"github.com/zbum/scouter-server-go/internal/config"
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

	packType := p.PackType()

	// Per-type debug logging controlled by config flags
	if cfg := config.Get(); cfg != nil {
		logUDPPack(cfg, packType, addr)
	}

	h, ok := d.handlers[packType]
	if ok {
		h(p, addr)
	} else {
		slog.Debug("no handler for pack type", "type", packType)
	}
}

// logUDPPack logs pack reception when the corresponding config flag is enabled.
func logUDPPack(cfg *config.Config, packType byte, addr *net.UDPAddr) {
	var enabled bool
	var typeName string

	switch packType {
	case pack.PackTypeXLog, pack.PackTypeDroppedXLog:
		enabled = cfg.LogUDPXLog()
		typeName = "xlog"
	case pack.PackTypeXLogProfile, pack.PackTypeXLogProfile2:
		enabled = cfg.LogUDPProfile()
		typeName = "profile"
	case pack.PackTypeText:
		enabled = cfg.LogUDPText()
		typeName = "text"
	case pack.PackTypePerfCounter:
		enabled = cfg.LogUDPCounter()
		typeName = "counter"
	case pack.PackTypeObject:
		enabled = cfg.LogUDPObject()
		typeName = "object"
	case pack.PackTypeAlert:
		enabled = cfg.LogUDPAlert()
		typeName = "alert"
	case pack.PackTypeSummary:
		enabled = cfg.LogUDPSummary()
		typeName = "summary"
	case pack.PackTypeBatch:
		enabled = cfg.LogUDPBatch()
		typeName = "batch"
	case pack.PackTypeSpan, pack.PackTypeSpanContainer:
		enabled = cfg.LogUDPSpan()
		typeName = "span"
	case pack.PackTypeStack:
		enabled = cfg.LogUDPStack()
		typeName = "stack"
	case pack.PackTypePerfStatus:
		enabled = cfg.LogUDPStatus()
		typeName = "status"
	case pack.PackTypePerfInteractionCounter:
		enabled = cfg.LogUDPInteractionCounter()
		typeName = "interaction_counter"
	}

	if enabled {
		slog.Info("UDP pack received", "type", typeName, "packType", packType, "addr", addr)
	}
}
