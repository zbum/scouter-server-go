package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/util"
)

// AgentManager handles agent/object registration and heartbeat tracking.
type AgentManager struct {
	objectCache *cache.ObjectCache
	deadTimeout time.Duration
}

func NewAgentManager(objectCache *cache.ObjectCache, deadTimeout time.Duration) *AgentManager {
	am := &AgentManager{
		objectCache: objectCache,
		deadTimeout: deadTimeout,
	}
	go am.monitorLoop()
	return am
}

func (am *AgentManager) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		op, ok := p.(*pack.ObjectPack)
		if !ok {
			return
		}
		if op.ObjHash == 0 && op.ObjName != "" {
			op.ObjHash = util.HashString(op.ObjName)
		}
		if op.Address == "" && addr != nil {
			op.Address = addr.IP.String()
		}
		op.Alive = true

		am.objectCache.Put(op.ObjHash, op)

		slog.Debug("Agent active",
			"objName", op.ObjName,
			"objHash", op.ObjHash,
			"objType", op.ObjType,
			"addr", op.Address)
	}
}

func (am *AgentManager) monitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		dead := am.objectCache.MarkDead(am.deadTimeout)
		for _, d := range dead {
			slog.Info("Agent inactive",
				"objName", d.Pack.ObjName,
				"objHash", d.Pack.ObjHash)
		}
	}
}
