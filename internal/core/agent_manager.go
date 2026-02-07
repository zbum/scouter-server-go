package core

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/counter"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/util"
)

// AgentManager handles agent/object registration and heartbeat tracking.
type AgentManager struct {
	objectCache *cache.ObjectCache
	textCache   *cache.TextCache
	textCore    *TextCore
	alertCore   *AlertCore
	deadTimeout time.Duration
	typeManager *counter.ObjectTypeManager
}

func NewAgentManager(objectCache *cache.ObjectCache, deadTimeout time.Duration, typeManager *counter.ObjectTypeManager, textCache *cache.TextCache, textCore *TextCore, alertCore *AlertCore) *AgentManager {
	am := &AgentManager{
		objectCache: objectCache,
		textCache:   textCache,
		textCore:    textCore,
		alertCore:   alertCore,
		deadTimeout: deadTimeout,
		typeManager: typeManager,
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

		// Check if this agent was previously dead (for ACTIVATED_OBJECT alert)
		wasDead := false
		if existing, ok := am.objectCache.Get(op.ObjHash); ok {
			wasDead = !existing.Pack.Alive
		}

		op.Alive = true
		op.Wakeup = time.Now().UnixMilli()

		if am.typeManager != nil {
			am.typeManager.AddObjectTypeIfNotExist(op.ObjType, op.Tags)
		}

		am.objectCache.Put(op.ObjHash, op)

		// Generate ACTIVATED_OBJECT alert if agent was previously dead
		if wasDead && am.alertCore != nil {
			am.alertCore.Add(&pack.AlertPack{
				Time:    time.Now().UnixMilli(),
				Level:   0, // INFO
				ObjType: "scouter",
				ObjHash: op.ObjHash,
				Title:   "ACTIVATED_OBJECT",
				Message: fmt.Sprintf("%s is running now.", op.ObjName),
			})
			slog.Info("Agent reactivated", "objName", op.ObjName, "objHash", op.ObjHash)
		}

		// Store objName in text cache so clients can resolve via GET_TEXT_100 type="object"
		// Java: AgentManager.procObjName()
		if op.ObjName != "" {
			if am.textCache != nil {
				am.textCache.Put("object", op.ObjHash, op.ObjName)
			}
			if am.textCore != nil {
				am.textCore.AddText("object", op.ObjHash, op.ObjName)
			}
		}

		slog.Debug("Agent heartbeat",
			"objName", op.ObjName,
			"objHash", op.ObjHash)
	}
}

func (am *AgentManager) monitorLoop() {
	slog.Info("AgentManager monitorLoop started", "deadTimeout", am.deadTimeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		dead := am.objectCache.MarkDead(am.deadTimeout)
		for _, d := range dead {
			slog.Info("Agent inactive",
				"objName", d.Pack.ObjName,
				"objHash", d.Pack.ObjHash)

			// Generate INACTIVE_OBJECT alert
			if am.alertCore != nil {
				am.alertCore.Add(&pack.AlertPack{
					Time:    time.Now().UnixMilli(),
					Level:   0, // INFO
					ObjType: "scouter",
					ObjHash: d.Pack.ObjHash,
					Title:   "INACTIVE_OBJECT",
					Message: fmt.Sprintf("%s is not running.", d.Pack.ObjName),
				})
			}
		}
	}
}
