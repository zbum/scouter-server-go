package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/util"
)

// AgentCaller sends commands to agents via TCP.
type AgentCaller interface {
	AgentCallSingle(objHash int32, cmd string, param *pack.MapPack) *pack.MapPack
}

// TextCacheReset sends OBJECT_RESET_CACHE to all live agents when the date
// changes, matching Scala's TextCacheReset. This forces agents to re-send
// all text mappings (service, sql, method, etc.).
type TextCacheReset struct {
	objectCache *cache.ObjectCache
	deadTimeout time.Duration
	caller      AgentCaller
}

func NewTextCacheReset(objectCache *cache.ObjectCache, deadTimeout time.Duration, caller AgentCaller) *TextCacheReset {
	return &TextCacheReset{
		objectCache: objectCache,
		deadTimeout: deadTimeout,
		caller:      caller,
	}
}

// Start begins the background date-change watcher.
func (t *TextCacheReset) Start(ctx context.Context) {
	go func() {
		oldDate := util.FormatDate(time.Now().UnixMilli())
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				curDate := util.FormatDate(time.Now().UnixMilli())
				if curDate != oldDate {
					oldDate = curDate
					t.resetAllAgents()
				}
			}
		}
	}()
}

func (t *TextCacheReset) resetAllAgents() {
	liveAgents := t.objectCache.GetLive(t.deadTimeout)
	slog.Info("TextCacheReset: date changed, resetting agent text caches", "agents", len(liveAgents))

	for _, info := range liveAgents {
		objHash := info.Pack.ObjHash
		go func(hash int32) {
			t.caller.AgentCallSingle(hash, protocol.OBJECT_RESET_CACHE, nil)
		}(objHash)
	}
}
