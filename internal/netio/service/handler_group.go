package service

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/zbum/scouter-server-go/internal/core"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterGroupHandlers registers the REALTIME_SERVICE_GROUP handler.
func RegisterGroupHandlers(r *Registry, xlogGroupPerf *core.XLogGroupPerf, textCache *cache.TextCache) {
	r.Register(protocol.REALTIME_SERVICE_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param, ok := pk.(*pack.MapPack)
		if !ok {
			return
		}

		// Build object hash filter from request
		objHashLv := param.GetList("objHash")
		objHashes := make(map[int32]bool)
		if objHashLv != nil {
			for _, v := range objHashLv.Value {
				if dv, ok := v.(*value.DecimalValue); ok {
					objHashes[int32(dv.Value)] = true
				}
			}
		}
		if len(objHashes) == 0 {
			return
		}

		// Get aggregated per-group performance stats
		groupStats := xlogGroupPerf.GetGroupPerfStat(objHashes)
		if len(groupStats) == 0 {
			return
		}

		// Build response MapPack with parallel lists
		resp := &pack.MapPack{}
		nameLv := value.NewListValue()
		countLv := value.NewListValue()
		elapsedLv := value.NewListValue()
		errorLv := value.NewListValue()

		for groupHash, stat := range groupStats {
			name, found := textCache.Get("group", groupHash)
			if !found || name == "" {
				name = "unknown"
			}
			nameLv.Value = append(nameLv.Value, value.NewTextValue(name))
			countLv.Value = append(countLv.Value, value.NewDecimalValue(stat.Count))
			elapsedLv.Value = append(elapsedLv.Value, &value.FloatValue{Value: stat.AvgElapsed()})
			errorLv.Value = append(errorLv.Value, value.NewDecimalValue(stat.Error))
		}

		resp.Put("name", nameLv)
		resp.Put("count", countLv)
		resp.Put("elapsed", elapsedLv)
		resp.Put("error", errorLv)

		// Diagnostic: log what we're sending to the client
		var parts []string
		for i := 0; i < len(nameLv.Value); i++ {
			tv := nameLv.Value[i].(*value.TextValue)
			dv := countLv.Value[i].(*value.DecimalValue)
			fv := elapsedLv.Value[i].(*value.FloatValue)
			parts = append(parts, fmt.Sprintf("%s(count=%d,elapsed=%.1f)", tv.Value, dv.Value, fv.Value))
		}
		slog.Info("REALTIME_SERVICE_GROUP response", "groups", strings.Join(parts, ", "), "objCount", len(objHashes))

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})
}
