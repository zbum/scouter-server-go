package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/db/summary"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// SummaryCore processes incoming SummaryPack data.
type SummaryCore struct {
	queue     chan *pack.SummaryPack
	summaryWR *summary.SummaryWR
}

func NewSummaryCore(summaryWR *summary.SummaryWR) *SummaryCore {
	sc := &SummaryCore{
		queue:     make(chan *pack.SummaryPack, 1024),
		summaryWR: summaryWR,
	}
	go sc.run()
	return sc
}

func (sc *SummaryCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		sp, ok := p.(*pack.SummaryPack)
		if !ok {
			return
		}
		if sp.Time == 0 {
			sp.Time = time.Now().UnixMilli()
		}
		select {
		case sc.queue <- sp:
		default:
			slog.Warn("SummaryCore queue overflow")
		}
	}
}

func (sc *SummaryCore) run() {
	for sp := range sc.queue {
		slog.Debug("SummaryCore processing",
			"objHash", sp.ObjHash,
			"objType", sp.ObjType,
			"stype", sp.SType,
			"time", sp.Time)

		if sc.summaryWR != nil {
			o := protocol.NewDataOutputX()
			pack.WritePack(o, sp)
			sc.summaryWR.Add(&summary.SummaryEntry{
				TimeMs: sp.Time,
				SType:  sp.SType,
				Data:   o.ToByteArray(),
			})
		}
	}
}
