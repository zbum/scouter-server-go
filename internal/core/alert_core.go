package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// AlertCore processes incoming AlertPack data.
type AlertCore struct {
	queue      chan *pack.AlertPack
	alertWR    *alert.AlertWR
	alertCache *cache.AlertCache
}

func NewAlertCore(alertWR *alert.AlertWR, alertCache *cache.AlertCache) *AlertCore {
	ac := &AlertCore{
		queue:      make(chan *pack.AlertPack, 1024),
		alertWR:    alertWR,
		alertCache: alertCache,
	}
	go ac.run()
	return ac
}

func (ac *AlertCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		ap, ok := p.(*pack.AlertPack)
		if !ok {
			return
		}
		if ap.Time == 0 {
			ap.Time = time.Now().UnixMilli()
		}
		ac.Add(ap)
	}
}

// Add enqueues an AlertPack for processing (usable by AgentManager too).
func (ac *AlertCore) Add(ap *pack.AlertPack) {
	select {
	case ac.queue <- ap:
	default:
		slog.Warn("AlertCore queue overflow")
	}
}

func (ac *AlertCore) run() {
	for ap := range ac.queue {
		slog.Debug("AlertCore processing",
			"objHash", ap.ObjHash,
			"title", ap.Title)

		o := protocol.NewDataOutputX()
		pack.WritePack(o, ap)
		data := o.ToByteArray()

		// Add to real-time cache for ALERT_REAL_TIME delivery
		if ac.alertCache != nil {
			ac.alertCache.Add(data)
		}

		// Persist to disk
		if ac.alertWR != nil {
			ac.alertWR.Add(&alert.AlertEntry{
				TimeMs: ap.Time,
				Data:   data,
			})
		}
	}
}
