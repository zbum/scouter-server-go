package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// AlertCore processes incoming AlertPack data.
type AlertCore struct {
	queue   chan *pack.AlertPack
	alertWR *alert.AlertWR
}

func NewAlertCore(alertWR *alert.AlertWR) *AlertCore {
	ac := &AlertCore{
		queue:   make(chan *pack.AlertPack, 1024),
		alertWR: alertWR,
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
		select {
		case ac.queue <- ap:
		default:
			slog.Warn("AlertCore queue overflow")
		}
	}
}

func (ac *AlertCore) run() {
	for ap := range ac.queue {
		slog.Debug("AlertCore processing",
			"objHash", ap.ObjHash,
			"objType", ap.ObjType,
			"level", ap.Level,
			"title", ap.Title)

		if ac.alertWR != nil {
			o := protocol.NewDataOutputX()
			pack.WritePack(o, ap)
			ac.alertWR.Add(&alert.AlertEntry{
				TimeMs: ap.Time,
				Data:   o.ToByteArray(),
			})
		}
	}
}
