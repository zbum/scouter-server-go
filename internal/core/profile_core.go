package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// ProfileCore processes incoming XLogProfilePack data.
type ProfileCore struct {
	profileWR *profile.ProfileWR
	queue     chan *pack.XLogProfilePack
}

func NewProfileCore(profileWR *profile.ProfileWR) *ProfileCore {
	pc := &ProfileCore{
		profileWR: profileWR,
		queue:     make(chan *pack.XLogProfilePack, 4096),
	}
	go pc.run()
	return pc
}

func (pc *ProfileCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		switch pp := p.(type) {
		case *pack.XLogProfilePack:
			if pp.Time == 0 {
				pp.Time = time.Now().UnixMilli()
			}
			select {
			case pc.queue <- pp:
			default:
				slog.Warn("ProfileCore queue overflow")
			}
		case *pack.XLogProfilePack2:
			// XLogProfilePack2 embeds XLogProfilePack, convert
			t := pp.Time
			if t == 0 {
				t = time.Now().UnixMilli()
			}
			converted := &pack.XLogProfilePack{
				Time:    t,
				ObjHash: pp.ObjHash,
				Service: pp.Service,
				Txid:    pp.Txid,
				Profile: pp.Profile,
			}
			select {
			case pc.queue <- converted:
			default:
				slog.Warn("ProfileCore queue overflow")
			}
		}
	}
}

func (pc *ProfileCore) run() {
	for pp := range pc.queue {
		if pc.profileWR != nil {
			pc.profileWR.Add(&profile.ProfileEntry{
				TimeMs: pp.Time,
				Txid:   pp.Txid,
				Data:   pp.Profile,
			})
		}
		slog.Debug("ProfileCore processing", "txid", pp.Txid, "profileLen", len(pp.Profile))
	}
}
