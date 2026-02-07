package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterLoginHandlers registers LOGIN and related handlers.
func RegisterLoginHandlers(r *Registry, sessions *login.SessionManager, version string) {
	r.Register(protocol.LOGIN, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)

		id := m.GetText("id")
		pass := m.GetText("pass")
		ip := m.GetText("ip")
		hostname := m.GetText("hostname")
		clientVer := m.GetText("version")

		session := sessions.Login(id, pass, ip)

		m.PutLong("session", session)
		if session == 0 {
			m.PutStr("error", "login fail")
		} else {
			user := sessions.GetUser(session)
			if user != nil {
				user.Hostname = hostname
				user.Version = clientVer
			}
			m.PutLong("time", time.Now().UnixMilli())
			m.PutStr("server_id", "scouter-go")
			m.PutStr("type", "default")
			m.PutStr("version", version)
			m.Put("menu", &value.MapValue{})
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, m)
	})
}
