package service

import (
	"os"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterLoginHandlers registers LOGIN and related handlers.
func RegisterLoginHandlers(r *Registry, sessions *login.SessionManager, accountManager *login.AccountManager, version string) {
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

			serverID := getHostname()
			soTimeout := int64(8000)
			if cfg := config.Get(); cfg != nil {
				serverID = cfg.GetString("server_id", serverID)
				soTimeout = int64(cfg.NetTcpClientSoTimeoutMs())
			}
			m.PutStr("server_id", serverID)

			// Set group type and email from account
			groupType := "default"
			if accountManager != nil {
				if acct := accountManager.GetAccount(id); acct != nil {
					groupType = acct.Group
					m.PutStr("email", acct.Email)
				}
				// Include group policy in login response
				if policy := accountManager.GetGroupPolicy(groupType); policy != nil {
					m.Put("policy", policy)
				}
			}
			m.PutStr("type", groupType)

			m.PutStr("version", version)

			tz, _ := time.Now().Zone()
			m.PutStr("timezone", tz)
			m.PutLong("so_time_out", soTimeout)

			menuMap := value.NewMapValue()
			menuMap.Put("tag_count", &value.BooleanValue{Value: true})
			m.Put("menu", menuMap)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, m)
	})

	// CHECK_SESSION: Validate an existing session.
	r.Register(protocol.CHECK_SESSION, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)
		session := m.GetLong("session")

		valid := sessions.OkSession(session)
		if valid {
			m.PutLong("validSession", 1)
		} else {
			m.PutLong("validSession", 0)
			m.PutStr("error", "login fail")
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, m)
	})
}

// RegisterLoginExtHandlers registers CHECK_LOGIN and GET_LOGIN_LIST handlers.
func RegisterLoginExtHandlers(r *Registry, sessions *login.SessionManager, accountManager *login.AccountManager) {

	// CHECK_LOGIN: verify user credentials without creating a session.
	r.Register(protocol.CHECK_LOGIN, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		m := pk.(*pack.MapPack)
		id := m.GetText("id")
		pass := m.GetText("pass")

		ok := false
		if accountManager != nil {
			ok = accountManager.AuthorizeAccount(id, pass)
		}

		result := &pack.MapPack{}
		result.Put("result", &value.BooleanValue{Value: ok})

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, result)
	})

	// GET_LOGIN_LIST: return list of currently logged-in users.
	r.Register(protocol.GET_LOGIN_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, loggedIn bool) {
		pack.ReadPack(din)

		users := sessions.GetAllUsers()

		resp := &pack.MapPack{}
		sessionList := value.NewListValue()
		userList := value.NewListValue()
		ipList := value.NewListValue()
		loginTimeList := value.NewListValue()
		verList := value.NewListValue()
		hostList := value.NewListValue()

		now := time.Now()
		for _, u := range users {
			sessionList.Value = append(sessionList.Value, value.NewDecimalValue(u.Session))
			userList.Value = append(userList.Value, value.NewTextValue(u.ID))
			ipList.Value = append(ipList.Value, value.NewTextValue(u.IP))
			loginTimeList.Value = append(loginTimeList.Value, value.NewDecimalValue(int64(now.Sub(u.LoginTime).Seconds())))
			verList.Value = append(verList.Value, value.NewTextValue(u.Version))
			hostList.Value = append(hostList.Value, value.NewTextValue(u.Hostname))
		}

		resp.Put("session", sessionList)
		resp.Put("user", userList)
		resp.Put("ip", ipList)
		resp.Put("logintime", loginTimeList)
		resp.Put("ver", verList)
		resp.Put("host", hostList)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})
}

func getHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "scouter-go"
	}
	return h
}
