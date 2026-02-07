package service

import (
	"log/slog"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// AgentCaller is the interface for making RPC calls to agents via TCP.
// This decouples the service package from the tcp package to avoid circular imports.
type AgentCaller interface {
	AgentCallSingle(objHash int32, cmd string, param *pack.MapPack) *pack.MapPack
	AgentCallStream(objHash int32, cmd string, param *pack.MapPack, handler func(pack.Pack))
}

// RegisterAgentProxyHandlers registers handlers that proxy client commands to agents
// through the TCP agent connection pool and return the agent responses.
func RegisterAgentProxyHandlers(r *Registry, caller AgentCaller) {
	simpleProxyCmds := []string{
		protocol.OBJECT_THREAD_LIST,
		protocol.OBJECT_THREAD_DETAIL,
		protocol.OBJECT_THREAD_CONTROL,
		protocol.OBJECT_ENV,
		protocol.OBJECT_ACTIVE_SERVICE_LIST,
		protocol.OBJECT_HEAPHISTO,
		protocol.OBJECT_THREAD_DUMP,
		protocol.OBJECT_SYSTEM_GC,
		protocol.OBJECT_STAT_LIST,
		protocol.OBJECT_CLASS_LIST,
		protocol.OBJECT_CLASS_DESC,
		protocol.OBJECT_RESET_CACHE,
		protocol.OBJECT_DUMP_FILE_LIST,
		protocol.OBJECT_DUMP_FILE_DETAIL,
		protocol.OBJECT_CALL_HEAP_DUMP,
		protocol.OBJECT_FILE_SOCKET,
		protocol.HOST_TOP,
		protocol.HOST_PROCESS_DETAIL,
		protocol.HOST_DISK_USAGE,
		protocol.HOST_NET_STAT,
		protocol.HOST_WHO,
		protocol.HOST_MEMINFO,
		protocol.GET_CONFIGURE_WAS,
		protocol.SET_CONFIGURE_WAS,
	}

	for _, cmd := range simpleProxyCmds {
		registerSimpleProxy(r, caller, cmd)
	}
}

// registerSimpleProxy registers a handler that reads a MapPack from the client,
// extracts the objHash, forwards the command to the target agent, and writes
// the agent response back to the client.
func registerSimpleProxy(r *Registry, caller AgentCaller, cmd string) {
	r.Register(cmd, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			slog.Debug("agent proxy: read param error", "cmd", cmd, "error", err)
			return
		}
		param, ok := pk.(*pack.MapPack)
		if !ok {
			slog.Debug("agent proxy: unexpected pack type", "cmd", cmd)
			return
		}

		objHash := param.GetInt("objHash")
		if objHash == 0 {
			slog.Debug("agent proxy: missing objHash", "cmd", cmd)
			return
		}

		result := caller.AgentCallSingle(objHash, cmd, param)
		if result != nil {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})
}
