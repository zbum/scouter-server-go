package service

import (
	"log/slog"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// AgentCaller is the interface for making RPC calls to agents via TCP.
// This decouples the service package from the tcp package to avoid circular imports.
type AgentCaller interface {
	AgentCallSingle(objHash int32, cmd string, param *pack.MapPack) *pack.MapPack
	AgentCallStream(objHash int32, cmd string, param *pack.MapPack, handler func(pack.Pack))
}

// RegisterAgentProxyHandlers registers handlers that proxy client commands to agents
// through the TCP agent connection pool and return the agent responses.
func RegisterAgentProxyHandlers(r *Registry, caller AgentCaller, objectCache *cache.ObjectCache, deadTimeout time.Duration) {
	simpleProxyCmds := []string{
		// Object commands
		protocol.OBJECT_THREAD_LIST,
		protocol.OBJECT_THREAD_DETAIL,
		protocol.OBJECT_THREAD_CONTROL,
		protocol.OBJECT_ENV,
		protocol.OBJECT_HEAPHISTO,
		protocol.OBJECT_THREAD_DUMP,
		protocol.OBJECT_SYSTEM_GC,
		protocol.OBJECT_STAT_LIST,
		protocol.OBJECT_CLASS_LIST,
		protocol.OBJECT_CLASS_DESC,
		protocol.OBJECT_RESET_CACHE,
		protocol.OBJECT_LOAD_CLASS_BY_STREAM,
		protocol.OBJECT_CHECK_RESOURCE_FILE,
		protocol.OBJECT_DOWNLOAD_JAR,
		protocol.OBJECT_SOCKET,

		// Object dump / heap / profile commands
		protocol.OBJECT_DUMP_FILE_LIST,
		protocol.OBJECT_DUMP_FILE_DETAIL,
		protocol.OBJECT_CALL_HEAP_DUMP,
		protocol.OBJECT_LIST_HEAP_DUMP,
		protocol.OBJECT_DELETE_HEAP_DUMP,
		protocol.OBJECT_CALL_CPU_PROFILE,
		protocol.OBJECT_CALL_BLOCK_PROFILE,
		protocol.OBJECT_CALL_MUTEX_PROFILE,
		protocol.OBJECT_FILE_SOCKET,
		protocol.OBJECT_BATCH_ACTIVE_LIST,

		// Trigger commands
		protocol.TRIGGER_ACTIVE_SERVICE_LIST,
		protocol.TRIGGER_THREAD_DUMP,
		protocol.TRIGGER_THREAD_LIST,
		protocol.TRIGGER_HEAPHISTO,
		protocol.TRIGGER_BLOCK_PROFILE,
		protocol.TRIGGER_MUTEX_PROFILE,

		// Host commands
		protocol.HOST_TOP,
		protocol.HOST_PROCESS_DETAIL,
		protocol.HOST_DISK_USAGE,
		protocol.HOST_NET_STAT,
		protocol.HOST_WHO,
		protocol.HOST_MEMINFO,

		// Configuration commands
		protocol.GET_CONFIGURE_WAS,
		protocol.SET_CONFIGURE_WAS,
		protocol.LIST_CONFIGURE_WAS,
		protocol.REDEFINE_CLASSES,

		// Database commands
		protocol.ACTIVE_QUERY_LIST,
		protocol.DB_PROCESS_LIST,
		protocol.DB_PROCESS_DETAIL,
		protocol.DB_EXPLAIN_PLAN,
		protocol.DB_VARIABLES,
		protocol.DB_KILL_PROCESS,
		protocol.LOCK_LIST,
		protocol.GET_QUERY_INTERVAL,
		protocol.SET_QUERY_INTERVAL,
		protocol.SCHEMA_SIZE_STATUS,
		protocol.TABLE_SIZE_STATUS,
		protocol.INNODB_STATUS,
		protocol.SLAVE_STATUS,
		protocol.EXPLAIN_PLAN_FOR_THREAD,
		protocol.USE_DATABASE,

		// Misc agent commands
		protocol.REDIS_INFO,
		protocol.DUMP_APACHE_STATUS,
		protocol.DEBUG_AGENT,
		protocol.BATCH_ACTIVE_STACK,
	}

	for _, cmd := range simpleProxyCmds {
		registerSimpleProxy(r, caller, cmd)
	}

	// OBJECT_ACTIVE_SERVICE_LIST: NOT a simple proxy.
	// Java: ThreadList.scala agentActiveServiceList
	// When objHash==0, iterates over all live agents of objType.
	// Always adds objHash to agent response so client can identify the agent.
	r.Register(protocol.OBJECT_ACTIVE_SERVICE_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param, ok := pk.(*pack.MapPack)
		if !ok {
			return
		}

		objType := param.GetText("objType")
		objHash := param.GetInt("objHash")

		if objHash == 0 {
			if objType == "" {
				return
			}
			// Iterate over all live agents of this type
			liveAgents := objectCache.GetLive(deadTimeout)
			for _, info := range liveAgents {
				if info.Pack.ObjType != objType {
					continue
				}
				agentHash := info.Pack.ObjHash
				result := caller.AgentCallSingle(agentHash, protocol.OBJECT_ACTIVE_SERVICE_LIST, param)
				if result == nil {
					result = &pack.MapPack{}
				}
				result.Put("objHash", value.NewDecimalValue(int64(agentHash)))
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, result)
			}
		} else {
			result := caller.AgentCallSingle(objHash, protocol.OBJECT_ACTIVE_SERVICE_LIST, param)
			if result == nil {
				result = &pack.MapPack{}
			}
			result.Put("objHash", value.NewDecimalValue(int64(objHash)))
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

	// OBJECT_ACTIVE_SERVICE_LIST_GROUP: iterate over multiple agents.
	// Java: ThreadList.scala agentActiveServiceListGroup
	r.Register(protocol.OBJECT_ACTIVE_SERVICE_LIST_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param, ok := pk.(*pack.MapPack)
		if !ok {
			return
		}

		objHashVal := param.Get("objHash")
		if objHashVal == nil {
			return
		}
		lv, ok := objHashVal.(*value.ListValue)
		if !ok {
			return
		}

		for _, v := range lv.Value {
			dv, ok := v.(*value.DecimalValue)
			if !ok {
				continue
			}
			objHash := int32(dv.Value)
			result := caller.AgentCallSingle(objHash, protocol.OBJECT_ACTIVE_SERVICE_LIST, param)
			if result == nil {
				result = &pack.MapPack{}
			}
			result.Put("objHash", value.NewDecimalValue(int64(objHash)))
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, result)
		}
	})

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
