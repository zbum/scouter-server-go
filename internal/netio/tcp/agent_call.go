package tcp

import (
	"log/slog"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// AgentCall provides RPC-style calls to agents via their pooled TCP connections.
type AgentCall struct {
	agentMgr *AgentManager
}

func NewAgentCall(agentMgr *AgentManager) *AgentCall {
	return &AgentCall{agentMgr: agentMgr}
}

// Call sends a command to the agent identified by objHash and returns the response MapPack.
// The connection is returned to the pool after use.
func (ac *AgentCall) Call(objHash int32, cmd string, param *pack.MapPack) *pack.MapPack {
	worker := ac.agentMgr.Get(objHash)
	if worker == nil {
		slog.Debug("AgentCall: no agent connection", "objHash", objHash)
		return nil
	}

	defer ac.agentMgr.Add(objHash, worker)

	if param == nil {
		param = &pack.MapPack{}
	}

	if err := worker.Write(cmd, param); err != nil {
		slog.Debug("AgentCall: write error", "objHash", objHash, "cmd", cmd, "error", err)
		return nil
	}

	var result pack.Pack
	for {
		flag, err := worker.ReadByte()
		if err != nil {
			slog.Debug("AgentCall: read flag error", "objHash", objHash, "error", err)
			return nil
		}
		if flag != protocol.FLAG_HAS_NEXT {
			break
		}
		p, err := worker.ReadPack()
		if err != nil {
			slog.Debug("AgentCall: read pack error", "objHash", objHash, "error", err)
			return nil
		}
		result = p
	}

	if result == nil {
		return nil
	}

	if mp, ok := result.(*pack.MapPack); ok {
		return mp
	}

	slog.Debug("AgentCall: unexpected response type", "objHash", objHash, "type", result.PackType())
	return nil
}

// CallStream sends a command and passes each response pack to the handler function.
// This is used for streaming responses (thread dump lines, etc.).
func (ac *AgentCall) CallStream(objHash int32, cmd string, param *pack.MapPack, handler func(pack.Pack)) {
	worker := ac.agentMgr.Get(objHash)
	if worker == nil {
		slog.Debug("AgentCall: no agent connection", "objHash", objHash)
		return
	}

	defer ac.agentMgr.Add(objHash, worker)

	if param == nil {
		param = &pack.MapPack{}
	}

	if err := worker.Write(cmd, param); err != nil {
		slog.Debug("AgentCall: write error", "objHash", objHash, "cmd", cmd, "error", err)
		return
	}

	for {
		flag, err := worker.ReadByte()
		if err != nil {
			return
		}
		if flag != protocol.FLAG_HAS_NEXT {
			break
		}
		p, err := worker.ReadPack()
		if err != nil {
			return
		}
		if p != nil {
			handler(p)
		}
	}
}
