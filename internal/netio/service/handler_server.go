package service

import (
	"time"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// RegisterServerHandlers registers SERVER_VERSION and SERVER_TIME handlers.
func RegisterServerHandlers(r *Registry, version string) {
	r.Register(protocol.SERVER_VERSION, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read the param pack (client sends it even though it's not needed)
		pack.ReadPack(din)

		resp := &pack.MapPack{}
		resp.PutStr("version", version)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	r.Register(protocol.SERVER_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		resp := &pack.MapPack{}
		resp.PutLong("time", time.Now().UnixMilli())
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

}
