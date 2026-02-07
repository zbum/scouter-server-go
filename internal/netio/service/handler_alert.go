package service

import (
	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// RegisterAlertHandlers registers handlers for loading historical alerts.
func RegisterAlertHandlers(r *Registry, alertRD *alert.AlertRD) {

	// ALERT_LOAD_TIME: load historical alerts by time range.
	r.Register(protocol.ALERT_LOAD_TIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")

		alertRD.ReadRange(date, stime, etime, func(data []byte) {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			dout.Write(data)
		})
	})
}
