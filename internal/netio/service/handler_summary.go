package service

import (
	"github.com/zbum/scouter-server-go/internal/db/summary"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// Summary type constants (matching Java Scouter)
const (
	SummaryTypeApp          byte = 1
	SummaryTypeSQL          byte = 2
	SummaryTypeAPICall      byte = 3
	SummaryTypeIP           byte = 4
	SummaryTypeUA           byte = 5
	SummaryTypeServiceError byte = 6
	SummaryTypeAlert        byte = 7
)

// RegisterSummaryHandlers registers handlers for loading historical summaries.
func RegisterSummaryHandlers(r *Registry, summaryRD *summary.SummaryRD) {

	// LOAD_SERVICE_SUMMARY: load service (app) summary data
	r.Register(protocol.LOAD_SERVICE_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeApp)
	})

	// LOAD_SQL_SUMMARY: load SQL summary data
	r.Register(protocol.LOAD_SQL_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeSQL)
	})

	// LOAD_APICALL_SUMMARY: load API call summary data
	r.Register(protocol.LOAD_APICALL_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeAPICall)
	})

	// LOAD_IP_SUMMARY: load IP summary data
	r.Register(protocol.LOAD_IP_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeIP)
	})

	// LOAD_UA_SUMMARY: load User-Agent summary data
	r.Register(protocol.LOAD_UA_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeUA)
	})

	// LOAD_SERVICE_ERROR_SUMMARY: load service error summary data
	r.Register(protocol.LOAD_SERVICE_ERROR_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeServiceError)
	})

	// LOAD_ALERT_SUMMARY: load alert summary data
	r.Register(protocol.LOAD_ALERT_SUMMARY, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		loadSummaryByType(din, dout, summaryRD, SummaryTypeAlert)
	})
}

// loadSummaryByType is a helper function that loads summary data for a specific type.
func loadSummaryByType(din *protocol.DataInputX, dout *protocol.DataOutputX, summaryRD *summary.SummaryRD, stype byte) {
	pk, err := pack.ReadPack(din)
	if err != nil {
		return
	}
	param := pk.(*pack.MapPack)
	date := param.GetText("date")
	stime := param.GetLong("stime")
	etime := param.GetLong("etime")

	summaryRD.ReadRange(date, stype, stime, etime, func(data []byte) {
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		dout.Write(data)
	})
}
