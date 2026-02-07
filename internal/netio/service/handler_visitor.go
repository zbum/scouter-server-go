package service

import (
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterVisitorHandlers registers visitor-related handlers.
// These are stubs that return 0/empty since VisitorDB is not yet implemented in Go.
func RegisterVisitorHandlers(r *Registry) {

	// VISITOR_REALTIME: real-time visitor count for a single object.
	r.Register(protocol.VISITOR_REALTIME, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(0))
	})

	// VISITOR_REALTIME_TOTAL: real-time visitor count for all objects of a type.
	r.Register(protocol.VISITOR_REALTIME_TOTAL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(0))
	})

	// VISITOR_REALTIME_GROUP: real-time visitor count for a group of objects.
	r.Register(protocol.VISITOR_REALTIME_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(0))
	})

	// VISITOR_LOADDATE: historical visitor count for an object on a date.
	r.Register(protocol.VISITOR_LOADDATE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(0))
	})

	// VISITOR_LOADDATE_TOTAL: historical visitor count for a type on a date.
	r.Register(protocol.VISITOR_LOADDATE_TOTAL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		value.WriteValue(dout, value.NewDecimalValue(0))
	})

	// VISITOR_LOADDATE_GROUP: historical visitor count per date for a group of objects.
	r.Register(protocol.VISITOR_LOADDATE_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		// Return empty - no data available without VisitorDB
	})

	// VISITOR_LOADHOUR_GROUP: historical visitor count per hour for a group of objects.
	r.Register(protocol.VISITOR_LOADHOUR_GROUP, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)
		// Return empty - no data available without VisitorDB
	})
}
