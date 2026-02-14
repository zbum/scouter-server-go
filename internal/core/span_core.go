package core

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/step"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// SpanCore processes incoming SpanPack and SpanContainerPack data
// from Zipkin (via zipkin-scouter UDP storage), converts them to
// XLogPack entries and stores them in the xlog pipeline.
type SpanCore struct {
	xlogCache   *cache.XLogCache
	objectCache *cache.ObjectCache
	xlogWR      *xlog.XLogWR
	profileWR   *profile.ProfileWR
	textCache   *cache.TextCache
	queue       chan *pack.SpanPack
}

func NewSpanCore(xlogCache *cache.XLogCache, xlogWR *xlog.XLogWR, objectCache *cache.ObjectCache, profileWR *profile.ProfileWR, textCache *cache.TextCache) *SpanCore {
	sc := &SpanCore{
		xlogCache:   xlogCache,
		objectCache: objectCache,
		xlogWR:      xlogWR,
		profileWR:   profileWR,
		textCache:   textCache,
		queue:       make(chan *pack.SpanPack, 4096),
	}
	go sc.run()
	return sc
}

// Handler returns a PackHandler for PackTypeSpan.
func (sc *SpanCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		sp, ok := p.(*pack.SpanPack)
		if !ok {
			return
		}
		select {
		case sc.queue <- sp:
		default:
			slog.Warn("SpanCore queue overflow")
		}
	}
}

// ContainerHandler returns a PackHandler for PackTypeSpanContainer.
// It deserializes the container's Spans blob into individual SpanPacks.
//
// Note: Java's zipkin-scouter plugin uses SpanPack.toBytesList() which may
// chunk the serialized spans into multiple byte arrays when they exceed the
// UDP max size. Each chunk is sent as a separate SpanContainerPack, but
// SpanCount is set to the total number of spans across all chunks. Therefore
// we read until the blob is exhausted rather than relying on SpanCount.
func (sc *SpanCore) ContainerHandler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		cp, ok := p.(*pack.SpanContainerPack)
		if !ok {
			return
		}
		if len(cp.Spans) == 0 {
			return
		}

		d := protocol.NewDataInputX(cp.Spans)
		for d.Available() > 0 {
			// Java's SpanPack.toBytesList() uses writePack() which
			// prepends the pack type byte before each SpanPack.
			p, err := pack.ReadPack(d)
			if err != nil {
				slog.Warn("SpanCore: failed to read span from container",
					"error", err, "offset", d.Offset(), "blobLen", len(cp.Spans))
				break
			}
			sp, ok := p.(*pack.SpanPack)
			if !ok {
				slog.Warn("SpanCore: unexpected pack type in container",
					"type", p.PackType())
				continue
			}
			select {
			case sc.queue <- sp:
			default:
				slog.Warn("SpanCore queue overflow")
			}
		}
	}
}

func (sc *SpanCore) run() {
	for sp := range sc.queue {
		xp := spanToXLog(sp)

		// Serialize XLogPack for caching and storage
		o := protocol.NewDataOutputX()
		pack.WritePack(o, xp)
		b := o.ToByteArray()

		sc.xlogCache.Put(xp.ObjHash, xp.Elapsed, xp.Error != 0, b)

		// Keep the object alive in ObjectCache while spans are flowing.
		// The initial ObjectPack registration comes from the zipkin-scouter
		// plugin, but its heartbeat interval may exceed the dead timeout.
		if sc.objectCache != nil {
			sc.objectCache.Touch(xp.ObjHash)
		}

		slog.Debug("SpanCore processing",
			"txid", xp.Txid,
			"gxid", xp.Gxid,
			"service", xp.Service,
			"elapsed", xp.Elapsed)

		if sc.xlogWR != nil {
			sc.xlogWR.Add(&xlog.XLogEntry{
				Time:    xp.EndTime,
				Txid:    xp.Txid,
				Gxid:    xp.Gxid,
				Elapsed: xp.Elapsed,
				Data:    b,
			})
		}

		// Generate and store a basic profile only when the span has
		// meaningful detail (annotations or tags). Plain spans without
		// extra data would produce a near-empty profile, wasting disk space.
		if sc.profileWR != nil && spanHasDetail(sp) {
			profileData := sc.buildSpanProfile(sp)
			if len(profileData) > 0 {
				sc.profileWR.Add(&profile.ProfileEntry{
					TimeMs: xp.EndTime,
					Txid:   xp.Txid,
					Data:   profileData,
				})
				xp.ProfileCount = 1
			}
		}
	}
}

// minReasonableTimeMs is 2000-01-01 00:00:00 UTC in milliseconds.
// Timestamps before this are considered invalid.
var minReasonableTimeMs = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// spanToXLog converts a SpanPack to an XLogPack.
func spanToXLog(sp *pack.SpanPack) *pack.XLogPack {
	endTime := sp.Timestamp + int64(sp.Elapsed)
	if endTime < minReasonableTimeMs {
		endTime = time.Now().UnixMilli()
	}

	return &pack.XLogPack{
		EndTime: endTime,
		ObjHash: sp.ObjHash,
		Service: sp.Name,
		Txid:    sp.Txid,
		Caller:  sp.Caller,
		Gxid:    sp.Gxid,
		Elapsed: sp.Elapsed,
		Error:   sp.Error,
		IPAddr:  sp.LocalEndpointIp,
		B3Mode:  true,
	}
}

// spanHasDetail returns true if the span carries annotations or tags
// that are worth persisting as a profile. Spans with only basic fields
// (kind, endpoints) are not stored to avoid excessive disk usage.
func spanHasDetail(sp *pack.SpanPack) bool {
	if sp.Tags != nil && len(sp.Tags.Entries) > 0 {
		return true
	}
	if sp.AnnotationTimestamps != nil && len(sp.AnnotationTimestamps.Value) > 0 {
		return true
	}
	return false
}

// spanKindName returns the Zipkin span kind as a human-readable string.
func spanKindName(kind byte) string {
	switch kind {
	case 1:
		return "CLIENT"
	case 2:
		return "SERVER"
	case 3:
		return "PRODUCER"
	case 4:
		return "CONSUMER"
	default:
		return ""
	}
}

// buildSpanProfile generates a profile (serialized step bytes) from span data.
// This creates MessageStep entries for span kind, annotations, tags, and
// endpoint info so the Scouter Client can display span details when clicked.
func (sc *SpanCore) buildSpanProfile(sp *pack.SpanPack) []byte {
	out := protocol.NewDataOutputX()
	idx := int32(0)

	// Span kind
	if kind := spanKindName(sp.SpanType); kind != "" {
		step.WriteStep(out, &step.MessageStep{
			StepSingle: step.StepSingle{Index: idx},
			Message:    fmt.Sprintf("[%s] span", kind),
		})
		idx++
	}

	// Service name from text cache
	if sp.LocalEndpointServiceName != 0 && sc.textCache != nil {
		svcName, ok := sc.textCache.Get("object", sp.LocalEndpointServiceName)
		if ok && svcName != "" {
			step.WriteStep(out, &step.MessageStep{
				StepSingle: step.StepSingle{Index: idx},
				Message:    fmt.Sprintf("local.service = %s", svcName),
			})
			idx++
		}
	}

	// Local endpoint
	if len(sp.LocalEndpointIp) > 0 {
		ip := formatIP(sp.LocalEndpointIp)
		if sp.LocalEndpointPort != 0 {
			ip = fmt.Sprintf("%s:%d", ip, sp.LocalEndpointPort)
		}
		step.WriteStep(out, &step.MessageStep{
			StepSingle: step.StepSingle{Index: idx},
			Message:    fmt.Sprintf("local.endpoint = %s", ip),
		})
		idx++
	}

	// Remote endpoint
	if sp.RemoteEndpointServiceName != 0 || len(sp.RemoteEndpointIp) > 0 {
		var remoteSvc string
		if sp.RemoteEndpointServiceName != 0 && sc.textCache != nil {
			remoteSvc, _ = sc.textCache.Get("object", sp.RemoteEndpointServiceName)
		}
		if remoteSvc != "" {
			step.WriteStep(out, &step.MessageStep{
				StepSingle: step.StepSingle{Index: idx},
				Message:    fmt.Sprintf("remote.service = %s", remoteSvc),
			})
			idx++
		}
		if len(sp.RemoteEndpointIp) > 0 {
			ip := formatIP(sp.RemoteEndpointIp)
			if sp.RemoteEndpointPort != 0 {
				ip = fmt.Sprintf("%s:%d", ip, sp.RemoteEndpointPort)
			}
			step.WriteStep(out, &step.MessageStep{
				StepSingle: step.StepSingle{Index: idx},
				Message:    fmt.Sprintf("remote.endpoint = %s", ip),
			})
			idx++
		}
	}

	// Annotations
	if sp.AnnotationTimestamps != nil && sp.AnnotationValues != nil {
		for i := 0; i < len(sp.AnnotationTimestamps.Value) && i < len(sp.AnnotationValues.Value); i++ {
			var ts int64
			if dv, ok := sp.AnnotationTimestamps.Value[i].(*value.DecimalValue); ok {
				ts = dv.Value
			}
			var val string
			if tv, ok := sp.AnnotationValues.Value[i].(*value.TextValue); ok {
				val = tv.Value
			}
			startTime := int32(0)
			if ts > 0 && sp.Timestamp > 0 {
				startTime = int32(ts - sp.Timestamp)
			}
			step.WriteStep(out, &step.MessageStep{
				StepSingle: step.StepSingle{Index: idx, StartTime: startTime},
				Message:    fmt.Sprintf("[annotation] %s", val),
			})
			idx++
		}
	}

	// Tags
	if sp.Tags != nil {
		for _, entry := range sp.Tags.Entries {
			var val string
			switch tv := entry.Value.(type) {
			case *value.TextValue:
				val = tv.Value
			case *value.DecimalValue:
				val = strconv.FormatInt(tv.Value, 10)
			case *value.FloatValue:
				val = strconv.FormatFloat(float64(tv.Value), 'f', -1, 32)
			default:
				val = fmt.Sprintf("%v", tv)
			}
			step.WriteStep(out, &step.MessageStep{
				StepSingle: step.StepSingle{Index: idx},
				Message:    fmt.Sprintf("[tag] %s = %s", entry.Key, val),
			})
			idx++
		}
	}

	return out.ToByteArray()
}

// formatIP converts raw IP bytes to a readable string.
func formatIP(ip []byte) string {
	switch len(ip) {
	case 4:
		return fmt.Sprintf("%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3])
	case 16:
		return net.IP(ip).String()
	default:
		return fmt.Sprintf("%x", ip)
	}
}
