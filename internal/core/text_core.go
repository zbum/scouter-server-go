package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// TextCore processes incoming TextPack data, caching text hash→string mappings.
type TextCore struct {
	textCache *cache.TextCache
	textWR    *text.TextWR
	queue     chan *pack.TextPack
}

func NewTextCore(textCache *cache.TextCache, textWR *text.TextWR) *TextCore {
	tc := &TextCore{
		textCache: textCache,
		textWR:    textWR,
		queue:     make(chan *pack.TextPack, 2048),
	}
	go tc.run()
	return tc
}

func (tc *TextCore) Handler() PackHandler {
	return func(p pack.Pack, addr *net.UDPAddr) {
		tp, ok := p.(*pack.TextPack)
		if !ok {
			return
		}
		tc.textCache.Put(tp.XType, tp.Hash, tp.Text)
		select {
		case tc.queue <- tp:
		default:
			slog.Warn("TextCore queue overflow")
		}
	}
}

// AddText stores a text entry programmatically (not from a network pack).
// Used by AgentManager to store object names, etc.
func (tc *TextCore) AddText(xtype string, hash int32, text string) {
	tp := &pack.TextPack{XType: xtype, Hash: hash, Text: text}
	select {
	case tc.queue <- tp:
	default:
	}
}

func (tc *TextCore) run() {
	for tp := range tc.queue {
		slog.Debug("TextCore processing", "type", tp.XType, "hash", tp.Hash, "text", tp.Text)
		if tc.textWR != nil {
			// Route to daily text storage if configured for this type
			if shouldUseDailyText(tp.XType) {
				date := time.Now().Format("20060102")
				tc.textWR.AddDaily(date, tp.XType, tp.Hash, tp.Text)
			}
			// Always write to permanent storage as well
			tc.textWR.Add(tp.XType, tp.Hash, tp.Text)
		}
	}
}

// shouldUseDailyText returns true if the text type should also be stored in daily text directories.
func shouldUseDailyText(div string) bool {
	cfg := config.Get()
	if cfg == nil {
		return false
	}
	switch div {
	case "service":
		return cfg.MgrTextDbDailyServiceEnabled()
	case "apicall":
		return cfg.MgrTextDbDailyApiEnabled()
	case "ua":
		return cfg.MgrTextDbDailyUaEnabled()
	}
	return false
}
