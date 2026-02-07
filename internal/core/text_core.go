package core

import (
	"log/slog"
	"net"
	"time"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/util"
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

func (tc *TextCore) run() {
	for tp := range tc.queue {
		slog.Debug("TextCore processing", "type", tp.XType, "hash", tp.Hash, "text", tp.Text)
		if tc.textWR != nil {
			date := util.FormatDate(time.Now().UnixMilli())
			tc.textWR.Add(date, tp.XType, tp.Hash, tp.Text)
		}
	}
}
