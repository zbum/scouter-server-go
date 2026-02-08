package core

import (
	"strings"
	"sync"

	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/util"
)

// XLogGroupUtil derives group hashes from service URLs when the agent
// hasn't explicitly set a group. This matches Java's XLogGroupUtil.
type XLogGroupUtil struct {
	mu       sync.Mutex
	groupMap map[int32]int32 // service hash → group hash (max 50000)

	textCache *cache.TextCache

	// Well-known group hashes
	hJsp     int32
	hImages  int32
	hStatics int32
	hRoot    int32
}

var (
	imageExts = map[string]bool{
		"gif": true, "jpg": true, "png": true, "bmp": true, "ico": true,
	}
	staticExts = map[string]bool{
		"html": true, "htm": true, "css": true, "xml": true, "js": true,
	}
)

const groupMapMax = 50000

func NewXLogGroupUtil(textCache *cache.TextCache) *XLogGroupUtil {
	g := &XLogGroupUtil{
		groupMap:  make(map[int32]int32),
		textCache: textCache,
		hJsp:      util.HashString("*.jsp"),
		hImages:   util.HashString("images"),
		hStatics:  util.HashString("statics"),
		hRoot:     util.HashString("/**"),
	}

	// Register well-known group names
	textCache.Put("group", g.hJsp, "*.jsp")
	textCache.Put("group", util.HashString("**"), "**")
	textCache.Put("group", g.hImages, "images")
	textCache.Put("group", g.hStatics, "statics")
	textCache.Put("group", g.hRoot, "/**")

	return g
}

// Process sets the group hash on an XLogPack if not already set.
func (g *XLogGroupUtil) Process(xp *pack.XLogPack) {
	if xp.Group != 0 {
		return
	}
	xp.Group = g.makeGroupHash(xp.Service)
}

func (g *XLogGroupUtil) makeGroupHash(service int32) int32 {
	g.mu.Lock()
	groupHash, ok := g.groupMap[service]
	g.mu.Unlock()

	if ok && groupHash != 0 {
		return groupHash
	}

	url, found := g.textCache.Get("service", service)
	if !found || url == "" {
		return 0
	}

	groupHash = g.getGroupHash(url)
	if groupHash != 0 {
		g.mu.Lock()
		if len(g.groupMap) >= groupMapMax {
			// Evict a random entry
			for k := range g.groupMap {
				delete(g.groupMap, k)
				break
			}
		}
		g.groupMap[service] = groupHash
		g.mu.Unlock()
	}
	return groupHash
}

func (g *XLogGroupUtil) getGroupHash(url string) int32 {
	if url == "" {
		return 0
	}

	// Check file extension
	x := strings.LastIndex(url, ".")
	if x > 0 {
		postfix := strings.ToLower(url[x+1:])
		if postfix == "jsp" {
			return g.hJsp
		}
		if imageExts[postfix] {
			return g.hImages
		}
		if staticExts[postfix] {
			return g.hStatics
		}
	}

	if url == "/" || url == "" {
		return g.hRoot
	}

	// Extract first path segment: /admin/list → /admin
	x1 := strings.Index(url[1:], "/")
	if x1 < 0 {
		return g.hRoot
	}
	x1++ // adjust for the offset of 1

	groupName := url[:x1]
	grpHash := util.HashString(groupName)

	// Store group name in text cache
	g.textCache.Put("group", grpHash, groupName)

	return grpHash
}
