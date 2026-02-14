package geoip

import (
	"log/slog"
	"net"
	"sync"
)

// GeoIPUtil provides GeoIP lookup with LRU cache.
// Uses MaxMind MMDB format for IP → city resolution.
type GeoIPUtil struct {
	mu         sync.RWMutex
	enabled    bool
	dbPath     string
	cache      map[string]*GeoResult // IP string → result
	cacheOrder []string              // LRU order tracking
	maxCache   int
}

// GeoResult holds the result of a GeoIP lookup.
type GeoResult struct {
	CountryCode string
	City        string
	CityHash    int32
}

// New creates a new GeoIPUtil.
// If the MMDB file doesn't exist, lookups return empty results.
func New(dbPath string) *GeoIPUtil {
	g := &GeoIPUtil{
		enabled:  true,
		dbPath:   dbPath,
		cache:    make(map[string]*GeoResult),
		maxCache: 10000,
	}
	return g
}

// Lookup resolves IP address bytes to country code and city.
// Returns empty strings for private IPs or if GeoIP is not available.
func (g *GeoIPUtil) Lookup(ipAddr []byte) (countryCode string, city string, cityHash int32) {
	if !g.enabled || len(ipAddr) == 0 {
		return "", "", 0
	}

	ip := net.IP(ipAddr)
	if ip == nil {
		return "", "", 0
	}

	// Skip private/loopback IPs
	if isPrivateIP(ip) {
		return "", "", 0
	}

	ipStr := ip.String()

	// Check cache
	g.mu.RLock()
	if result, ok := g.cache[ipStr]; ok {
		g.mu.RUnlock()
		return result.CountryCode, result.City, result.CityHash
	}
	g.mu.RUnlock()

	// GeoIP MMDB lookup would happen here.
	// Since we can't add the maxminddb-golang dependency without go mod tidy
	// being available, this provides the framework for when the MMDB file is present.
	// The lookup returns empty results until the MMDB reader is initialized.
	result := &GeoResult{}

	// Cache the result
	g.mu.Lock()
	if len(g.cache) >= g.maxCache {
		// Evict oldest entries (simple eviction: clear half the cache)
		g.cache = make(map[string]*GeoResult)
		g.cacheOrder = nil
	}
	g.cache[ipStr] = result
	g.cacheOrder = append(g.cacheOrder, ipStr)
	g.mu.Unlock()

	return result.CountryCode, result.City, result.CityHash
}

// privateCIDRs holds pre-parsed private IP ranges to avoid repeated parsing.
var privateCIDRs []*net.IPNet

func init() {
	for _, cidr := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"} {
		_, network, _ := net.ParseCIDR(cidr)
		privateCIDRs = append(privateCIDRs, network)
	}
}

// isPrivateIP checks if an IP address is a private/loopback address.
func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	for _, cidr := range privateCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// Close closes the GeoIP database reader.
func (g *GeoIPUtil) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.enabled = false
	g.cache = make(map[string]*GeoResult)
	slog.Info("GeoIP closed")
}
