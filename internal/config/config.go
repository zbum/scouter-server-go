package config

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds all server configuration values.
type Config struct {
	mu       sync.RWMutex
	props    map[string]string
	filePath string
	modTime  time.Time
}

var globalConfig atomic.Pointer[Config]

// Get returns the global config instance.
func Get() *Config {
	return globalConfig.Load()
}

// Load reads a scouter.conf file and returns a new Config.
// If the file does not exist, a Config with empty props (defaults) is returned
// without an error, so the server can start without a config file.
func Load(filePath string) (*Config, error) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		absPath = filePath
	}

	cfg := &Config{
		props:    make(map[string]string),
		filePath: absPath,
	}

	info, err := os.Stat(absPath)
	if err != nil {
		// File does not exist -- return default config, no error.
		globalConfig.Store(cfg)
		return cfg, nil
	}
	cfg.modTime = info.ModTime()

	f, err := os.Open(absPath)
	if err != nil {
		slog.Warn("config file open failed, using defaults", "path", absPath, "error", err)
		globalConfig.Store(cfg)
		return cfg, nil
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		if key != "" {
			cfg.props[key] = val
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	globalConfig.Store(cfg)
	slog.Info("config loaded", "path", absPath, "properties", len(cfg.props))
	return cfg, nil
}

// ---------------------------------------------------------------------------
// Generic typed getters
// ---------------------------------------------------------------------------

// GetString returns a config value, or the default if not set.
func (c *Config) GetString(key, defaultVal string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.props[key]; ok {
		return v
	}
	return defaultVal
}

// GetInt returns an integer config value.
func (c *Config) GetInt(key string, defaultVal int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.props[key]; ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

// GetInt64 returns an int64 config value.
func (c *Config) GetInt64(key string, defaultVal int64) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.props[key]; ok {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

// GetBool returns a boolean config value.
// Truthy values: "true", "1", "yes", "on" (case-insensitive).
func (c *Config) GetBool(key string, defaultVal bool) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.props[key]; ok {
		switch strings.ToLower(v) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return defaultVal
}

// ---------------------------------------------------------------------------
// Convenience accessors for well-known configuration keys
// ---------------------------------------------------------------------------

// ServerID returns the server_id (default "0").
func (c *Config) ServerID() string {
	return c.GetString("server_id", "0")
}

// UDPPort returns net_udp_listen_port (default 6100).
func (c *Config) UDPPort() int {
	return c.GetInt("net_udp_listen_port", 6100)
}

// TCPPort returns net_tcp_listen_port (default 6100).
func (c *Config) TCPPort() int {
	return c.GetInt("net_tcp_listen_port", 6100)
}

// HTTPPort returns net_http_port (default 6180).
func (c *Config) HTTPPort() int {
	return c.GetInt("net_http_port", 6180)
}

// HTTPEnabled returns net_http_enabled (default false).
func (c *Config) HTTPEnabled() bool {
	return c.GetBool("net_http_enabled", false)
}

// DBDir returns db_dir (default "./database").
func (c *Config) DBDir() string {
	return c.GetString("db_dir", "./database")
}

// LogDir returns log_dir (default "./logs").
func (c *Config) LogDir() string {
	return c.GetString("log_dir", "./logs")
}

// LogRotationEnabled returns log_rotation_enabled (default true).
func (c *Config) LogRotationEnabled() bool {
	return c.GetBool("log_rotation_enabled", true)
}

// LogKeepDays returns log_keep_days (default 30).
func (c *Config) LogKeepDays() int {
	return c.GetInt("log_keep_days", 30)
}

// DBKeepDays returns db_keep_days (default 30).
func (c *Config) DBKeepDays() int {
	return c.GetInt("db_keep_days", 30)
}

// DBMaxDiskUsagePct returns db_max_disk_usage_pct (default 80).
func (c *Config) DBMaxDiskUsagePct() int {
	return c.GetInt("db_max_disk_usage_pct", 80)
}

// ObjectDeadTimeMs returns object_deadtime_ms (default 30000).
func (c *Config) ObjectDeadTimeMs() int {
	return c.GetInt("object_deadtime_ms", 30000)
}

// XLogQueueSize returns xlog_queue_size (default 10000).
func (c *Config) XLogQueueSize() int {
	return c.GetInt("xlog_queue_size", 10000)
}

// TextCacheMaxSize returns text_cache_max_size (default 100000).
func (c *Config) TextCacheMaxSize() int {
	return c.GetInt("text_cache_max_size", 100000)
}

// DayContainerKeepHours returns day_container_keep_hours (default 48).
// Containers older than this are automatically closed to free memory and file handles.
func (c *Config) DayContainerKeepHours() int {
	return c.GetInt("day_container_keep_hours", 48)
}

// IsDebug returns debug (default false).
func (c *Config) IsDebug() bool {
	return c.GetBool("debug", false)
}

// FilePath returns the absolute path to the config file.
func (c *Config) FilePath() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.filePath
}

// ConfDir returns the directory containing the config file.
func (c *Config) ConfDir() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.filePath == "" {
		return ""
	}
	return filepath.Dir(c.filePath)
}

// NetTcpClientSoTimeoutMs returns net_tcp_client_so_timeout_ms (default 8000).
func (c *Config) NetTcpClientSoTimeoutMs() int {
	return c.GetInt("net_tcp_client_so_timeout_ms", 8000)
}
