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

// ObjectDeadTimeMs returns object_deadtime_ms (default 8000).
func (c *Config) ObjectDeadTimeMs() int {
	return c.GetInt("object_deadtime_ms", 8000)
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

// ---------------------------------------------------------------------------
// Network – listen addresses
// ---------------------------------------------------------------------------

// NetUDPListenIP returns net_udp_listen_ip (default "0.0.0.0").
func (c *Config) NetUDPListenIP() string {
	return c.GetString("net_udp_listen_ip", "0.0.0.0")
}

// NetTCPListenIP returns net_tcp_listen_ip (default "0.0.0.0").
func (c *Config) NetTCPListenIP() string {
	return c.GetString("net_tcp_listen_ip", "0.0.0.0")
}

// ---------------------------------------------------------------------------
// Network – TCP agent
// ---------------------------------------------------------------------------

// NetTcpAgentSoTimeoutMs returns net_tcp_agent_so_timeout_ms (default 60000).
func (c *Config) NetTcpAgentSoTimeoutMs() int {
	return c.GetInt("net_tcp_agent_so_timeout_ms", 60000)
}

// NetTcpAgentKeepaliveIntervalMs returns net_tcp_agent_keepalive_interval_ms (default 5000).
func (c *Config) NetTcpAgentKeepaliveIntervalMs() int {
	return c.GetInt("net_tcp_agent_keepalive_interval_ms", 5000)
}

// NetTcpGetAgentConnectionWaitMs returns net_tcp_get_agent_connection_wait_ms (default 1000).
func (c *Config) NetTcpGetAgentConnectionWaitMs() int {
	return c.GetInt("net_tcp_get_agent_connection_wait_ms", 1000)
}

// NetTcpServicePoolSize returns net_tcp_service_pool_size (default 100).
func (c *Config) NetTcpServicePoolSize() int {
	return c.GetInt("net_tcp_service_pool_size", 100)
}

// ---------------------------------------------------------------------------
// Network – UDP buffer
// ---------------------------------------------------------------------------

// NetUDPPacketBufferSize returns net_udp_packet_buffer_size (default 65535).
func (c *Config) NetUDPPacketBufferSize() int {
	return c.GetInt("net_udp_packet_buffer_size", 65535)
}

// NetUDPSoRcvbufSize returns net_udp_so_rcvbuf_size (default 4MB).
func (c *Config) NetUDPSoRcvbufSize() int {
	return c.GetInt("net_udp_so_rcvbuf_size", 4*1024*1024)
}

// ---------------------------------------------------------------------------
// Network – HTTP API
// ---------------------------------------------------------------------------

// NetHTTPApiEnabled returns net_http_api_enabled (default false).
func (c *Config) NetHTTPApiEnabled() bool {
	return c.GetBool("net_http_api_enabled", false)
}

// NetHTTPApiCorsAllowOrigin returns net_http_api_cors_allow_origin (default "*").
func (c *Config) NetHTTPApiCorsAllowOrigin() string {
	return c.GetString("net_http_api_cors_allow_origin", "*")
}

// NetHTTPApiCorsAllowCredentials returns net_http_api_cors_allow_credentials (default "true").
func (c *Config) NetHTTPApiCorsAllowCredentials() string {
	return c.GetString("net_http_api_cors_allow_credentials", "true")
}

// NetHTTPApiAuthIpEnabled returns net_http_api_auth_ip_enabled (default false).
func (c *Config) NetHTTPApiAuthIpEnabled() bool {
	return c.GetBool("net_http_api_auth_ip_enabled", false)
}

// NetHTTPApiAuthSessionEnabled returns net_http_api_auth_session_enabled (default false).
func (c *Config) NetHTTPApiAuthSessionEnabled() bool {
	return c.GetBool("net_http_api_auth_session_enabled", false)
}

// NetHTTPApiSessionTimeout returns net_http_api_session_timeout in seconds (default 86400).
func (c *Config) NetHTTPApiSessionTimeout() int {
	return c.GetInt("net_http_api_session_timeout", 86400)
}

// NetHTTPApiAuthBearerTokenEnabled returns net_http_api_auth_bearer_token_enabled (default false).
func (c *Config) NetHTTPApiAuthBearerTokenEnabled() bool {
	return c.GetBool("net_http_api_auth_bearer_token_enabled", false)
}

// NetHTTPApiGzipEnabled returns net_http_api_gzip_enabled (default true).
func (c *Config) NetHTTPApiGzipEnabled() bool {
	return c.GetBool("net_http_api_gzip_enabled", true)
}

// NetHTTPApiAllowIps returns net_http_api_allow_ips (default "localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1").
func (c *Config) NetHTTPApiAllowIps() string {
	return c.GetString("net_http_api_allow_ips", "localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1")
}

// ---------------------------------------------------------------------------
// Network – webapp TCP client pool
// ---------------------------------------------------------------------------

// NetWebappTcpClientPoolSize returns net_webapp_tcp_client_pool_size (default 30).
func (c *Config) NetWebappTcpClientPoolSize() int {
	return c.GetInt("net_webapp_tcp_client_pool_size", 30)
}

// NetWebappTcpClientPoolTimeout returns net_webapp_tcp_client_pool_timeout in ms (default 60000).
func (c *Config) NetWebappTcpClientPoolTimeout() int {
	return c.GetInt("net_webapp_tcp_client_pool_timeout", 60000)
}

// NetWebappTcpClientSoTimeout returns net_webapp_tcp_client_so_timeout in ms (default 30000).
func (c *Config) NetWebappTcpClientSoTimeout() int {
	return c.GetInt("net_webapp_tcp_client_so_timeout", 30000)
}

// ---------------------------------------------------------------------------
// Logging – debug flags
// ---------------------------------------------------------------------------

// LogTcpActionEnabled returns log_tcp_action_enabled (default false).
func (c *Config) LogTcpActionEnabled() bool {
	return c.GetBool("log_tcp_action_enabled", false)
}

// LogUDPMultipacket returns log_udp_multipacket (default false).
func (c *Config) LogUDPMultipacket() bool {
	return c.GetBool("log_udp_multipacket", false)
}

// LogExpiredMultipacket returns log_expired_multipacket (default true).
func (c *Config) LogExpiredMultipacket() bool {
	return c.GetBool("log_expired_multipacket", true)
}

// LogUDPPacket returns log_udp_packet (default false).
func (c *Config) LogUDPPacket() bool {
	return c.GetBool("log_udp_packet", false)
}

// LogUDPCounter returns log_udp_counter (default false).
func (c *Config) LogUDPCounter() bool {
	return c.GetBool("log_udp_counter", false)
}

// LogUDPInteractionCounter returns log_udp_interaction_counter (default false).
func (c *Config) LogUDPInteractionCounter() bool {
	return c.GetBool("log_udp_interaction_counter", false)
}

// LogUDPXLog returns log_udp_xlog (default false).
func (c *Config) LogUDPXLog() bool {
	return c.GetBool("log_udp_xlog", false)
}

// LogUDPProfile returns log_udp_profile (default false).
func (c *Config) LogUDPProfile() bool {
	return c.GetBool("log_udp_profile", false)
}

// LogUDPText returns log_udp_text (default false).
func (c *Config) LogUDPText() bool {
	return c.GetBool("log_udp_text", false)
}

// LogUDPAlert returns log_udp_alert (default false).
func (c *Config) LogUDPAlert() bool {
	return c.GetBool("log_udp_alert", false)
}

// LogUDPObject returns log_udp_object (default false).
func (c *Config) LogUDPObject() bool {
	return c.GetBool("log_udp_object", false)
}

// LogUDPStatus returns log_udp_status (default false).
func (c *Config) LogUDPStatus() bool {
	return c.GetBool("log_udp_status", false)
}

// LogUDPStack returns log_udp_stack (default false).
func (c *Config) LogUDPStack() bool {
	return c.GetBool("log_udp_stack", false)
}

// LogUDPSummary returns log_udp_summary (default false).
func (c *Config) LogUDPSummary() bool {
	return c.GetBool("log_udp_summary", false)
}

// LogUDPBatch returns log_udp_batch (default false).
func (c *Config) LogUDPBatch() bool {
	return c.GetBool("log_udp_batch", false)
}

// LogUDPSpan returns log_udp_span (default false).
func (c *Config) LogUDPSpan() bool {
	return c.GetBool("log_udp_span", false)
}

// LogIndexTraversalWarningCount returns log_index_traversal_warning_count (default 100).
func (c *Config) LogIndexTraversalWarningCount() int {
	return c.GetInt("log_index_traversal_warning_count", 100)
}

// LogSqlParsingFailEnabled returns log_sql_parsing_fail_enabled (default false).
func (c *Config) LogSqlParsingFailEnabled() bool {
	return c.GetBool("log_sql_parsing_fail_enabled", false)
}

// ---------------------------------------------------------------------------
// Directories
// ---------------------------------------------------------------------------

// PluginDir returns plugin_dir (default "./plugin").
func (c *Config) PluginDir() string {
	return c.GetString("plugin_dir", "./plugin")
}

// PluginEnabled returns plugin_enabled (default true).
func (c *Config) PluginEnabled() bool {
	return c.GetBool("plugin_enabled", true)
}

// ClientDir returns client_dir (default "./client").
func (c *Config) ClientDir() string {
	return c.GetString("client_dir", "./client")
}

// TempDir returns temp_dir (default "./tempdata").
func (c *Config) TempDir() string {
	return c.GetString("temp_dir", "./tempdata")
}

// ---------------------------------------------------------------------------
// Object management
// ---------------------------------------------------------------------------

// ObjectInactiveAlertLevel returns object_inactive_alert_level (default 0).
func (c *Config) ObjectInactiveAlertLevel() int {
	return c.GetInt("object_inactive_alert_level", 0)
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

// CompressXLogEnabled returns compress_xlog_enabled (default false).
func (c *Config) CompressXLogEnabled() bool {
	return c.GetBool("compress_xlog_enabled", false)
}

// CompressProfileEnabled returns compress_profile_enabled (default true).
func (c *Config) CompressProfileEnabled() bool {
	return c.GetBool("compress_profile_enabled", true)
}

// ---------------------------------------------------------------------------
// Purge / Retention manager
// ---------------------------------------------------------------------------

// MgrPurgeEnabled returns mgr_purge_enabled (default true).
func (c *Config) MgrPurgeEnabled() bool {
	return c.GetBool("mgr_purge_enabled", true)
}

// MgrPurgeDiskUsagePct returns mgr_purge_disk_usage_pct (default 80).
func (c *Config) MgrPurgeDiskUsagePct() int {
	return c.GetInt("mgr_purge_disk_usage_pct", 80)
}

// MgrPurgeProfileKeepDays returns mgr_purge_profile_keep_days (default 10).
func (c *Config) MgrPurgeProfileKeepDays() int {
	return c.GetInt("mgr_purge_profile_keep_days", 10)
}

// MgrPurgeXLogKeepDays returns mgr_purge_xlog_keep_days (default 30).
func (c *Config) MgrPurgeXLogKeepDays() int {
	return c.GetInt("mgr_purge_xlog_keep_days", 30)
}

// MgrPurgeCounterKeepDays returns mgr_purge_counter_keep_days (default 70).
func (c *Config) MgrPurgeCounterKeepDays() int {
	return c.GetInt("mgr_purge_counter_keep_days", 70)
}

// MgrPurgeRealtimeCounterKeepDays returns mgr_purge_realtime_counter_keep_days (default 70).
func (c *Config) MgrPurgeRealtimeCounterKeepDays() int {
	return c.GetInt("mgr_purge_realtime_counter_keep_days", 70)
}

// MgrPurgeDailyTextDays returns mgr_purge_daily_text_days (default 140).
func (c *Config) MgrPurgeDailyTextDays() int {
	return c.GetInt("mgr_purge_daily_text_days", 140)
}

// MgrPurgeSumDataDays returns mgr_purge_sum_data_days (default 60).
func (c *Config) MgrPurgeSumDataDays() int {
	return c.GetInt("mgr_purge_sum_data_days", 60)
}

// ---------------------------------------------------------------------------
// Text DB
// ---------------------------------------------------------------------------

// MgrTextDbDailyServiceEnabled returns mgr_text_db_daily_service_enabled (default false).
func (c *Config) MgrTextDbDailyServiceEnabled() bool {
	return c.GetBool("mgr_text_db_daily_service_enabled", false)
}

// MgrTextDbDailyApiEnabled returns mgr_text_db_daily_api_enabled (default false).
func (c *Config) MgrTextDbDailyApiEnabled() bool {
	return c.GetBool("mgr_text_db_daily_api_enabled", false)
}

// MgrTextDbDailyUaEnabled returns mgr_text_db_daily_ua_enabled (default false).
func (c *Config) MgrTextDbDailyUaEnabled() bool {
	return c.GetBool("mgr_text_db_daily_ua_enabled", false)
}

// MgrTextDbIndexMB returns the hash index size in MB for a given text div.
// Matches Java's per-type configuration: _mgr_text_db_index_{type}_mb.
func (c *Config) MgrTextDbIndexMB(div string) int {
	switch div {
	case "service":
		return c.GetInt("_mgr_text_db_index_service_mb", 1)
	case "apicall":
		return c.GetInt("_mgr_text_db_index_api_mb", 1)
	case "ua":
		return c.GetInt("_mgr_text_db_index_ua_mb", 1)
	case "login":
		return c.GetInt("_mgr_text_db_index_login_mb", 1)
	case "desc":
		return c.GetInt("_mgr_text_db_index_desc_mb", 1)
	case "hmsg":
		return c.GetInt("_mgr_text_db_index_hmsg_mb", 1)
	default:
		return c.GetInt("_mgr_text_db_index_default_mb", 1)
	}
}

// MgrTextDbDailyIndexMB returns _mgr_text_db_daily_index_mb (default 1).
func (c *Config) MgrTextDbDailyIndexMB() int {
	return c.GetInt("_mgr_text_db_daily_index_mb", 1)
}


// ---------------------------------------------------------------------------
// XLog / Profile queue
// ---------------------------------------------------------------------------

// XLogRealtimeLowerBoundMs returns xlog_realtime_lower_bound_ms (default 0).
func (c *Config) XLogRealtimeLowerBoundMs() int {
	return c.GetInt("xlog_realtime_lower_bound_ms", 0)
}

// XLogPasttimeLowerBoundMs returns xlog_pasttime_lower_bound_ms (default 0).
func (c *Config) XLogPasttimeLowerBoundMs() int {
	return c.GetInt("xlog_pasttime_lower_bound_ms", 0)
}

// ProfileQueueSize returns profile_queue_size (default 1000).
func (c *Config) ProfileQueueSize() int {
	return c.GetInt("profile_queue_size", 1000)
}

// ---------------------------------------------------------------------------
// GeoIP
// ---------------------------------------------------------------------------

// GeoIPEnabled returns geoip_enabled (default true).
func (c *Config) GeoIPEnabled() bool {
	return c.GetBool("geoip_enabled", true)
}

// GeoIPDataCityFile returns geoip_data_city_file (default "./conf/GeoLiteCity.dat").
func (c *Config) GeoIPDataCityFile() string {
	return c.GetString("geoip_data_city_file", "./conf/GeoLiteCity.dat")
}

// ---------------------------------------------------------------------------
// SQL & features
// ---------------------------------------------------------------------------

// SqlTableParsingEnabled returns sql_table_parsing_enabled (default true).
func (c *Config) SqlTableParsingEnabled() bool {
	return c.GetBool("sql_table_parsing_enabled", true)
}

// TagcntEnabled returns tagcnt_enabled (default true).
func (c *Config) TagcntEnabled() bool {
	return c.GetBool("tagcnt_enabled", true)
}

// ReqSearchXLogMaxCount returns req_search_xlog_max_count (default 500).
func (c *Config) ReqSearchXLogMaxCount() int {
	return c.GetInt("req_search_xlog_max_count", 500)
}

// VisitorHourlyCountEnabled returns visitor_hourly_count_enabled (default true).
func (c *Config) VisitorHourlyCountEnabled() bool {
	return c.GetBool("visitor_hourly_count_enabled", true)
}

// ---------------------------------------------------------------------------
// External link
// ---------------------------------------------------------------------------

// ExtLinkName returns ext_link_name (default "scouter-paper").
func (c *Config) ExtLinkName() string {
	return c.GetString("ext_link_name", "scouter-paper")
}

// ExtLinkUrlPattern returns ext_link_url_pattern (default "").
func (c *Config) ExtLinkUrlPattern() string {
	return c.GetString("ext_link_url_pattern", "")
}

// ---------------------------------------------------------------------------
// Zipkin span ingestion
// ---------------------------------------------------------------------------

// ZipkinEnabled returns zipkin_enabled (default false).
// When enabled, the server accepts Zipkin span data (via zipkin-scouter UDP storage)
// and converts them to XLog entries for display in the Scouter client.
func (c *Config) ZipkinEnabled() bool {
	return c.GetBool("zipkin_enabled", false)
}
