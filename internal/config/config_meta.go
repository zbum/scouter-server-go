package config

// ValueType constants matching Java's scouter.lang.conf.ValueType enum.
const (
	ValueTypeString = 1 // Plain string
	ValueTypeNum    = 2 // Integer/Long
	ValueTypeBool   = 3 // Boolean
)

// ConfigMeta holds description and value type for a config key.
type ConfigMeta struct {
	Desc      string
	ValueType int
}

// ConfigMetaMap returns metadata for all known server config keys.
func ConfigMetaMap() map[string]ConfigMeta {
	return map[string]ConfigMeta{
		// Server identity
		"server_id": {"Server ID", ValueTypeString},

		// Network – UDP
		"net_udp_listen_ip":         {"UDP listen IP address", ValueTypeString},
		"net_udp_listen_port":       {"UDP listen port for agent data", ValueTypeNum},
		"net_udp_packet_buffer_size": {"UDP packet buffer size in bytes", ValueTypeNum},
		"net_udp_so_rcvbuf_size":    {"UDP socket receive buffer size in bytes", ValueTypeNum},

		// Network – TCP
		"net_tcp_listen_ip":                      {"TCP listen IP address", ValueTypeString},
		"net_tcp_listen_port":                    {"TCP listen port for client connections", ValueTypeNum},
		"net_tcp_client_so_timeout_ms":           {"TCP client socket timeout in ms", ValueTypeNum},
		"net_tcp_agent_so_timeout_ms":            {"TCP agent socket timeout in ms", ValueTypeNum},
		"net_tcp_agent_keepalive_interval_ms":    {"TCP agent keepalive interval in ms", ValueTypeNum},
		"net_tcp_get_agent_connection_wait_ms":   {"Wait time for agent connection in ms", ValueTypeNum},
		"net_tcp_service_pool_size":              {"TCP service thread pool size", ValueTypeNum},

		// Network – HTTP API
		"net_http_port":                           {"HTTP API port", ValueTypeNum},
		"net_http_enabled":                        {"Enable HTTP API server", ValueTypeBool},
		"net_http_api_enabled":                    {"Enable HTTP API", ValueTypeBool},
		"net_http_api_cors_allow_origin":          {"CORS allow origin header", ValueTypeString},
		"net_http_api_cors_allow_credentials":     {"CORS allow credentials header", ValueTypeString},
		"net_http_api_auth_ip_enabled":            {"Enable HTTP API IP-based auth", ValueTypeBool},
		"net_http_api_auth_session_enabled":       {"Enable HTTP API session auth", ValueTypeBool},
		"net_http_api_session_timeout":            {"HTTP API session timeout in seconds", ValueTypeNum},
		"net_http_api_auth_bearer_token_enabled":  {"Enable HTTP API bearer token auth", ValueTypeBool},
		"net_http_api_gzip_enabled":               {"Enable HTTP API gzip compression", ValueTypeBool},
		"net_http_api_allow_ips":                  {"Allowed IPs for HTTP API access", ValueTypeString},

		// Network – webapp TCP pool
		"net_webapp_tcp_client_pool_size":    {"Webapp TCP client pool size", ValueTypeNum},
		"net_webapp_tcp_client_pool_timeout": {"Webapp TCP client pool timeout in ms", ValueTypeNum},
		"net_webapp_tcp_client_so_timeout":   {"Webapp TCP client socket timeout in ms", ValueTypeNum},

		// Database
		"db_dir":               {"Database directory path", ValueTypeString},
		"db_keep_days":         {"Number of days to keep database files", ValueTypeNum},
		"db_max_disk_usage_pct": {"Maximum disk usage percentage for database", ValueTypeNum},

		// Logging
		"debug":                  {"Enable debug logging", ValueTypeBool},
		"log_dir":                {"Log directory path", ValueTypeString},
		"log_rotation_enabled":   {"Enable log file rotation", ValueTypeBool},
		"log_keep_days":          {"Number of days to keep log files", ValueTypeNum},
		"log_tcp_action_enabled": {"Log TCP actions for debugging", ValueTypeBool},

		// Logging – UDP debug
		"log_udp_multipacket":       {"Log UDP multipacket debug info", ValueTypeBool},
		"log_expired_multipacket":   {"Log expired multipacket warnings", ValueTypeBool},
		"log_udp_packet":            {"Log UDP packet debug info", ValueTypeBool},
		"log_udp_counter":           {"Log UDP counter data", ValueTypeBool},
		"log_udp_interaction_counter": {"Log UDP interaction counter data", ValueTypeBool},
		"log_udp_xlog":              {"Log UDP XLog data", ValueTypeBool},
		"log_udp_profile":           {"Log UDP profile data", ValueTypeBool},
		"log_udp_text":              {"Log UDP text data", ValueTypeBool},
		"log_udp_alert":             {"Log UDP alert data", ValueTypeBool},
		"log_udp_object":            {"Log UDP object data", ValueTypeBool},
		"log_udp_status":            {"Log UDP status data", ValueTypeBool},
		"log_udp_stack":             {"Log UDP stack data", ValueTypeBool},
		"log_udp_summary":           {"Log UDP summary data", ValueTypeBool},
		"log_udp_batch":             {"Log UDP batch data", ValueTypeBool},
		"log_udp_span":              {"Log UDP span data", ValueTypeBool},
		"log_index_traversal_warning_count": {"Index traversal warning threshold count", ValueTypeNum},
		"log_sql_parsing_fail_enabled":      {"Log SQL parsing failures", ValueTypeBool},

		// Object management
		"object_deadtime_ms":          {"Object dead time threshold in ms", ValueTypeNum},
		"object_inactive_alert_level": {"Alert level for inactive objects (0=disabled)", ValueTypeNum},

		// XLog / Profile
		"xlog_queue_size":             {"XLog queue size for real-time streaming", ValueTypeNum},
		"xlog_realtime_lower_bound_ms": {"Minimum elapsed ms for real-time XLog", ValueTypeNum},
		"xlog_pasttime_lower_bound_ms": {"Minimum elapsed ms for past-time XLog", ValueTypeNum},
		"profile_queue_size":           {"Profile write queue size", ValueTypeNum},
		"text_cache_max_size":          {"Maximum text cache entries", ValueTypeNum},

		// Compression
		"compress_xlog_enabled":    {"Enable XLog compression", ValueTypeBool},
		"compress_profile_enabled": {"Enable profile compression", ValueTypeBool},

		// Purge / Retention
		"day_container_keep_hours":           {"Hours to keep day containers open", ValueTypeNum},
		"mgr_purge_enabled":                 {"Enable automatic data purge", ValueTypeBool},
		"mgr_purge_disk_usage_pct":          {"Disk usage threshold for purging", ValueTypeNum},
		"mgr_purge_profile_keep_days":       {"Days to keep profile data", ValueTypeNum},
		"mgr_purge_xlog_keep_days":          {"Days to keep XLog data", ValueTypeNum},
		"mgr_purge_counter_keep_days":       {"Days to keep counter data", ValueTypeNum},
		"mgr_purge_realtime_counter_keep_days": {"Days to keep realtime counter data", ValueTypeNum},
		"mgr_purge_daily_text_days":         {"Days to keep daily text data", ValueTypeNum},
		"mgr_purge_sum_data_days":           {"Days to keep summary data", ValueTypeNum},

		// Text DB
		"mgr_text_db_daily_service_enabled": {"Enable daily text DB for services", ValueTypeBool},
		"mgr_text_db_daily_api_enabled":     {"Enable daily text DB for APIs", ValueTypeBool},
		"mgr_text_db_daily_ua_enabled":      {"Enable daily text DB for user agents", ValueTypeBool},

		// Directories
		"plugin_dir":     {"Plugin directory path", ValueTypeString},
		"plugin_enabled": {"Enable plugin system", ValueTypeBool},
		"client_dir":     {"Client file directory path", ValueTypeString},
		"temp_dir":       {"Temporary data directory path", ValueTypeString},

		// GeoIP
		"geoip_enabled":         {"Enable GeoIP lookups", ValueTypeBool},
		"geoip_data_city_file":  {"GeoIP city database file path", ValueTypeString},

		// SQL & features
		"sql_table_parsing_enabled": {"Enable SQL table name parsing", ValueTypeBool},
		"tagcnt_enabled":            {"Enable tag counting", ValueTypeBool},
		"req_search_xlog_max_count": {"Maximum XLog count for search requests", ValueTypeNum},
		"visitor_hourly_count_enabled": {"Enable hourly visitor counting", ValueTypeBool},

		// External link
		"ext_link_name":        {"External link display name", ValueTypeString},
		"ext_link_url_pattern": {"External link URL pattern", ValueTypeString},
	}
}
