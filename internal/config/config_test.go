package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempConf(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "scouter.conf")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_BasicProperties(t *testing.T) {
	path := writeTempConf(t, `
server_id=42
net_udp_listen_port=7100
net_tcp_listen_port=7200
db_dir=/data/scouter
debug=true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.GetString("server_id", "0") != "42" {
		t.Errorf("expected server_id=42, got %q", cfg.GetString("server_id", "0"))
	}
	if cfg.GetInt("net_udp_listen_port", 6100) != 7100 {
		t.Errorf("expected udp port 7100, got %d", cfg.GetInt("net_udp_listen_port", 6100))
	}
	if cfg.GetInt("net_tcp_listen_port", 6100) != 7200 {
		t.Errorf("expected tcp port 7200, got %d", cfg.GetInt("net_tcp_listen_port", 6100))
	}
	if cfg.GetString("db_dir", "./database") != "/data/scouter" {
		t.Errorf("expected db_dir=/data/scouter, got %q", cfg.GetString("db_dir", "./database"))
	}
	if cfg.GetBool("debug", false) != true {
		t.Error("expected debug=true")
	}
}

func TestLoad_Comments(t *testing.T) {
	path := writeTempConf(t, `
# This is a comment
server_id=1

# Another comment

net_udp_listen_port=8100
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetString("server_id", "0") != "1" {
		t.Errorf("expected server_id=1, got %q", cfg.GetString("server_id", "0"))
	}
	if cfg.GetInt("net_udp_listen_port", 6100) != 8100 {
		t.Errorf("expected 8100, got %d", cfg.GetInt("net_udp_listen_port", 6100))
	}
	// Ensure comments are not parsed as keys.
	if cfg.GetString("# This is a comment", "") != "" {
		t.Error("comment should not be a key")
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Load an empty config file.
	path := writeTempConf(t, "")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetString("server_id", "0") != "0" {
		t.Errorf("expected default server_id=0, got %q", cfg.GetString("server_id", "0"))
	}
	if cfg.GetInt("net_udp_listen_port", 6100) != 6100 {
		t.Errorf("expected default 6100, got %d", cfg.GetInt("net_udp_listen_port", 6100))
	}
	if cfg.GetBool("debug", false) != false {
		t.Error("expected default debug=false")
	}
}

func TestGetString(t *testing.T) {
	path := writeTempConf(t, "key1=value1\n  key2 = value with spaces  \n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetString("key1", "") != "value1" {
		t.Errorf("expected value1, got %q", cfg.GetString("key1", ""))
	}
	if cfg.GetString("key2", "") != "value with spaces" {
		t.Errorf("expected 'value with spaces', got %q", cfg.GetString("key2", ""))
	}
	if cfg.GetString("nonexistent", "def") != "def" {
		t.Errorf("expected default 'def', got %q", cfg.GetString("nonexistent", "def"))
	}
}

func TestGetInt(t *testing.T) {
	path := writeTempConf(t, "port=9090\nbad=abc\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetInt("port", 0) != 9090 {
		t.Errorf("expected 9090, got %d", cfg.GetInt("port", 0))
	}
	// Non-numeric should fall back to default.
	if cfg.GetInt("bad", 42) != 42 {
		t.Errorf("expected default 42 for non-numeric value, got %d", cfg.GetInt("bad", 42))
	}
	// Missing key.
	if cfg.GetInt("missing", 100) != 100 {
		t.Errorf("expected default 100, got %d", cfg.GetInt("missing", 100))
	}
}

func TestGetBool(t *testing.T) {
	path := writeTempConf(t, "a=true\nb=false\nc=1\nd=0\ne=yes\nf=no\ng=on\nh=off\ni=TRUE\nj=invalid\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		key      string
		expected bool
	}{
		{"a", true},
		{"b", false},
		{"c", true},
		{"d", false},
		{"e", true},
		{"f", false},
		{"g", true},
		{"h", false},
		{"i", true},
	}
	for _, tc := range cases {
		got := cfg.GetBool(tc.key, !tc.expected) // default is opposite to detect override
		if got != tc.expected {
			t.Errorf("GetBool(%q): expected %v, got %v", tc.key, tc.expected, got)
		}
	}

	// Invalid bool string should return default.
	if cfg.GetBool("j", true) != true {
		t.Error("invalid bool value should return default")
	}
	if cfg.GetBool("j", false) != false {
		t.Error("invalid bool value should return default")
	}
}

func TestLoad_NonExistent(t *testing.T) {
	cfg, err := Load("/tmp/nonexistent_scouter_test_12345.conf")
	if err != nil {
		t.Fatalf("expected no error for missing file, got %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil Config for missing file")
	}
	// Should return defaults.
	if cfg.ServerID() != "0" {
		t.Errorf("expected default ServerID=0, got %q", cfg.ServerID())
	}
	if cfg.UDPPort() != 6100 {
		t.Errorf("expected default UDPPort=6100, got %d", cfg.UDPPort())
	}
}

func TestConvenienceMethods(t *testing.T) {
	path := writeTempConf(t, `
server_id=myserver
net_udp_listen_port=7100
net_tcp_listen_port=7200
net_http_port=8080
net_http_enabled=true
db_dir=/var/scouter/db
log_dir=/var/scouter/logs
log_rotation_enabled=false
log_keep_days=7
db_keep_days=14
db_max_disk_usage_pct=90
object_deadtime_ms=60000
xlog_queue_size=20000
debug=true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		got      interface{}
		expected interface{}
	}{
		{"ServerID", cfg.ServerID(), "myserver"},
		{"UDPPort", cfg.UDPPort(), 7100},
		{"TCPPort", cfg.TCPPort(), 7200},
		{"HTTPPort", cfg.HTTPPort(), 8080},
		{"HTTPEnabled", cfg.HTTPEnabled(), true},
		{"DBDir", cfg.DBDir(), "/var/scouter/db"},
		{"LogDir", cfg.LogDir(), "/var/scouter/logs"},
		{"LogRotationEnabled", cfg.LogRotationEnabled(), false},
		{"LogKeepDays", cfg.LogKeepDays(), 7},
		{"DBKeepDays", cfg.DBKeepDays(), 14},
		{"DBMaxDiskUsagePct", cfg.DBMaxDiskUsagePct(), 90},
		{"ObjectDeadTimeMs", cfg.ObjectDeadTimeMs(), 60000},
		{"XLogQueueSize", cfg.XLogQueueSize(), 20000},
		{"IsDebug", cfg.IsDebug(), true},
	}

	for _, tc := range tests {
		if tc.got != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.expected, tc.got)
		}
	}
}

func TestGetInt64(t *testing.T) {
	path := writeTempConf(t, "big=9223372036854775807\nsmall=42\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetInt64("big", 0) != 9223372036854775807 {
		t.Errorf("expected max int64, got %d", cfg.GetInt64("big", 0))
	}
	if cfg.GetInt64("small", 0) != 42 {
		t.Errorf("expected 42, got %d", cfg.GetInt64("small", 0))
	}
	if cfg.GetInt64("missing", -1) != -1 {
		t.Errorf("expected default -1, got %d", cfg.GetInt64("missing", -1))
	}
}
