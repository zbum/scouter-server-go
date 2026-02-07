package service

import (
	"os"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
)

// RegisterConfigureHandlers registers configuration management handlers.
func RegisterConfigureHandlers(r *Registry, version string) {

	// GET_CONFIGURE_SERVER: Read the config file and return its contents.
	r.Register(protocol.GET_CONFIGURE_SERVER, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack (even though not needed)
		pack.ReadPack(din)

		resp := &pack.MapPack{}

		cfgPath := config.Get().FilePath()
		if cfgPath == "" {
			resp.PutStr("configContents", "")
		} else {
			data, err := os.ReadFile(cfgPath)
			if err != nil {
				// File doesn't exist or can't be read - return empty
				resp.PutStr("configContents", "")
			} else {
				resp.PutStr("configContents", string(data))
			}
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SET_CONFIGURE_SERVER: Write new configuration content to the config file.
	r.Register(protocol.SET_CONFIGURE_SERVER, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		configContents := param.GetText("configContents")

		cfgPath := config.Get().FilePath()
		if cfgPath != "" {
			err = os.WriteFile(cfgPath, []byte(configContents), 0644)
		}

		resp := &pack.MapPack{}
		if err != nil {
			resp.PutStr("result", "error: "+err.Error())
		} else {
			resp.PutStr("result", "ok")
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// LIST_CONFIGURE_SERVER: List all configuration keys and their descriptions.
	r.Register(protocol.LIST_CONFIGURE_SERVER, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack
		pack.ReadPack(din)

		// Create a map of known config keys with descriptions
		configDescs := map[string]string{
			"server_id":              "Server identifier (default: 0)",
			"net_udp_listen_port":    "UDP listen port for agent data (default: 6100)",
			"net_tcp_listen_port":    "TCP listen port for client connections (default: 6100)",
			"net_http_port":          "HTTP API port (default: 6180)",
			"net_http_enabled":       "Enable HTTP API server (default: false)",
			"db_dir":                 "Database directory path (default: ./database)",
			"log_dir":                "Log directory path (default: ./logs)",
			"log_rotation_enabled":   "Enable log rotation (default: true)",
			"log_keep_days":          "Number of days to keep log files (default: 30)",
			"db_keep_days":           "Number of days to keep database files (default: 30)",
			"db_max_disk_usage_pct":  "Maximum disk usage percentage (default: 80)",
			"object_deadtime_ms":     "Object dead time in milliseconds (default: 30000)",
			"xlog_queue_size":        "XLog queue size (default: 10000)",
			"debug":                  "Enable debug logging (default: false)",
		}

		resp := &pack.MapPack{}
		for key, desc := range configDescs {
			resp.PutStr(key, desc)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})
}
