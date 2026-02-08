package service

import (
	"os"
	"strconv"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/counter"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterConfigureHandlers registers configuration management handlers.
func RegisterConfigureHandlers(r *Registry, version string, typeManager *counter.ObjectTypeManager) {

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

	// GET_XML_COUNTER: Return counter definitions XML for the client's CounterEngine.
	r.Register(protocol.GET_XML_COUNTER, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		resp := &pack.MapPack{}
		resp.Put("default", &value.BlobValue{Value: counter.DefaultCountersXML})

		// Load custom counters.site.xml if it exists on disk
		var customData []byte
		if cfg := config.Get(); cfg != nil {
			confDir := cfg.ConfDir()
			if confDir != "" {
				customPath := confDir + "/counters.site.xml"
				if data, err := os.ReadFile(customPath); err == nil && len(data) > 0 {
					customData = data
				}
			}
		}

		// If no file-based custom XML, use dynamically registered types
		if customData == nil && typeManager != nil {
			customData = typeManager.GetCustomXML()
		}

		if customData != nil {
			resp.Put("custom", &value.BlobValue{Value: customData})
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// LIST_CONFIGURE_SERVER: List all configuration keys and their descriptions.
	r.Register(protocol.LIST_CONFIGURE_SERVER, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack
		pack.ReadPack(din)

		metaMap := config.ConfigMetaMap()
		resp := &pack.MapPack{}
		for key, meta := range metaMap {
			resp.PutStr(key, meta.Desc)
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})
}

// RegisterConfigureExtHandlers registers configuration handlers that require
// agent proxy support (hybrid server/agent handlers).
func RegisterConfigureExtHandlers(r *Registry, caller AgentCaller) {

	// CONFIGURE_DESC: Return config key descriptions.
	// objHash==0 → server config, objHash>0 → proxy to agent.
	r.Register(protocol.CONFIGURE_DESC, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHash := param.GetInt("objHash")

		if objHash == 0 {
			metaMap := config.ConfigMetaMap()
			resp := &pack.MapPack{}
			for key, meta := range metaMap {
				resp.PutStr(key, meta.Desc)
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, resp)
		} else {
			result := caller.AgentCallSingle(objHash, protocol.CONFIGURE_DESC, param)
			if result != nil {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, result)
			}
		}
	})

	// CONFIGURE_VALUE_TYPE: Return config key value types (1=string, 2=num, 3=bool).
	// objHash==0 → server config, objHash>0 → proxy to agent.
	r.Register(protocol.CONFIGURE_VALUE_TYPE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHash := param.GetInt("objHash")

		if objHash == 0 {
			metaMap := config.ConfigMetaMap()
			resp := &pack.MapPack{}
			for key, meta := range metaMap {
				resp.PutStr(key, strconv.Itoa(meta.ValueType))
			}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, resp)
		} else {
			result := caller.AgentCallSingle(objHash, protocol.CONFIGURE_VALUE_TYPE, param)
			if result != nil {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, result)
			}
		}
	})

	// CONFIGURE_VALUE_TYPE_DESC: Return detailed metadata for complex value types.
	// objHash==0 → server (currently empty), objHash>0 → proxy to agent.
	r.Register(protocol.CONFIGURE_VALUE_TYPE_DESC, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		objHash := param.GetInt("objHash")

		if objHash == 0 {
			// Server has no complex value types for now
			resp := &pack.MapPack{}
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, resp)
		} else {
			result := caller.AgentCallSingle(objHash, protocol.CONFIGURE_VALUE_TYPE_DESC, param)
			if result != nil {
				dout.WriteByte(protocol.FLAG_HAS_NEXT)
				pack.WritePack(dout, result)
			}
		}
	})

	// GET_CONFIGURE_COUNTERS_SITE: Read custom counters.site.xml from conf dir.
	r.Register(protocol.GET_CONFIGURE_COUNTERS_SITE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		resp := &pack.MapPack{}
		contents := ""
		if cfg := config.Get(); cfg != nil {
			confDir := cfg.ConfDir()
			if confDir != "" {
				if data, err := os.ReadFile(confDir + "/counters.site.xml"); err == nil {
					contents = string(data)
				}
			}
		}
		resp.PutStr("contents", contents)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SET_CONFIGURE_COUNTERS_SITE: Save custom counters.site.xml to conf dir.
	r.Register(protocol.SET_CONFIGURE_COUNTERS_SITE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		contents := param.GetText("contents")

		success := false
		if cfg := config.Get(); cfg != nil {
			confDir := cfg.ConfDir()
			if confDir != "" {
				if err := os.WriteFile(confDir+"/counters.site.xml", []byte(contents), 0644); err == nil {
					success = true
				}
			}
		}

		resp := &pack.MapPack{}
		resp.PutStr("result", strconv.FormatBool(success))
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// GET_CONFIGURE_TELEGRAF: Read telegraf config file.
	r.Register(protocol.GET_CONFIGURE_TELEGRAF, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		resp := &pack.MapPack{}
		contents := ""
		if cfg := config.Get(); cfg != nil {
			confDir := cfg.ConfDir()
			if confDir != "" {
				if data, err := os.ReadFile(confDir + "/scouter-telegraf.xml"); err == nil {
					contents = string(data)
				}
			}
		}
		resp.PutStr("tgConfigContents", contents)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SET_CONFIGURE_TELEGRAF: Save telegraf config file.
	r.Register(protocol.SET_CONFIGURE_TELEGRAF, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		contents := param.GetText("tgConfigContents")

		success := false
		if cfg := config.Get(); cfg != nil {
			confDir := cfg.ConfDir()
			if confDir != "" {
				if err := os.WriteFile(confDir+"/scouter-telegraf.xml", []byte(contents), 0644); err == nil {
					success = true
				}
			}
		}

		resp := &pack.MapPack{}
		resp.PutStr("result", strconv.FormatBool(success))
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})
}
