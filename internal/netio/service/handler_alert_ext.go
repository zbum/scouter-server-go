package service

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db/summary"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
	"github.com/zbum/scouter-server-go/internal/util"
)

// RegisterAlertExtHandlers registers extended alert handlers:
// ALERT_TITLE_COUNT, alert scripting, and alert descriptor commands.
func RegisterAlertExtHandlers(r *Registry, summaryRD *summary.SummaryRD) {

	// ALERT_TITLE_COUNT: aggregate alert summaries by title with hourly breakdowns.
	r.Register(protocol.ALERT_TITLE_COUNT, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")
		stime := param.GetLong("stime")
		etime := param.GetLong("etime")

		// Aggregate: title → MapPack{title, level, count(MapValue of HHMM→count)}
		valueMap := make(map[string]*pack.MapPack)

		summaryRD.ReadRangeWithTime(date, SummaryTypeAlert, stime, etime, func(timeMs int64, data []byte) {
			d := protocol.NewDataInputX(data)
			p, err := pack.ReadPack(d)
			if err != nil {
				slog.Debug("ALERT_TITLE_COUNT: failed to read summary pack", "error", err)
				return
			}
			sp, ok := p.(*pack.SummaryPack)
			if !ok || sp.Table == nil {
				return
			}

			hhmm := util.HHMM(timeMs)

			titleLv := getListFromMapValue(sp.Table, "title")
			levelLv := getListFromMapValue(sp.Table, "level")
			countLv := getListFromMapValue(sp.Table, "count")
			if titleLv == nil || countLv == nil {
				return
			}

			for i := 0; i < len(titleLv.Value); i++ {
				title := titleLv.GetString(i)
				count := countLv.GetInt(i)
				var level byte
				if levelLv != nil && i < len(levelLv.Value) {
					level = byte(levelLv.GetLong(i))
				}

				mp, exists := valueMap[title]
				if !exists {
					mp = &pack.MapPack{}
					mp.PutStr("title", title)
					mp.Put("level", value.NewDecimalValue(int64(level)))
					mp.Put("count", value.NewMapValue())
					valueMap[title] = mp
				}

				mv := mp.Get("count").(*value.MapValue)
				// Add count to existing HHMM bucket or create new
				if existing, found := mv.Get(hhmm); found {
					if dv, ok := existing.(*value.DecimalValue); ok {
						dv.Value += int64(count)
					}
				} else {
					mv.Put(hhmm, value.NewDecimalValue(int64(count)))
				}
			}
		})

		for _, mp := range valueMap {
			dout.WriteByte(protocol.FLAG_HAS_NEXT)
			pack.WritePack(dout, mp)
		}
	})

	// GET_ALERT_SCRIPTING_CONTETNS: read alert rule script file.
	r.Register(protocol.GET_ALERT_SCRIPTING_CONTETNS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counterName")

		contents := ""
		if pluginDir := getPluginDir(); pluginDir != "" {
			path := filepath.Join(pluginDir, counterName+".alert")
			if data, err := os.ReadFile(path); err == nil {
				contents = string(data)
			}
		}

		resp := &pack.MapPack{}
		resp.PutStr("contents", contents)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// GET_ALERT_SCRIPTING_CONFIG_CONTETNS: read alert config file.
	r.Register(protocol.GET_ALERT_SCRIPTING_CONFIG_CONTETNS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counterName")

		contents := ""
		if pluginDir := getPluginDir(); pluginDir != "" {
			path := filepath.Join(pluginDir, counterName+".conf")
			if data, err := os.ReadFile(path); err == nil {
				contents = string(data)
			}
		}

		resp := &pack.MapPack{}
		resp.PutStr("contents", contents)
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SAVE_ALERT_SCRIPTING_CONTETNS: save alert rule script file.
	r.Register(protocol.SAVE_ALERT_SCRIPTING_CONTETNS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counterName")
		contents := param.GetText("contents")

		success := false
		if pluginDir := getPluginDir(); pluginDir != "" {
			os.MkdirAll(pluginDir, 0755)
			path := filepath.Join(pluginDir, counterName+".alert")
			if err := os.WriteFile(path, []byte(contents), 0644); err == nil {
				success = true
			}
		}

		resp := &pack.MapPack{}
		resp.Put("success", &value.BooleanValue{Value: success})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS: save alert config file.
	r.Register(protocol.SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		counterName := param.GetText("counterName")
		contents := param.GetText("contents")

		success := false
		if pluginDir := getPluginDir(); pluginDir != "" {
			os.MkdirAll(pluginDir, 0755)
			path := filepath.Join(pluginDir, counterName+".conf")
			if err := os.WriteFile(path, []byte(contents), 0644); err == nil {
				success = true
			}
		}

		resp := &pack.MapPack{}
		resp.Put("success", &value.BooleanValue{Value: success})
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// GET_ALERT_SCRIPT_LOAD_MESSAGE: return script load messages.
	// Go server does not support Groovy scripting, so return empty response.
	r.Register(protocol.GET_ALERT_SCRIPT_LOAD_MESSAGE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		_ = pk.(*pack.MapPack) // consume param

		resp := &pack.MapPack{}
		resp.Put("loop", value.NewDecimalValue(0))
		resp.Put("index", value.NewDecimalValue(0))
		resp.Put("messages", value.NewListValue())
		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// GET_ALERT_REAL_COUNTER_DESC: return RealCounter method descriptions.
	// Go server does not support Java/Groovy alert scripting, return empty.
	r.Register(protocol.GET_ALERT_REAL_COUNTER_DESC, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// No param to read (client sends null)
	})

	// GET_PLUGIN_HELPER_DESC: return PluginHelper method descriptions.
	// Go server does not support Java/Groovy alert scripting, return empty.
	r.Register(protocol.GET_PLUGIN_HELPER_DESC, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// No param to read (client sends null)
	})
}

// getPluginDir returns the configured plugin directory, or empty string.
func getPluginDir() string {
	if cfg := config.Get(); cfg != nil {
		return cfg.PluginDir()
	}
	return ""
}

// getListFromMapValue extracts a ListValue from a MapValue by key.
func getListFromMapValue(mv *value.MapValue, key string) *value.ListValue {
	v, ok := mv.Get(key)
	if !ok || v == nil {
		return nil
	}
	if lv, ok := v.(*value.ListValue); ok {
		return lv
	}
	return nil
}
