package service

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// RegisterServerMgmtHandlers registers server management and monitoring handlers.
func RegisterServerMgmtHandlers(r *Registry, version string, dataDir string) {

	// SERVER_STATUS: Return current server status info.
	// The client reads "used" and "total" to display server memory in the Objects Perf column.
	// Client sends null param (no pack written), so we must NOT read a param pack here.
	r.Register(protocol.SERVER_STATUS, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		resp := &pack.MapPack{}
		resp.PutLong("used", int64(m.Alloc))
		resp.PutLong("total", int64(m.Sys))
		resp.PutLong("time", time.Now().UnixMilli())

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SERVER_ENV: Return server environment variables.
	r.Register(protocol.SERVER_ENV, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack
		pack.ReadPack(din)

		resp := &pack.MapPack{}
		for _, env := range os.Environ() {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 {
				resp.PutStr(parts[0], parts[1])
			}
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SERVER_THREAD_LIST: Return goroutine info.
	r.Register(protocol.SERVER_THREAD_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack
		pack.ReadPack(din)

		stackInfo := string(debug.Stack())

		resp := &pack.MapPack{}
		resp.PutStr("info", stackInfo)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SERVER_DB_LIST: List date directories in the database.
	r.Register(protocol.SERVER_DB_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack
		pack.ReadPack(din)

		dates, err := db.GetDateDirs(dataDir)
		if err != nil {
			dates = []string{}
		}

		resp := &pack.MapPack{}
		listVal := value.NewListValue()
		for _, date := range dates {
			listVal.Value = append(listVal.Value, value.NewTextValue(date))
		}
		resp.Put("dates", listVal)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SERVER_DB_DELETE: Delete a date directory.
	r.Register(protocol.SERVER_DB_DELETE, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		date := param.GetText("date")

		resp := &pack.MapPack{}

		// Validate date format (should be YYYYMMDD)
		if len(date) != 8 {
			resp.PutStr("result", "error: invalid date format")
		} else {
			// Validate it's just alphanumeric to prevent path traversal
			isValid := true
			for _, c := range date {
				if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
					isValid = false
					break
				}
			}

			if !isValid {
				resp.PutStr("result", "error: invalid date format")
			} else {
				dirPath := filepath.Join(dataDir, date)
				err := os.RemoveAll(dirPath)
				if err != nil {
					resp.PutStr("result", "error: "+err.Error())
				} else {
					resp.PutStr("result", "ok")
				}
			}
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SERVER_LOG_LIST: List log files.
	r.Register(protocol.SERVER_LOG_LIST, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Read param pack
		pack.ReadPack(din)

		logDir := config.Get().LogDir()
		entries, err := os.ReadDir(logDir)
		if err != nil {
			entries = []os.DirEntry{}
		}

		resp := &pack.MapPack{}
		listVal := value.NewListValue()
		for _, entry := range entries {
			if !entry.IsDir() {
				listVal.Value = append(listVal.Value, value.NewTextValue(entry.Name()))
			}
		}
		resp.Put("logs", listVal)

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// SERVER_THREAD_DETAIL: Return goroutine stack trace detail.
	r.Register(protocol.SERVER_THREAD_DETAIL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pack.ReadPack(din)

		// Go doesn't have individual thread IDs like Java.
		// Return full goroutine stack as detail info.
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)

		resp := &pack.MapPack{}
		resp.PutStr("info", string(buf[:n]))

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})

	// CHECK_JOB: Poll for pending remote control commands.
	// The client sends a MapPack with "session"; we return nothing (no remote control support yet).
	r.Register(protocol.CHECK_JOB, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		// Must read the parameter pack to keep the protocol in sync
		pack.ReadPack(din)
		// No pending commands - return nothing (no FLAG_HAS_NEXT)
	})

	// SERVER_LOG_DETAIL: Return content of a specific log file.
	r.Register(protocol.SERVER_LOG_DETAIL, func(din *protocol.DataInputX, dout *protocol.DataOutputX, login bool) {
		pk, err := pack.ReadPack(din)
		if err != nil {
			return
		}
		param := pk.(*pack.MapPack)
		fileName := param.GetText("fileName")

		resp := &pack.MapPack{}

		// Security: validate fileName doesn't contain path traversal
		if strings.Contains(fileName, "..") || strings.Contains(fileName, "/") || strings.Contains(fileName, "\\") {
			resp.PutStr("content", "")
		} else {
			logDir := config.Get().LogDir()
			logPath := filepath.Join(logDir, fileName)

			data, err := os.ReadFile(logPath)
			if err != nil {
				resp.PutStr("content", "")
			} else {
				resp.PutStr("content", string(data))
			}
		}

		dout.WriteByte(protocol.FLAG_HAS_NEXT)
		pack.WritePack(dout, resp)
	})
}
