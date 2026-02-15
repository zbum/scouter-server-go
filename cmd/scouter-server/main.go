package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/core"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	scoutercounter "github.com/zbum/scouter-server-go/internal/counter"
	"github.com/zbum/scouter-server-go/internal/db"
	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/db/kv"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/summary"
	dbtext "github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/db/visitor"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	"github.com/zbum/scouter-server-go/internal/geoip"
	scouterhttp "github.com/zbum/scouter-server-go/internal/http"
	"github.com/zbum/scouter-server-go/internal/logging"
	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/netio/service"
	"github.com/zbum/scouter-server-go/internal/netio/tcp"
	"github.com/zbum/scouter-server-go/internal/netio/udp"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
	"github.com/zbum/scouter-server-go/internal/tagcnt"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Printf("scouter-server %s (built %s)\n", Version, BuildTime)
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "rehash" {
		runRehash()
		return
	}

	// --- Startup banner ---
	printBanner()

	// --- Configuration ---
	confFile := "./conf/scouter.conf"
	if f := os.Getenv("SCOUTER_CONF"); f != "" {
		confFile = f
	}
	cfg, err := config.Load(confFile)
	if err != nil {
		slog.Warn("Config load error, using defaults", "path", confFile, "error", err)
		cfg, _ = config.Load("") // load empty defaults
	}

	// Configure logging with file rotation (matching Java's Logger behavior)
	logLevel := slog.LevelInfo
	if cfg.IsDebug() {
		logLevel = slog.LevelDebug
	}
	logWriter := logging.SetupWriter(cfg.LogDir(), cfg.LogRotationEnabled(), cfg.LogKeepDays())
	slog.SetDefault(slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: logLevel})))

	slog.Info("Scouter Server (Go) starting", "version", Version, "build", BuildTime)

	// --- Create temp directory ---
	if err := os.MkdirAll(cfg.TempDir(), 0755); err != nil {
		slog.Warn("Failed to create temp directory", "path", cfg.TempDir(), "error", err)
	}

	// --- Caches ---
	textCache := cache.NewTextCacheWithSize(cfg.TextCacheMaxSize())
	xlogCache := cache.NewXLogCache(cfg.XLogQueueSize())
	counterCache := cache.NewCounterCache()
	objectCache := cache.NewObjectCache()

	// --- Data directory ---
	dataDir := cfg.DBDir()
	if d := os.Getenv("SCOUTER_DATA_DIR"); d != "" {
		dataDir = d
	}
	slog.Info("Data directory", "path", dataDir)

	// --- Graceful shutdown context ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Start log rotation & cleanup goroutines ---
	if rw, ok := logWriter.(*logging.RotatingWriter); ok {
		rw.Start(ctx)
		defer rw.Close()
	}

	// --- Config file watcher (polls every 5 seconds) ---
	config.StartWatcher(ctx, confFile, 5*time.Second)

	// --- Storage writers ---
	textWR := dbtext.NewTextWR(dataDir)
	textWR.Start(ctx)

	xlogWR := xlog.NewXLogWR(dataDir)
	xlogWR.Start(ctx)

	counterWR := counter.NewCounterWR(dataDir)
	counterWR.Start(ctx)

	profileWR := profile.NewProfileWR(dataDir, cfg.ProfileQueueSize())
	profileWR.Start(ctx)

	alertWR := alert.NewAlertWR(dataDir)
	alertWR.Start(ctx)

	summaryWR := summary.NewSummaryWR(dataDir)
	summaryWR.Start(ctx)

	defer textWR.Close()
	defer xlogWR.Close()
	defer counterWR.Close()
	defer profileWR.Close()
	defer alertWR.Close()
	defer summaryWR.Close()

	// --- Storage readers ---
	textRD := dbtext.NewTextRD(dataDir)
	xlogRD := xlog.NewXLogRD(dataDir)
	counterRD := counter.NewCounterRD(dataDir)
	profileRD := profile.NewProfileRD(dataDir)
	alertRD := alert.NewAlertRD(dataDir)
	summaryRD := summary.NewSummaryRD(dataDir)

	defer textRD.Close()
	defer xlogRD.Close()
	defer counterRD.Close()
	defer profileRD.Close()
	defer alertRD.Close()
	defer summaryRD.Close()

	// --- KV stores ---
	globalKV := kv.NewKVStore(dataDir, "global.json")
	globalKV.Start(ctx)
	defer globalKV.Close()

	customKV := kv.NewKVStore(dataDir, "custom.json")
	customKV.Start(ctx)
	defer customKV.Close()

	// --- Alert cache ---
	alertCache := cache.NewAlertCache(1024)

	// --- Core processors ---
	textCore := core.NewTextCore(textCache, textWR)
	xlogGroupPerf := core.NewXLogGroupPerf(textCache, textRD)
	deadTimeout := time.Duration(cfg.ObjectDeadTimeMs()) * time.Millisecond

	// --- Optional subsystems for XLogCore ---
	var xlogOpts []core.XLogCoreOption
	xlogOpts = append(xlogOpts, core.WithObjectCache(objectCache))

	// GeoIP
	var geoIPUtil *geoip.GeoIPUtil
	if cfg.GeoIPEnabled() {
		geoIPUtil = geoip.New(cfg.GeoIPDataCityFile())
		xlogOpts = append(xlogOpts, core.WithGeoIP(geoIPUtil))
		slog.Info("GeoIP lookup enabled", "db", cfg.GeoIPDataCityFile())
	}

	// SQL table parser
	sqlTables := core.NewSqlTables(textCache, textWR)
	xlogOpts = append(xlogOpts, core.WithSqlTables(sqlTables))

	// Visitor counting
	visitorDB := visitor.NewVisitorDB(dataDir)
	visitorDB.StartFlusher(ctx.Done())
	var hourlyDB *visitor.VisitorHourlyDB
	if cfg.VisitorHourlyCountEnabled() {
		hourlyDB = visitor.NewVisitorHourlyDB(dataDir)
		hourlyDB.StartFlusher(ctx.Done())
		slog.Info("Visitor hourly counting enabled")
	}
	visitorCore := core.NewVisitorCore(visitorDB, hourlyDB, objectCache, cfg.VisitorHourlyCountEnabled())
	xlogOpts = append(xlogOpts, core.WithVisitorCore(visitorCore))

	// Tag counting
	var tagCountCore *tagcnt.TagCountCore
	if cfg.TagcntEnabled() {
		tagCountCore = tagcnt.NewTagCountCore(dataDir)
		xlogOpts = append(xlogOpts, core.WithTagCountCore(tagCountCore))
		slog.Info("Tag counting enabled")
	}

	xlogCore := core.NewXLogCore(xlogCache, xlogWR, profileWR, xlogGroupPerf, xlogOpts...)
	perfCountCore := core.NewPerfCountCore(counterCache, counterWR)
	profileCore := core.NewProfileCore(profileWR)
	typeManager := scoutercounter.NewObjectTypeManager()
	alertCore := core.NewAlertCore(alertWR, alertCache)
	agentManager := core.NewAgentManager(objectCache, deadTimeout, typeManager, textCache, textCore, alertCore)
	summaryCore := core.NewSummaryCore(summaryWR)

	// --- Cleanup for optional subsystems ---
	if geoIPUtil != nil {
		defer geoIPUtil.Close()
	}

	// --- Dispatcher ---
	dispatcher := core.NewDispatcher()
	dispatcher.Register(pack.PackTypeText, textCore.Handler())
	dispatcher.Register(pack.PackTypeXLog, xlogCore.Handler())
	dispatcher.Register(pack.PackTypePerfCounter, perfCountCore.Handler())
	dispatcher.Register(pack.PackTypeXLogProfile, profileCore.Handler())
	dispatcher.Register(pack.PackTypeXLogProfile2, profileCore.Handler())
	dispatcher.Register(pack.PackTypeObject, agentManager.Handler())
	dispatcher.Register(pack.PackTypeAlert, alertCore.Handler())
	dispatcher.Register(pack.PackTypeSummary, summaryCore.Handler())

	// --- Zipkin span ingestion (optional) ---
	if cfg.ZipkinEnabled() {
		spanCore := core.NewSpanCore(xlogCache, xlogWR, objectCache, profileWR, textCache)
		dispatcher.Register(pack.PackTypeSpan, spanCore.Handler())
		dispatcher.Register(pack.PackTypeSpanContainer, spanCore.ContainerHandler())
		slog.Info("Zipkin span ingestion enabled")
	}

	// --- Account Manager ---
	confDir := cfg.ConfDir()
	if confDir == "" {
		confDir = "./conf"
	}
	accountManager := login.NewAccountManager(confDir)
	accountManager.StartWatcher(ctx)

	// --- Login / Session ---
	sessions := login.NewSessionManager(accountManager)

	// --- TCP service handlers ---
	registry := service.NewRegistry()
	service.RegisterLoginHandlers(registry, sessions, accountManager, Version)
	service.RegisterServerHandlers(registry, Version)
	service.RegisterObjectHandlers(registry, objectCache, deadTimeout, counterCache, typeManager)
	service.RegisterCounterHandlers(registry, counterCache, objectCache, deadTimeout, counterRD)
	service.RegisterXLogHandlers(registry, xlogCache, xlogRD)
	service.RegisterTextHandlers(registry, textCache, textRD, textWR)
	service.RegisterXLogReadHandlers(registry, xlogRD, profileRD, profileWR, xlogWR)
	service.RegisterCounterReadHandlers(registry, counterRD, objectCache, deadTimeout)
	service.RegisterAlertHandlers(registry, alertRD, alertCache)
	service.RegisterSummaryHandlers(registry, summaryRD)
	service.RegisterCounterExtHandlers(registry, counterCache, objectCache, deadTimeout, counterRD)
	service.RegisterObjectExtHandlers(registry, objectCache, deadTimeout)
	service.RegisterConfigureHandlers(registry, Version, typeManager)
	service.RegisterServerMgmtHandlers(registry, Version, dataDir)
	service.RegisterKVHandlers(registry, globalKV, customKV)
	service.RegisterActiveSpeedHandlers(registry, counterCache, objectCache, deadTimeout)
	service.RegisterLoginExtHandlers(registry, sessions, accountManager)
	service.RegisterAccountHandlers(registry, accountManager)
	service.RegisterVisitorHandlers(registry, visitorDB, hourlyDB, objectCache, deadTimeout)
	service.RegisterAlertExtHandlers(registry, summaryRD)
	service.RegisterGroupHandlers(registry, xlogGroupPerf, textCache)

	// --- UDP pipeline ---
	processor := udp.NewNetDataProcessor(dispatcher, 4)
	udpConfig := udp.ServerConfig{
		ListenIP:   cfg.NetUDPListenIP(),
		ListenPort: cfg.UDPPort(),
		BufSize:    cfg.NetUDPPacketBufferSize(),
		RcvBufSize: cfg.NetUDPSoRcvbufSize(),
	}
	udpServer := udp.NewServer(udpConfig, processor)

	// --- TCP server ---
	tcpConfig := tcp.ServerConfig{
		ListenIP:        cfg.NetTCPListenIP(),
		ListenPort:      cfg.TCPPort(),
		ClientTimeout:   time.Duration(cfg.NetTcpClientSoTimeoutMs()) * time.Millisecond,
		AgentSoTimeout:  time.Duration(cfg.NetTcpAgentSoTimeoutMs()) * time.Millisecond,
		ServicePoolSize: cfg.NetTcpServicePoolSize(),
		AgentConfig: tcp.AgentManagerConfig{
			KeepaliveInterval: time.Duration(cfg.NetTcpAgentKeepaliveIntervalMs()) * time.Millisecond,
			GetConnWait:       time.Duration(cfg.NetTcpGetAgentConnectionWaitMs()) * time.Millisecond,
		},
	}
	tcpServer := tcp.NewServer(tcpConfig, registry, sessions)

	// --- Agent proxy handlers (requires tcpServer for agent RPC) ---
	service.RegisterAgentProxyHandlers(registry, tcpServer, objectCache, deadTimeout)
	service.RegisterConfigureExtHandlers(registry, tcpServer)

	// --- Text cache reset (sends OBJECT_RESET_CACHE to agents on date change) ---
	textCacheReset := core.NewTextCacheReset(objectCache, deadTimeout, tcpServer)
	textCacheReset.Start(ctx)

	// --- Day container purger ---
	purger := db.NewDayContainerPurger(cfg.DayContainerKeepHours(),
		xlogWR, xlogRD,
		counterWR, counterRD,
		profileWR, profileRD,
		alertWR, alertRD,
		summaryWR, summaryRD,
	)
	purger.Start(ctx)
	slog.Info("Day container purger started", "keepHours", cfg.DayContainerKeepHours())

	// --- Auto-delete scheduler ---
	if keepDays := cfg.DBKeepDays(); keepDays > 0 {
		cleaner := db.NewAutoDeleteScheduler(dataDir, keepDays)
		cleaner.Start(ctx)
		slog.Info("Auto-delete scheduler started", "keepDays", keepDays)
	}

	// --- Per-type data purge scheduler (matching Java's AutoDeleteScheduler) ---
	if cfg.MgrPurgeEnabled() {
		dataPurger := db.NewDataPurgeScheduler(dataDir,
			cfg.MgrPurgeProfileKeepDays(),
			cfg.MgrPurgeXLogKeepDays(),
			cfg.MgrPurgeSumDataDays(),
			cfg.MgrPurgeCounterKeepDays(),
			cfg.MgrPurgeRealtimeCounterKeepDays(),
			cfg.MgrPurgeDailyTextDays(),
			cfg.MgrPurgeDiskUsagePct(),
		)
		dataPurger.Start(ctx)
		slog.Info("Data purge scheduler started",
			"profileKeepDays", cfg.MgrPurgeProfileKeepDays(),
			"xlogKeepDays", cfg.MgrPurgeXLogKeepDays(),
			"sumKeepDays", cfg.MgrPurgeSumDataDays(),
			"counterKeepDays", cfg.MgrPurgeCounterKeepDays(),
			"realtimeCounterKeepDays", cfg.MgrPurgeRealtimeCounterKeepDays(),
			"dailyTextKeepDays", cfg.MgrPurgeDailyTextDays(),
			"diskUsagePct", cfg.MgrPurgeDiskUsagePct(),
		)
	}

	// --- HTTP API server (optional) ---
	if cfg.HTTPEnabled() {
		httpSrv := scouterhttp.NewServer(scouterhttp.ServerConfig{
			Port:                 cfg.HTTPPort(),
			CorsAllowOrigin:      cfg.NetHTTPApiCorsAllowOrigin(),
			CorsAllowCredentials: cfg.NetHTTPApiCorsAllowCredentials(),
			GzipEnabled:          cfg.NetHTTPApiGzipEnabled(),
			ClientDir:            cfg.ClientDir(),
			AccountManager:       accountManager,
			SessionTimeout:       time.Duration(cfg.NetHTTPApiSessionTimeout()) * time.Second,
			ObjectCache:          objectCache,
			CounterCache:         counterCache,
			XLogCache:            xlogCache,
			TextCache:            textCache,
			XLogRD:               xlogRD,
			CounterRD:            counterRD,
			AlertRD:              alertRD,
		})
		go func() {
			if err := httpSrv.Start(ctx); err != nil {
				slog.Error("HTTP API server error", "error", err)
			}
		}()
	}

	// --- PID file ---
	pidFile := fmt.Sprintf("%d.scouter", os.Getpid())
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
		slog.Warn("Failed to create PID file", "file", pidFile, "error", err)
	} else {
		slog.Info("PID file created", "file", pidFile)
		defer os.Remove(pidFile)
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if _, err := os.Stat(pidFile); os.IsNotExist(err) {
						slog.Info("PID file deleted, shutting down", "file", pidFile)
						cancel()
						return
					}
				}
			}
		}()
	}

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start UDP server in background
	go func() {
		slog.Info("UDP server starting", "port", udpConfig.ListenPort)
		if err := udpServer.Start(ctx); err != nil {
			slog.Error("UDP server error", "error", err)
		}
	}()

	// Start TCP server (blocks until context cancelled)
	slog.Info("TCP server starting", "port", tcpConfig.ListenPort)
	if err := tcpServer.Start(ctx); err != nil {
		slog.Error("TCP server error", "error", err)
		os.Exit(1)
	}

	slog.Info("Scouter Server stopped")
}

func runRehash() {
	// --- Configuration ---
	confFile := "./conf/scouter.conf"
	if f := os.Getenv("SCOUTER_CONF"); f != "" {
		confFile = f
	}
	cfg, err := config.Load(confFile)
	if err != nil {
		slog.Warn("Config load error, using defaults", "path", confFile, "error", err)
		cfg, _ = config.Load("")
	}

	dataDir := cfg.DBDir()
	if d := os.Getenv("SCOUTER_DATA_DIR"); d != "" {
		dataDir = d
	}

	// Default: 128MB, override with --size flag
	hashSizeMB := 128
	for i, arg := range os.Args {
		if arg == "--size" && i+1 < len(os.Args) {
			if n, err := fmt.Sscanf(os.Args[i+1], "%d", &hashSizeMB); n != 1 || err != nil {
				fmt.Fprintf(os.Stderr, "Invalid --size value: %s\n", os.Args[i+1])
				os.Exit(1)
			}
		}
	}

	fmt.Printf("Rehash text index: dataDir=%s, newHashSizeMB=%d\n", dataDir, hashSizeMB)
	fmt.Printf("This will rebuild .hfile and .kfile for all text divs.\n")
	fmt.Printf("Old files will be backed up as .hfile.bak / .kfile.bak\n\n")

	results, err := dbtext.RehashAll(dataDir, hashSizeMB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Rehash failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n=== Rehash Complete ===\n")
	for _, r := range results {
		if r.Records < 0 {
			fmt.Printf("  %-12s  (skipped - already at %dMB)\n", r.Div, r.HashMB)
			continue
		}
		if r.Records == 0 {
			fmt.Printf("  %-12s  (skipped - empty)\n", r.Div)
			continue
		}
		avgOld := float64(r.Records) / float64(max(r.OldBucket, 1))
		avgNew := float64(r.Records) / float64(max(r.NewBucket, 1))
		fmt.Printf("  %-12s  records=%-12d  %dMB  chain: %.1f → %.1f  elapsed=%s\n",
			r.Div, r.Records, r.HashMB, avgOld, avgNew, r.Elapsed.Round(time.Millisecond))
	}
}

func printBanner() {
	fmt.Printf(`  ____                  _
 / ___|  ___ ___  _   _| |_ ___ _ __
 \___ \ / __/   \| | | | __/ _ \ '__|
  ___) | (_| (+) | |_| | ||  __/ |
 |____/ \___\___/ \__,_|\__\___|_|
 Scouter Server (Go) version %s %s
 Open Source S/W Performance Monitoring
 Runtime: %s %s/%s

`, Version, BuildTime, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
