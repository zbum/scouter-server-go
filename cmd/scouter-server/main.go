package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/core"
	"github.com/zbum/scouter-server-go/internal/core/cache"
	"github.com/zbum/scouter-server-go/internal/db"
	"github.com/zbum/scouter-server-go/internal/db/alert"
	"github.com/zbum/scouter-server-go/internal/db/counter"
	"github.com/zbum/scouter-server-go/internal/db/kv"
	"github.com/zbum/scouter-server-go/internal/db/profile"
	"github.com/zbum/scouter-server-go/internal/db/summary"
	dbtext "github.com/zbum/scouter-server-go/internal/db/text"
	"github.com/zbum/scouter-server-go/internal/db/xlog"
	scouterhttp "github.com/zbum/scouter-server-go/internal/http"
	"github.com/zbum/scouter-server-go/internal/login"
	"github.com/zbum/scouter-server-go/internal/netio/service"
	"github.com/zbum/scouter-server-go/internal/netio/tcp"
	"github.com/zbum/scouter-server-go/internal/netio/udp"
	"github.com/zbum/scouter-server-go/internal/protocol/pack"
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

	// --- Configuration ---
	confFile := "./scouter.conf"
	if f := os.Getenv("SCOUTER_CONF"); f != "" {
		confFile = f
	}
	cfg, err := config.Load(confFile)
	if err != nil {
		slog.Warn("Config load error, using defaults", "path", confFile, "error", err)
		cfg, _ = config.Load("") // load empty defaults
	}

	// Configure logging
	logLevel := slog.LevelInfo
	if cfg.IsDebug() {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	slog.Info("Scouter Server (Go) starting", "version", Version, "build", BuildTime)

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

	// --- Config file watcher (polls every 5 seconds) ---
	config.StartWatcher(ctx, confFile, 5*time.Second)

	// --- Storage writers ---
	textWR := dbtext.NewTextWR(dataDir)
	textWR.Start(ctx)

	xlogWR := xlog.NewXLogWR(dataDir)
	xlogWR.Start(ctx)

	counterWR := counter.NewCounterWR(dataDir)
	counterWR.Start(ctx)

	profileWR := profile.NewProfileWR(dataDir)
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

	// --- Core processors ---
	textCore := core.NewTextCore(textCache, textWR)
	xlogCore := core.NewXLogCore(xlogCache, xlogWR, profileWR)
	perfCountCore := core.NewPerfCountCore(counterCache, counterWR)
	profileCore := core.NewProfileCore(profileWR)
	deadTimeout := time.Duration(cfg.ObjectDeadTimeMs()) * time.Millisecond
	agentManager := core.NewAgentManager(objectCache, deadTimeout)
	alertCore := core.NewAlertCore(alertWR)
	summaryCore := core.NewSummaryCore(summaryWR)

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

	// --- Login / Session ---
	sessions := login.NewSessionManager("")

	// --- TCP service handlers ---
	registry := service.NewRegistry()
	service.RegisterLoginHandlers(registry, sessions, Version)
	service.RegisterServerHandlers(registry, Version)
	service.RegisterObjectHandlers(registry, objectCache, deadTimeout)
	service.RegisterCounterHandlers(registry, counterCache, objectCache, deadTimeout, counterRD)
	service.RegisterXLogHandlers(registry, xlogCache, xlogRD)
	service.RegisterTextHandlers(registry, textCache, textRD)
	service.RegisterXLogReadHandlers(registry, xlogRD, profileRD)
	service.RegisterCounterReadHandlers(registry, counterRD, objectCache, deadTimeout)
	service.RegisterAlertHandlers(registry, alertRD)
	service.RegisterSummaryHandlers(registry, summaryRD)
	service.RegisterCounterExtHandlers(registry, counterCache, objectCache, deadTimeout, counterRD)
	service.RegisterObjectExtHandlers(registry, objectCache, deadTimeout)
	service.RegisterConfigureHandlers(registry, Version)
	service.RegisterServerMgmtHandlers(registry, Version, dataDir)
	service.RegisterKVHandlers(registry, globalKV, customKV)

	// --- UDP pipeline ---
	processor := udp.NewNetDataProcessor(dispatcher, 4)
	udpConfig := udp.DefaultServerConfig()
	udpConfig.ListenPort = cfg.UDPPort()
	udpServer := udp.NewServer(udpConfig, processor)

	// --- TCP server ---
	tcpConfig := tcp.DefaultServerConfig()
	tcpConfig.ListenPort = cfg.TCPPort()
	tcpServer := tcp.NewServer(tcpConfig, registry, sessions)

	// --- Agent proxy handlers (requires tcpServer for agent RPC) ---
	service.RegisterAgentProxyHandlers(registry, tcpServer)

	// --- Day container purger ---
	purger := db.NewDayContainerPurger(cfg.DayContainerKeepHours(),
		xlogWR, xlogRD,
		counterWR, counterRD,
		textWR, textRD,
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

	// --- HTTP API server (optional) ---
	if cfg.HTTPEnabled() {
		httpSrv := scouterhttp.NewServer(scouterhttp.ServerConfig{
			Port:         cfg.HTTPPort(),
			ObjectCache:  objectCache,
			CounterCache: counterCache,
			XLogCache:    xlogCache,
			TextCache:    textCache,
			XLogRD:       xlogRD,
			CounterRD:    counterRD,
			AlertRD:      alertRD,
		})
		go func() {
			if err := httpSrv.Start(ctx); err != nil {
				slog.Error("HTTP API server error", "error", err)
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
