package config

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// StartWatcher starts a background goroutine that checks the config file
// for changes every interval and reloads it if modified.
func StartWatcher(ctx context.Context, filePath string, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := Get()
				if current == nil {
					continue
				}
				info, err := os.Stat(filePath)
				if err != nil {
					continue
				}
				if info.ModTime().After(current.modTime) {
					newCfg, err := Load(filePath)
					if err != nil {
						slog.Error("config reload failed", "error", err)
						continue
					}
					globalConfig.Store(newCfg)
					slog.Info("config reloaded", "file", filePath)
				}
			}
		}
	}()
}
