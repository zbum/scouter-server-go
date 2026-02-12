package logging

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	serverLogPrefix = "server-"
	serverLogSuffix = ".log"
	serverLogFixed  = "server.log"
	dateFormat      = "20060102"
)

// RotatingWriter is an io.Writer that writes to both stdout and a daily-rotated
// log file. It matches the Java Scouter server's Logger behavior:
//   - Rotation enabled:  server-YYYYMMDD.log, new file each day
//   - Rotation disabled: server.log (fixed name)
//   - Old log files are cleaned up based on keepDays
type RotatingWriter struct {
	mu              sync.Mutex
	logDir          string
	rotationEnabled bool
	keepDays        int

	currentFile *os.File
	currentDate string // YYYYMMDD of the open file
}

// NewRotatingWriter creates a RotatingWriter.
// The actual file is opened lazily on first Write.
func NewRotatingWriter(logDir string, rotationEnabled bool, keepDays int) *RotatingWriter {
	return &RotatingWriter{
		logDir:          logDir,
		rotationEnabled: rotationEnabled,
		keepDays:        keepDays,
	}
}

// Write implements io.Writer. It writes to both stdout and the log file.
func (w *RotatingWriter) Write(p []byte) (n int, err error) {
	// Always write to stdout
	os.Stdout.Write(p)

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.ensureFile(); err != nil {
		return len(p), nil // don't fail the caller if file logging fails
	}

	n, err = w.currentFile.Write(p)
	if err != nil {
		// File write failed — close and retry next time
		w.closeFileLocked()
		return len(p), nil
	}
	return n, nil
}

// Start begins background goroutines for daily rotation and hourly cleanup.
func (w *RotatingWriter) Start(ctx context.Context) {
	// Daily rotation check (every 5 seconds, matching Java)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.checkRotation()
			}
		}
	}()

	// Hourly cleanup of old log files
	go func() {
		// Run once at startup
		w.clearOldLogs()

		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.clearOldLogs()
			}
		}
	}()
}

// Close closes the underlying file.
func (w *RotatingWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeFileLocked()
}

// ensureFile opens the log file if not already open. Must be called with mu held.
func (w *RotatingWriter) ensureFile() error {
	today := time.Now().Format(dateFormat)

	if w.currentFile != nil && w.currentDate == today {
		return nil
	}

	// Date changed or first open — close old file
	w.closeFileLocked()

	if err := os.MkdirAll(w.logDir, 0755); err != nil {
		return err
	}

	var filename string
	if w.rotationEnabled {
		filename = serverLogPrefix + today + serverLogSuffix
	} else {
		filename = serverLogFixed
	}

	f, err := os.OpenFile(
		filepath.Join(w.logDir, filename),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	w.currentFile = f
	w.currentDate = today
	return nil
}

// closeFileLocked closes the current file. Must be called with mu held.
func (w *RotatingWriter) closeFileLocked() {
	if w.currentFile != nil {
		w.currentFile.Close()
		w.currentFile = nil
		w.currentDate = ""
	}
}

// checkRotation closes the file when the date changes so ensureFile will open a new one.
func (w *RotatingWriter) checkRotation() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.rotationEnabled {
		return
	}

	today := time.Now().Format(dateFormat)
	if w.currentDate != "" && w.currentDate != today {
		w.closeFileLocked()
	}
}

// clearOldLogs deletes log files older than keepDays (matching Java's clearOldLog).
func (w *RotatingWriter) clearOldLogs() {
	if !w.rotationEnabled || w.keepDays <= 0 {
		return
	}

	entries, err := os.ReadDir(w.logDir)
	if err != nil {
		return
	}

	cutoff := time.Now().AddDate(0, 0, -w.keepDays)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, serverLogPrefix) || !strings.HasSuffix(name, serverLogSuffix) {
			continue
		}

		// Extract date part: server-YYYYMMDD.log
		dateStr := strings.TrimPrefix(name, serverLogPrefix)
		dateStr = strings.TrimSuffix(dateStr, serverLogSuffix)
		if len(dateStr) != 8 {
			continue
		}

		fileDate, err := time.Parse(dateFormat, dateStr)
		if err != nil {
			continue
		}

		if fileDate.Before(cutoff) {
			path := filepath.Join(w.logDir, name)
			if err := os.Remove(path); err == nil {
				fmt.Fprintf(os.Stdout, "time=%s level=INFO msg=\"deleted old log file\" path=%s\n",
					time.Now().Format(time.RFC3339), path)
			}
		}
	}
}

// SetupWriter creates a RotatingWriter and returns an io.Writer suitable for slog.
// If rotation is disabled and logDir is empty, returns os.Stdout only.
func SetupWriter(logDir string, rotationEnabled bool, keepDays int) io.Writer {
	if logDir == "" {
		return os.Stdout
	}
	return NewRotatingWriter(logDir, rotationEnabled, keepDays)
}
