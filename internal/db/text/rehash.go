package text

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db/io"
)

// RehashResult holds statistics for a single div rehash operation.
type RehashResult struct {
	Div       string
	Records   int
	OldBucket int
	NewBucket int
	HashMB    int
	Elapsed   time.Duration
}

// RehashAll rehashes all text index files in the permanent text directory.
// The hash size for each div is read from config (MgrTextDbIndexMB).
// fallbackMB is used when config returns the default 1MB (to allow
// overriding small defaults for large datasets).
//
// Memory usage is bounded: only the two hfiles are held in memory.
// Records are streamed directly from old to new without buffering.
func RehashAll(dataDir string, fallbackMB int) ([]RehashResult, error) {
	textDir := filepath.Join(dataDir, textDirName, "text")

	if _, err := os.Stat(textDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("text directory not found: %s", textDir)
	}

	// Discover divs by scanning for .hfile files
	entries, err := os.ReadDir(textDir)
	if err != nil {
		return nil, fmt.Errorf("reading text directory: %w", err)
	}

	var divs []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "text_") && strings.HasSuffix(name, ".hfile") {
			div := strings.TrimSuffix(strings.TrimPrefix(name, "text_"), ".hfile")
			if div != "" {
				divs = append(divs, div)
			}
		}
	}

	if len(divs) == 0 {
		return nil, fmt.Errorf("no text index files found in %s", textDir)
	}

	slog.Info("Rehash: found divs", "divs", divs, "fallbackMB", fallbackMB)

	var results []RehashResult
	for _, div := range divs {
		hashMB := resolveHashSizeMB(div, fallbackMB)
		result, err := rehashDiv(textDir, div, hashMB)
		if err != nil {
			return results, fmt.Errorf("rehash %q failed: %w", div, err)
		}
		results = append(results, *result)
	}

	return results, nil
}

// resolveHashSizeMB returns the target hash size for a div.
// If config has an explicit per-div setting (> default 1MB), use it.
// Otherwise, use fallbackMB.
func resolveHashSizeMB(div string, fallbackMB int) int {
	if cfg := config.Get(); cfg != nil {
		cfgMB := cfg.MgrTextDbIndexMB(div)
		if cfgMB > 1 {
			return cfgMB // explicit config setting
		}
	}
	return fallbackMB
}

// rehashDiv rebuilds the index for a single div by streaming records
// from the old IndexKeyFile directly into a new one.
// Memory usage: old hfile + new hfile + kfile append buffer (16KB).
func rehashDiv(textDir, div string, newHashSizeMB int) (*RehashResult, error) {
	start := time.Now()

	oldPath := filepath.Join(textDir, "text_"+div)
	newPath := filepath.Join(textDir, "text_"+div+"_rehash_tmp")

	slog.Info("Rehash: starting", "div", div, "targetMB", newHashSizeMB)

	// Read old hfile size to report statistics and check if already rehashed
	oldHfileInfo, err := os.Stat(oldPath + ".hfile")
	if err != nil {
		return nil, fmt.Errorf("stat old hfile: %w", err)
	}
	oldBufSize := int(oldHfileInfo.Size()) - 1024 // subtract memHeadReserved
	oldBuckets := oldBufSize / 5

	// Skip if hfile is already the target size
	targetBufSize := newHashSizeMB * 1024 * 1024
	if oldBufSize == targetBufSize {
		slog.Info("Rehash: skipping, already at target size", "div", div, "sizeMB", newHashSizeMB)
		return &RehashResult{
			Div:       div,
			Records:   -1,
			OldBucket: oldBuckets,
			NewBucket: oldBuckets,
			HashMB:    newHashSizeMB,
			Elapsed:   time.Since(start),
		}, nil
	}

	// Clean up any leftover temp files from a previous failed attempt
	os.Remove(newPath + ".hfile")
	os.Remove(newPath + ".kfile")

	// Open old IndexKeyFile — reads existing hfile size from disk
	oldIdx, err := io.NewIndexKeyFile(oldPath, 1) // hashSizeMB ignored for existing files
	if err != nil {
		return nil, fmt.Errorf("open old index: %w", err)
	}

	// First, count records to detect empty divs without creating new files.
	recordCount := 0
	err = oldIdx.Read(func(key []byte, dataPos []byte) {
		recordCount++
	})
	if err != nil {
		oldIdx.Close()
		return nil, fmt.Errorf("count old records: %w", err)
	}

	if recordCount == 0 {
		oldIdx.Close()
		slog.Info("Rehash: skipping empty div", "div", div)
		return &RehashResult{
			Div:       div,
			Records:   0,
			OldBucket: oldBuckets,
			NewBucket: newHashSizeMB * 1024 * 1024 / 5,
			HashMB:    newHashSizeMB,
			Elapsed:   time.Since(start),
		}, nil
	}

	// Create new IndexKeyFile with the target hash size
	var newIdx *io.IndexKeyFile
	newIdx, err = io.NewIndexKeyFile(newPath, newHashSizeMB)
	if err != nil {
		oldIdx.Close()
		return nil, fmt.Errorf("create new index: %w", err)
	}

	// Stream records directly from old to new — no bulk memory allocation.
	inserted := 0
	var insertErr error
	err = oldIdx.Read(func(key []byte, dataPos []byte) {
		if insertErr != nil {
			return
		}
		if err := newIdx.Put(key, dataPos); err != nil {
			insertErr = err
			return
		}
		inserted++
		if inserted%1000000 == 0 {
			slog.Info("Rehash: progress", "div", div, "inserted", inserted, "total", recordCount)
		}
	})

	oldIdx.Close()
	newIdx.Close()

	if err != nil {
		os.Remove(newPath + ".hfile")
		os.Remove(newPath + ".kfile")
		return nil, fmt.Errorf("read old records: %w", err)
	}
	if insertErr != nil {
		os.Remove(newPath + ".hfile")
		os.Remove(newPath + ".kfile")
		return nil, fmt.Errorf("insert record: %w", insertErr)
	}

	newBufSize := newHashSizeMB * 1024 * 1024
	newBuckets := newBufSize / 5

	slog.Info("Rehash: insert complete",
		"div", div,
		"records", inserted,
		"oldBuckets", oldBuckets,
		"newBuckets", newBuckets,
		"avgChain", fmt.Sprintf("%.1f → %.1f",
			float64(inserted)/float64(max(oldBuckets, 1)),
			float64(inserted)/float64(max(newBuckets, 1))),
	)

	// Atomic swap: backup old, rename new
	for _, ext := range []string{".hfile", ".kfile"} {
		oldFile := oldPath + ext
		bakFile := oldPath + ext + ".bak"
		newFile := newPath + ext

		os.Remove(bakFile)

		if err := os.Rename(oldFile, bakFile); err != nil {
			return nil, fmt.Errorf("backup %s: %w", ext, err)
		}
		if err := os.Rename(newFile, oldFile); err != nil {
			os.Rename(bakFile, oldFile)
			return nil, fmt.Errorf("rename new %s: %w", ext, err)
		}
	}

	elapsed := time.Since(start)
	slog.Info("Rehash: completed",
		"div", div,
		"records", inserted,
		"elapsed", elapsed.Round(time.Millisecond),
	)

	return &RehashResult{
		Div:       div,
		Records:   inserted,
		OldBucket: oldBuckets,
		NewBucket: newBuckets,
		HashMB:    newHashSizeMB,
		Elapsed:   elapsed,
	}, nil
}
