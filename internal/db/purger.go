package db

import (
	"context"
	"log/slog"
	"time"
)

// DayPurgeable is implemented by components that cache per-day containers in memory.
type DayPurgeable interface {
	PurgeOldDays(keepDates map[string]bool)
}

// DayContainerPurger periodically closes old day containers to free memory and file handles.
type DayContainerPurger struct {
	targets   []DayPurgeable
	keepHours int
	interval  time.Duration
}

// NewDayContainerPurger creates a purger that keeps containers for the last keepHours.
func NewDayContainerPurger(keepHours int, targets ...DayPurgeable) *DayContainerPurger {
	if keepHours <= 0 {
		keepHours = 48
	}
	return &DayContainerPurger{
		targets:   targets,
		keepHours: keepHours,
		interval:  1 * time.Hour,
	}
}

// Start begins periodic purging in the background.
func (p *DayContainerPurger) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.purge()
			}
		}
	}()
}

func (p *DayContainerPurger) purge() {
	keepDates := p.buildKeepDates()
	purged := 0
	for _, t := range p.targets {
		t.PurgeOldDays(keepDates)
		purged++
	}
	slog.Debug("Day container purge completed", "keepDates", len(keepDates), "targets", purged)
}

func (p *DayContainerPurger) buildKeepDates() map[string]bool {
	now := time.Now()
	dates := make(map[string]bool)
	for h := 0; h < p.keepHours; h += 24 {
		t := now.Add(-time.Duration(h) * time.Hour)
		dates[t.Format("20060102")] = true
	}
	// Always include today and yesterday explicitly
	dates[now.Format("20060102")] = true
	dates[now.Add(-24*time.Hour).Format("20060102")] = true
	return dates
}
