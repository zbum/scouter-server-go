package summary

import (
	"os"
	"path/filepath"
	"sync"
)

// SummaryRD is a summary reader.
type SummaryRD struct {
	mu      sync.Mutex
	baseDir string
	days    map[dayKey]*SummaryData
}

// NewSummaryRD creates a new summary reader.
func NewSummaryRD(baseDir string) *SummaryRD {
	return &SummaryRD{
		baseDir: baseDir,
		days:    make(map[dayKey]*SummaryData),
	}
}

// getContainer retrieves or opens a day+type container for reading.
func (r *SummaryRD) getContainer(date string, stype byte) (*SummaryData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := dayKey{date: date, stype: stype}
	container, exists := r.days[key]
	if exists {
		return container, nil
	}

	// Check if directory exists
	dir := filepath.Join(r.baseDir, date, "summary")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, nil // No data for this date
	}

	sd, err := NewSummaryData(dir, stype)
	if err != nil {
		return nil, err
	}

	r.days[key] = sd
	return sd, nil
}

// ReadRange reads summaries in a time range for the given date and type.
func (r *SummaryRD) ReadRange(date string, stype byte, stime, etime int64, handler func(data []byte)) error {
	container, err := r.getContainer(date, stype)
	if err != nil {
		return err
	}
	if container == nil {
		return nil // No data for this date+type
	}

	return container.ReadRange(stime, etime, func(timeMs int64, data []byte) {
		handler(data)
	})
}

// PurgeOldDays closes day containers not in the keepDates set.
func (r *SummaryRD) PurgeOldDays(keepDates map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, sd := range r.days {
		if keepDates[key.date] {
			continue
		}
		if sd != nil {
			sd.Close()
		}
		delete(r.days, key)
	}
}

// Close closes all open day containers.
func (r *SummaryRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sd := range r.days {
		if sd != nil {
			sd.Close()
		}
	}
	r.days = make(map[dayKey]*SummaryData)
}
