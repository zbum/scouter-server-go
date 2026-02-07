package alert

import (
	"os"
	"path/filepath"
	"sync"
)

// AlertRD is an alert reader.
type AlertRD struct {
	mu      sync.Mutex
	baseDir string
	days    map[string]*AlertData
}

// NewAlertRD creates a new alert reader.
func NewAlertRD(baseDir string) *AlertRD {
	return &AlertRD{
		baseDir: baseDir,
		days:    make(map[string]*AlertData),
	}
}

// getContainer retrieves or opens a day container for reading.
func (r *AlertRD) getContainer(date string) (*AlertData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	container, exists := r.days[date]
	if exists {
		return container, nil
	}

	// Check if directory exists
	dir := filepath.Join(r.baseDir, date, "alert")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, nil // No data for this date
	}

	ad, err := NewAlertData(dir)
	if err != nil {
		return nil, err
	}

	r.days[date] = ad
	return ad, nil
}

// ReadRange reads alerts in a time range for the given date.
func (r *AlertRD) ReadRange(date string, stime, etime int64, handler func(data []byte)) error {
	container, err := r.getContainer(date)
	if err != nil {
		return err
	}
	if container == nil {
		return nil // No data for this date
	}

	return container.ReadRange(stime, etime, func(timeMs int64, data []byte) {
		handler(data)
	})
}

// PurgeOldDays closes day containers not in the keepDates set.
func (r *AlertRD) PurgeOldDays(keepDates map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for date, ad := range r.days {
		if keepDates[date] {
			continue
		}
		if ad != nil {
			ad.Close()
		}
		delete(r.days, date)
	}
}

// Close closes all open day containers.
func (r *AlertRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, ad := range r.days {
		if ad != nil {
			ad.Close()
		}
	}
	r.days = make(map[string]*AlertData)
}
