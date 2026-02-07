package counter

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/protocol/value"
)

// CounterRD reads both realtime and daily counter data.
type CounterRD struct {
	mu           sync.Mutex
	baseDir      string
	realtimeDays map[string]*RealtimeCounterData
	dailyDays    map[string]*DailyCounterData
}

func NewCounterRD(baseDir string) *CounterRD {
	return &CounterRD{
		baseDir:      baseDir,
		realtimeDays: make(map[string]*RealtimeCounterData),
		dailyDays:    make(map[string]*DailyCounterData),
	}
}

// ReadRealtime retrieves counter values for an object at a specific second.
func (r *CounterRD) ReadRealtime(date string, objHash int32, timeSec int32) (map[string]value.Value, error) {
	data, err := r.getRealtimeData(date)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return data.Read(objHash, timeSec)
}

// ReadRealtimeRange reads all realtime entries for an object in a time range.
func (r *CounterRD) ReadRealtimeRange(date string, objHash int32, startSec, endSec int32,
	handler func(timeSec int32, counters map[string]value.Value)) error {
	data, err := r.getRealtimeData(date)
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	return data.ReadRange(objHash, startSec, endSec, handler)
}

// ReadDaily retrieves the value at a specific 5-minute bucket.
func (r *CounterRD) ReadDaily(date string, objHash int32, counterName string, bucket int) (float64, bool, error) {
	data, err := r.getDailyData(date)
	if err != nil {
		return 0, false, err
	}
	if data == nil {
		return 0, false, nil
	}
	return data.Read(objHash, counterName, bucket)
}

// ReadDailyAll retrieves all 288 bucket values for a counter key.
func (r *CounterRD) ReadDailyAll(date string, objHash int32, counterName string) ([]float64, error) {
	data, err := r.getDailyData(date)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return data.ReadAll(objHash, counterName)
}

func (r *CounterRD) getRealtimeData(date string) (*RealtimeCounterData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if d, ok := r.realtimeDays[date]; ok {
		return d, nil
	}

	dir := filepath.Join(r.baseDir, date, "counter")
	if _, err := os.Stat(filepath.Join(dir, "real.data")); os.IsNotExist(err) {
		return nil, nil
	}

	d, err := NewRealtimeCounterData(dir)
	if err != nil {
		return nil, err
	}
	r.realtimeDays[date] = d
	return d, nil
}

func (r *CounterRD) getDailyData(date string) (*DailyCounterData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if d, ok := r.dailyDays[date]; ok {
		return d, nil
	}

	dir := filepath.Join(r.baseDir, date, "counter")
	if _, err := os.Stat(filepath.Join(dir, "5m.data")); os.IsNotExist(err) {
		return nil, nil
	}

	d, err := NewDailyCounterData(dir)
	if err != nil {
		return nil, err
	}
	r.dailyDays[date] = d
	return d, nil
}

// PurgeOldDays closes day containers not in the keepDates set.
func (r *CounterRD) PurgeOldDays(keepDates map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for date, d := range r.realtimeDays {
		if keepDates[date] {
			continue
		}
		d.Close()
		delete(r.realtimeDays, date)
	}
	for date, d := range r.dailyDays {
		if keepDates[date] {
			continue
		}
		d.Close()
		delete(r.dailyDays, date)
	}
}

// Close closes all open data files.
func (r *CounterRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range r.realtimeDays {
		d.Close()
	}
	for _, d := range r.dailyDays {
		d.Close()
	}
}
