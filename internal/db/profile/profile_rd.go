package profile

import (
	"os"
	"path/filepath"
	"sync"
)

// ProfileRD reads profile data.
type ProfileRD struct {
	mu      sync.Mutex
	baseDir string
	days    map[string]*ProfileData
}

func NewProfileRD(baseDir string) *ProfileRD {
	return &ProfileRD{
		baseDir: baseDir,
		days:    make(map[string]*ProfileData),
	}
}

// GetProfile retrieves all profile blocks for a txid on a given date.
// maxBlocks limits the number of blocks returned (-1 for unlimited).
func (r *ProfileRD) GetProfile(date string, txid int64, maxBlocks int) ([][]byte, error) {
	data, err := r.getData(date)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return data.Read(txid, maxBlocks)
}

func (r *ProfileRD) getData(date string) (*ProfileData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if d, ok := r.days[date]; ok {
		return d, nil
	}

	dir := filepath.Join(r.baseDir, date, "xlog")
	if _, err := os.Stat(filepath.Join(dir, "xlog_prof.data")); os.IsNotExist(err) {
		return nil, nil
	}

	d, err := NewProfileData(dir)
	if err != nil {
		return nil, err
	}
	r.days[date] = d
	return d, nil
}

// PurgeOldDays closes day containers not in the keepDates set.
func (r *ProfileRD) PurgeOldDays(keepDates map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for date, d := range r.days {
		if keepDates[date] {
			continue
		}
		d.Close()
		delete(r.days, date)
	}
}

// Close closes all open data files.
func (r *ProfileRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range r.days {
		d.Close()
	}
}
