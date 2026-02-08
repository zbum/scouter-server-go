package xlog

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/protocol"
)

// XLogRD is an XLog reader.
type XLogRD struct {
	mu      sync.Mutex
	baseDir string
	days    map[string]*dayContainer
}

// NewXLogRD creates a new XLog reader.
func NewXLogRD(baseDir string) *XLogRD {
	return &XLogRD{
		baseDir: baseDir,
		days:    make(map[string]*dayContainer),
	}
}

// getContainer retrieves or opens a day container for reading.
func (r *XLogRD) getContainer(date string) (*dayContainer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	container, exists := r.days[date]
	if exists {
		return container, nil
	}

	// Check if directory exists
	dir := filepath.Join(r.baseDir, date, "xlog")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, nil // No data for this date
	}

	// Open index and data files
	index, err := NewXLogIndex(dir)
	if err != nil {
		return nil, err
	}

	data, err := NewXLogData(dir)
	if err != nil {
		index.Close()
		return nil, err
	}

	container = &dayContainer{
		index: index,
		data:  data,
	}
	r.days[date] = container
	return container, nil
}

// ReadByTime reads XLog entries within a time range and calls the handler for each.
func (r *XLogRD) ReadByTime(date string, stime, etime int64, handler func(data []byte)) error {
	container, err := r.getContainer(date)
	if err != nil {
		return err
	}
	if container == nil {
		return nil // No data for this date
	}

	return container.index.timeIndex.Read(stime, etime, func(timeMs int64, dataPos []byte) {
		offset := protocol.ToLong5(dataPos, 0)
		data, err := container.data.Read(offset)
		if err == nil && data != nil {
			handler(data)
		}
	})
}

// GetByTxid retrieves a single XLog by transaction ID.
func (r *XLogRD) GetByTxid(date string, txid int64) ([]byte, error) {
	container, err := r.getContainer(date)
	if err != nil {
		return nil, err
	}
	if container == nil {
		return nil, nil // No data for this date
	}

	offset, err := container.index.GetByTxid(txid)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, nil // Not found
	}

	return container.data.Read(offset)
}

// ReadByGxid reads all XLog entries related to a global transaction ID.
func (r *XLogRD) ReadByGxid(date string, gxid int64, handler func(data []byte)) error {
	container, err := r.getContainer(date)
	if err != nil {
		return err
	}
	if container == nil {
		return nil // No data for this date
	}

	offsets, err := container.index.GetByGxid(gxid)
	if err != nil {
		return err
	}

	for _, offset := range offsets {
		data, err := container.data.Read(offset)
		if err == nil && data != nil {
			handler(data)
		}
	}

	return nil
}

// ReadFromEndTime reads XLog entries within a time range in reverse order.
func (r *XLogRD) ReadFromEndTime(date string, stime, etime int64, handler func(data []byte)) error {
	container, err := r.getContainer(date)
	if err != nil {
		return err
	}
	if container == nil {
		return nil
	}

	return container.index.timeIndex.ReadFromEnd(stime, etime, func(timeMs int64, dataPos []byte) {
		offset := protocol.ToLong5(dataPos, 0)
		data, err := container.data.Read(offset)
		if err == nil && data != nil {
			handler(data)
		}
	})
}

// PurgeOldDays closes day containers not in the keepDates set.
func (r *XLogRD) PurgeOldDays(keepDates map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for date, container := range r.days {
		if keepDates[date] {
			continue
		}
		if container.data != nil {
			container.data.Close()
		}
		if container.index != nil {
			container.index.Close()
		}
		delete(r.days, date)
	}
}

// Close closes all open day containers.
func (r *XLogRD) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, container := range r.days {
		if container.data != nil {
			container.data.Close()
		}
		if container.index != nil {
			container.index.Close()
		}
	}
	r.days = make(map[string]*dayContainer)
}
