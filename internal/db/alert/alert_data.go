package alert

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// AlertData stores alert entries for a single day.
// Time-indexed using IndexTimeFile, data in RealDataFile.
type AlertData struct {
	mu    sync.Mutex
	dir   string
	index *io.IndexTimeFile
	data  *io.RealDataFile
}

// NewAlertData creates an AlertData for the given directory.
// Index file: "alert", data file: "alert.data".
func NewAlertData(dir string) (*AlertData, error) {
	indexPath := filepath.Join(dir, "alert")
	dataPath := filepath.Join(dir, "alert.data")

	idx, err := io.NewIndexTimeFile(indexPath)
	if err != nil {
		return nil, err
	}

	df, err := io.NewRealDataFile(dataPath)
	if err != nil {
		idx.Close()
		return nil, err
	}

	return &AlertData{
		dir:   dir,
		index: idx,
		data:  df,
	}, nil
}

// Write stores alert bytes into the data file and indexes by time.
func (ad *AlertData) Write(timeMs int64, alertBytes []byte) error {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	// Write length header (2 bytes) + data to the data file
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(alertBytes)))
	offset, err := ad.data.Write(lenBuf[:])
	if err != nil {
		return err
	}
	if _, err := ad.data.Write(alertBytes); err != nil {
		return err
	}

	// Index by time: store 5-byte encoded offset
	_, err = ad.index.Put(timeMs, protocol.BigEndian.Bytes5(offset))
	return err
}

// ReadRange reads alerts in the given time range and calls handler for each.
func (ad *AlertData) ReadRange(stime, etime int64, handler func(timeMs int64, data []byte)) error {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	dataPath := filepath.Join(ad.dir, "alert.data")

	return ad.index.Read(stime, etime, func(timeMs int64, dataPos []byte) bool {
		offset := protocol.BigEndian.Int5(dataPos)
		raw, err := readEntryAt(dataPath, offset)
		if err == nil && raw != nil {
			handler(timeMs, raw)
		}
		return true
	})
}

// readEntryAt reads a [2-byte length][data] entry from the file at the given offset.
func readEntryAt(path string, offset int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(offset, 0); err != nil {
		return nil, err
	}

	var lenBuf [2]byte
	if _, err := f.Read(lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint16(lenBuf[:])

	data := make([]byte, length)
	if _, err := f.Read(data); err != nil {
		return nil, err
	}

	return data, nil
}

// Flush flushes buffered data to disk.
func (ad *AlertData) Flush() error {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	return ad.data.Flush()
}

// Close closes both index and data files.
func (ad *AlertData) Close() {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	if ad.index != nil {
		ad.index.Close()
	}
	if ad.data != nil {
		ad.data.Close()
	}
}
