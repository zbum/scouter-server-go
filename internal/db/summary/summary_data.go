package summary

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// SummaryData stores summary entries for a single day and summary type.
// Time-indexed using IndexTimeFile, data in RealDataFile.
type SummaryData struct {
	mu    sync.Mutex
	dir   string
	stype byte
	index *io.IndexTimeFile
	data  *io.RealDataFile
}

// NewSummaryData creates a SummaryData for the given directory and summary type.
// Index file: "summary_{stype}", data file: "summary_{stype}.data".
func NewSummaryData(dir string, stype byte) (*SummaryData, error) {
	indexPath := filepath.Join(dir, formatFileName(stype))
	dataPath := filepath.Join(dir, formatFileName(stype)+".data")

	idx, err := io.NewIndexTimeFile(indexPath)
	if err != nil {
		return nil, err
	}

	df, err := io.NewRealDataFile(dataPath)
	if err != nil {
		idx.Close()
		return nil, err
	}

	return &SummaryData{
		dir:   dir,
		stype: stype,
		index: idx,
		data:  df,
	}, nil
}

// formatFileName generates file name based on summary type.
func formatFileName(stype byte) string {
	names := map[byte]string{
		1: "summary_app",
		2: "summary_sql",
		3: "summary_apicall",
		4: "summary_ip",
		5: "summary_ua",
		6: "summary_error",
		7: "summary_alert",
	}
	if name, ok := names[stype]; ok {
		return name
	}
	return "summary_unknown"
}

// Write stores summary bytes into the data file and indexes by time.
func (sd *SummaryData) Write(timeMs int64, summaryBytes []byte) error {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	// Write length header (2 bytes) + data to the data file
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(summaryBytes)))
	offset, err := sd.data.Write(lenBuf[:])
	if err != nil {
		return err
	}
	if _, err := sd.data.Write(summaryBytes); err != nil {
		return err
	}

	// Index by time: store 5-byte encoded offset
	_, err = sd.index.Put(timeMs, protocol.BigEndian.Bytes5(offset))
	return err
}

// ReadRange reads summaries in the given time range and calls handler for each.
func (sd *SummaryData) ReadRange(stime, etime int64, handler func(timeMs int64, data []byte)) error {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	dataPath := filepath.Join(sd.dir, formatFileName(sd.stype)+".data")

	return sd.index.Read(stime, etime, func(timeMs int64, dataPos []byte) bool {
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
func (sd *SummaryData) Flush() error {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	return sd.data.Flush()
}

// Close closes both index and data files.
func (sd *SummaryData) Close() {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	if sd.index != nil {
		sd.index.Close()
	}
	if sd.data != nil {
		sd.data.Close()
	}
}
