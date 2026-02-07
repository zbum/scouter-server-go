package counter

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
)

const (
	// BucketsPerDay is the number of 5-minute intervals in a day (24*12).
	BucketsPerDay = 288
	// BytesPerBucket is 8 bytes (float64) per bucket.
	BytesPerBucket = 8
	// RecordSize is 2 (header: valueType + timeType) + 288*8 bytes per counter record.
	RecordSize = 2 + BucketsPerDay*BytesPerBucket
)

// DailyCounterData stores 5-minute aggregated counter values for a single day.
// Each counter key maps to a fixed-length record of 288 float64 buckets.
type DailyCounterData struct {
	mu    sync.Mutex
	dir   string
	index *io.IndexKeyFile
	data  *os.File
}

func NewDailyCounterData(dir string) (*DailyCounterData, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	index, err := io.NewIndexKeyFile(filepath.Join(dir, "5m"), 1)
	if err != nil {
		return nil, err
	}

	dataPath := filepath.Join(dir, "5m.data")
	data, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		index.Close()
		return nil, err
	}

	return &DailyCounterData{
		dir:   dir,
		index: index,
		data:  data,
	}, nil
}

// HHMMToBucket converts HHMM (e.g., 1430 for 14:30) to a 5-minute bucket index.
func HHMMToBucket(hhmm int) int {
	h := hhmm / 100
	m := hhmm % 100
	return h*12 + m/5
}

// BucketToHHMM converts a bucket index back to HHMM format.
func BucketToHHMM(bucket int) int {
	h := bucket / 12
	m := (bucket % 12) * 5
	return h*100 + m
}

// TimeSecToBucket converts seconds-since-midnight to bucket index.
func TimeSecToBucket(sec int) int {
	return sec / 300 // 300 seconds = 5 minutes
}

// makeCounterKey builds a composite key for the daily counter.
// Format: objHash(4) + counterNameHash(4)
func makeCounterKey(objHash int32, counterName string) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], uint32(objHash))
	h := hashStr(counterName)
	binary.BigEndian.PutUint32(key[4:8], uint32(h))
	return key
}

func hashStr(s string) int32 {
	h := int32(0)
	for _, c := range s {
		h = 31*h + int32(c)
	}
	return h
}

// Write stores a value at a specific 5-minute bucket for a counter key.
func (d *DailyCounterData) Write(objHash int32, counterName string, bucket int, val float64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if bucket < 0 || bucket >= BucketsPerDay {
		return nil
	}

	key := makeCounterKey(objHash, counterName)

	// Check if record already exists
	posBytes, err := d.index.Get(key)
	if err != nil {
		return err
	}

	var offset int64
	if posBytes != nil {
		// Record exists, update in place
		offset = protocol.ToLong5(posBytes, 0)
	} else {
		// Create new record: seek to end, write empty record
		fi, err := d.data.Stat()
		if err != nil {
			return err
		}
		offset = fi.Size()

		// Write header + empty buckets (NaN = no data)
		record := make([]byte, RecordSize)
		record[0] = 0 // valueType (float64)
		record[1] = 0 // timeType (daily)
		for i := 0; i < BucketsPerDay; i++ {
			binary.BigEndian.PutUint64(record[2+i*BytesPerBucket:], math.Float64bits(math.NaN()))
		}
		if _, err := d.data.WriteAt(record, offset); err != nil {
			return err
		}

		// Index the new record
		if err := d.index.Put(key, protocol.ToBytes5(offset)); err != nil {
			return err
		}
	}

	// Write the value at the bucket position
	bucketOffset := offset + 2 + int64(bucket)*BytesPerBucket
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(val))
	_, err = d.data.WriteAt(buf, bucketOffset)
	return err
}

// Read retrieves the value at a specific 5-minute bucket.
func (d *DailyCounterData) Read(objHash int32, counterName string, bucket int) (float64, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if bucket < 0 || bucket >= BucketsPerDay {
		return 0, false, nil
	}

	key := makeCounterKey(objHash, counterName)
	posBytes, err := d.index.Get(key)
	if err != nil {
		return 0, false, err
	}
	if posBytes == nil {
		return 0, false, nil
	}

	offset := protocol.ToLong5(posBytes, 0)
	bucketOffset := offset + 2 + int64(bucket)*BytesPerBucket

	buf := make([]byte, 8)
	if _, err := d.data.ReadAt(buf, bucketOffset); err != nil {
		return 0, false, err
	}

	val := math.Float64frombits(binary.BigEndian.Uint64(buf))
	if math.IsNaN(val) {
		return 0, false, nil
	}
	return val, true, nil
}

// ReadAll retrieves all 288 bucket values for a counter key.
func (d *DailyCounterData) ReadAll(objHash int32, counterName string) ([]float64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := makeCounterKey(objHash, counterName)
	posBytes, err := d.index.Get(key)
	if err != nil {
		return nil, err
	}
	if posBytes == nil {
		return nil, nil
	}

	offset := protocol.ToLong5(posBytes, 0)
	record := make([]byte, RecordSize)
	if _, err := d.data.ReadAt(record, offset); err != nil {
		return nil, err
	}

	values := make([]float64, BucketsPerDay)
	for i := 0; i < BucketsPerDay; i++ {
		values[i] = math.Float64frombits(binary.BigEndian.Uint64(record[2+i*BytesPerBucket:]))
	}
	return values, nil
}

func (d *DailyCounterData) Close() {
	d.data.Close()
	d.index.Close()
}
