package io

import (
	"errors"
	"sort"

	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/util"
)

// TimeToData pairs a timestamp with a data position reference.
type TimeToData struct {
	Time    int64
	DataPos []byte
}

// IndexTimeFile is a time-range index combining MemTimeBlock + RealKeyFile.
// It provides 500ms-resolution bucketed access plus chain-based collision storage.
type IndexTimeFile struct {
	path          string
	timeBlockHash *MemTimeBlock
	keyFile       *RealKeyFile
}

func NewIndexTimeFile(path string) (*IndexTimeFile, error) {
	tb, err := NewMemTimeBlock(path)
	if err != nil {
		return nil, err
	}
	kf, err := NewRealKeyFile(path)
	if err != nil {
		tb.Close()
		return nil, err
	}
	return &IndexTimeFile{
		path:          path,
		timeBlockHash: tb,
		keyFile:       kf,
	}, nil
}

func (f *IndexTimeFile) Put(timeMs int64, dataPos []byte) (int64, error) {
	if timeMs <= 0 || dataPos == nil {
		return 0, errors.New("invalid key/value")
	}
	prevKeyPos := f.timeBlockHash.Get(timeMs)
	newKeyPos, err := f.keyFile.Append(prevKeyPos, protocol.ToBytesLong(timeMs), dataPos)
	if err != nil {
		return 0, err
	}
	f.timeBlockHash.Put(timeMs, newKeyPos)
	f.timeBlockHash.AddCount(1)
	return newKeyPos, nil
}

func (f *IndexTimeFile) getSecAll(timeMs int64) ([]TimeToData, error) {
	if timeMs <= 0 {
		return nil, errors.New("invalid key")
	}
	var items []TimeToData
	pos := f.timeBlockHash.Get(timeMs)
	for pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return nil, err
		}
		if !r.Deleted {
			t := protocol.ToLong(r.TimeKey, 0)
			items = append(items, TimeToData{Time: t, DataPos: r.DataPos})
		}
		pos = r.PrevPos
	}
	// Sort by time ascending
	sort.Slice(items, func(i, j int) bool {
		return items[i].Time < items[j].Time
	})
	return items, nil
}

func (f *IndexTimeFile) GetDirect(pos int64) (*TimeToData, error) {
	r, err := f.keyFile.GetRecord(pos)
	if err != nil {
		return nil, err
	}
	if r.Deleted {
		return nil, nil
	}
	return &TimeToData{
		Time:    protocol.ToLong(r.TimeKey, 0),
		DataPos: r.DataPos,
	}, nil
}

func (f *IndexTimeFile) Delete(timeMs int64) (int, error) {
	if timeMs <= 0 {
		return 0, errors.New("invalid key")
	}
	pos := f.timeBlockHash.Get(timeMs)
	deleted := 0
	for pos > 0 {
		isDel, err := f.keyFile.IsDeleted(pos)
		if err != nil {
			return deleted, err
		}
		if !isDel {
			if err := f.keyFile.SetDelete(pos, true); err != nil {
				return deleted, err
			}
			deleted++
		}
		pos, err = f.keyFile.GetPrevPos(pos)
		if err != nil {
			return deleted, err
		}
	}
	f.timeBlockHash.Put(timeMs, 0)
	f.timeBlockHash.AddCount(-deleted)
	return deleted, nil
}

// Read iterates forward through time buckets from stime to etime (500ms increments).
// Handler returns false to stop iteration early.
func (f *IndexTimeFile) Read(stime int64, etime int64, handler func(time int64, dataPos []byte) bool) error {
	t := stime
	for i := 0; i < util.SecondsPerDay*2 && t <= etime; i++ {
		if f.timeBlockHash.Get(t) == 0 {
			t += 500
			continue
		}
		items, err := f.getSecAll(t)
		if err != nil {
			return err
		}
		for _, item := range items {
			if !handler(item.Time, item.DataPos) {
				return nil
			}
		}
		t += 500
	}
	return nil
}

// ReadFromEnd iterates backward through time buckets from etime to stime.
// Handler returns false to stop iteration early.
func (f *IndexTimeFile) ReadFromEnd(stime int64, etime int64, handler func(time int64, dataPos []byte) bool) error {
	t := etime
	for i := 0; i < util.SecondsPerDay*2 && stime <= t; i++ {
		if f.timeBlockHash.Get(t) == 0 {
			t -= 500
			continue
		}
		items, err := f.getSecAll(t)
		if err != nil {
			return err
		}
		for j := len(items) - 1; j >= 0; j-- {
			if !handler(items[j].Time, items[j].DataPos) {
				return nil
			}
		}
		t -= 500
	}
	return nil
}

// ReadWithDataReader iterates forward, resolving data positions to actual data via reader.
// Handler returns false to stop iteration early.
func (f *IndexTimeFile) ReadWithDataReader(stime int64, etime int64,
	handler func(time int64, data []byte) bool, reader func(int64) []byte) error {
	t := stime
	for i := 0; i < util.SecondsPerDay*2 && t <= etime; i++ {
		if f.timeBlockHash.Get(t) == 0 {
			t += 500
			continue
		}
		items, err := f.getSecAll(t)
		if err != nil {
			return err
		}
		for _, item := range items {
			if item.Time >= stime && item.Time <= etime {
				dataPos := protocol.ToLong5(item.DataPos, 0)
				if !handler(item.Time, reader(dataPos)) {
					return nil
				}
			}
		}
		t += 500
	}
	return nil
}

// ReadFromEndWithDataReader iterates backward, resolving data positions via reader.
// Handler returns false to stop iteration early.
func (f *IndexTimeFile) ReadFromEndWithDataReader(stime int64, etime int64,
	handler func(time int64, data []byte) bool, reader func(int64) []byte) error {
	t := etime
	for i := 0; i < util.SecondsPerDay*2 && stime <= t; i++ {
		if f.timeBlockHash.Get(t) == 0 {
			t -= 500
			continue
		}
		items, err := f.getSecAll(t)
		if err != nil {
			return err
		}
		for j := len(items) - 1; j >= 0; j-- {
			item := items[j]
			if item.Time >= stime && item.Time <= etime {
				dataPos := protocol.ToLong5(item.DataPos, 0)
				if !handler(item.Time, reader(dataPos)) {
					return nil
				}
			}
		}
		t -= 500
	}
	return nil
}

// ReadAll iterates over all records sequentially in the key file.
func (f *IndexTimeFile) ReadAll(handler func(key []byte, dataPos []byte)) error {
	pos := f.keyFile.GetFirstPos()
	length := f.keyFile.GetLength()
	for pos < length && pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return err
		}
		if !r.Deleted {
			handler(r.TimeKey, r.DataPos)
		}
		pos = r.Offset
	}
	return nil
}

// GetStartEndDataPos returns the first and last data positions in the time range.
func (f *IndexTimeFile) GetStartEndDataPos(stime int64, etime int64) ([]byte, []byte, error) {
	first, err := f.getDataPosFirst(stime, etime)
	if err != nil {
		return nil, nil, err
	}
	last, err := f.getDataPosLast(stime, etime)
	if err != nil {
		return nil, nil, err
	}
	return first, last, nil
}

func (f *IndexTimeFile) getDataPosFirst(stime int64, etime int64) ([]byte, error) {
	t := stime
	for i := 0; i < util.SecondsPerDay*2 && t <= etime; i++ {
		dp, err := f.getDataPosFirstAt(t)
		if err != nil {
			return nil, err
		}
		if dp != nil {
			return dp, nil
		}
		t += 500
	}
	return nil, nil
}

func (f *IndexTimeFile) getDataPosFirstAt(timeMs int64) ([]byte, error) {
	pos := f.timeBlockHash.Get(timeMs)
	for pos > 0 {
		prevPos, err := f.keyFile.GetPrevPos(pos)
		if err != nil {
			return nil, err
		}
		if prevPos == 0 {
			return f.keyFile.GetDataPos(pos)
		}
		pos = prevPos
	}
	return nil, nil
}

func (f *IndexTimeFile) getDataPosLast(stime int64, etime int64) ([]byte, error) {
	t := etime
	for i := 0; i < util.SecondsPerDay*2 && stime <= t; i++ {
		dp, err := f.getDataPosLastAt(t)
		if err != nil {
			return nil, err
		}
		if dp != nil {
			return dp, nil
		}
		t -= 500
	}
	return nil, nil
}

func (f *IndexTimeFile) getDataPosLastAt(timeMs int64) ([]byte, error) {
	pos := f.timeBlockHash.Get(timeMs)
	if pos == 0 {
		return nil, nil
	}
	return f.keyFile.GetDataPos(pos)
}

func (f *IndexTimeFile) Close() {
	f.timeBlockHash.Close()
	f.keyFile.Close()
}
