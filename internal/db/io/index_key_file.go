package io

import (
	"bytes"
	"errors"
	"log/slog"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/util"
)

const (
	defaultHashSizeMB = 1
	MB                = 1024 * 1024
)

// IndexKeyFile is a composite hash-based key-value index combining MemHashBlock + RealKeyFile.
type IndexKeyFile struct {
	path      string
	hashBlock *MemHashBlock
	keyFile   *RealKeyFile
}

func NewIndexKeyFile(path string, hashSizeMB int) (*IndexKeyFile, error) {
	if hashSizeMB <= 0 {
		hashSizeMB = defaultHashSizeMB
	}
	hb, err := NewMemHashBlock(path, hashSizeMB*MB)
	if err != nil {
		return nil, err
	}
	kf, err := NewRealKeyFile(path)
	if err != nil {
		hb.Close()
		return nil, err
	}
	return &IndexKeyFile{
		path:      path,
		hashBlock: hb,
		keyFile:   kf,
	}, nil
}

func (f *IndexKeyFile) Put(indexKey []byte, dataOffset []byte) error {
	if indexKey == nil || dataOffset == nil {
		return errors.New("invalid key/value")
	}
	keyHash := util.HashBytes(indexKey)
	prevKeyPos := f.hashBlock.Get(keyHash)
	newKeyPos, err := f.keyFile.Append(prevKeyPos, indexKey, dataOffset)
	if err != nil {
		return err
	}
	f.hashBlock.Put(keyHash, newKeyPos)
	return nil
}

func (f *IndexKeyFile) Update(key []byte, value []byte) (bool, error) {
	if key == nil || value == nil {
		return false, errors.New("invalid key/value")
	}
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	return f.keyFile.Update(pos, key, value)
}

func (f *IndexKeyFile) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	realKeyPos := f.hashBlock.Get(keyHash)

	looping := 0
	for realKeyPos > 0 {
		r, err := f.keyFile.GetRecord(realKeyPos)
		if err != nil {
			return nil, err
		}
		if !r.Deleted && bytes.Equal(r.TimeKey, key) {
			return r.DataPos, nil
		}
		realKeyPos = r.PrevPos
		looping++
	}
	warnCount := 100
	if cfg := config.Get(); cfg != nil {
		warnCount = cfg.LogIndexTraversalWarningCount()
	}
	if looping > warnCount {
		slog.Warn("Too many index deep searching", "looping", looping)
	}
	return nil, nil
}

func (f *IndexKeyFile) HasKey(key []byte) (bool, error) {
	if key == nil {
		return false, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	for pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return false, err
		}
		if !r.Deleted && bytes.Equal(r.TimeKey, key) {
			return true, nil
		}
		pos = r.PrevPos
	}
	return false, nil
}

func (f *IndexKeyFile) GetAll(key []byte) ([][]byte, error) {
	if key == nil {
		return nil, errors.New("invalid key")
	}
	var out [][]byte
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	for pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return nil, err
		}
		if !r.Deleted && bytes.Equal(r.TimeKey, key) {
			out = append(out, r.DataPos)
		}
		pos = r.PrevPos
	}
	return out, nil
}

func (f *IndexKeyFile) Delete(key []byte) (int, error) {
	if key == nil {
		return 0, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	deleted := 0
	for pos > 0 {
		isDel, err := f.keyFile.IsDeleted(pos)
		if err != nil {
			return deleted, err
		}
		if !isDel {
			oKey, err := f.keyFile.GetTimeKey(pos)
			if err != nil {
				return deleted, err
			}
			if bytes.Equal(oKey, key) {
				if err := f.keyFile.SetDelete(pos, true); err != nil {
					return deleted, err
				}
				deleted++
			}
		}
		pos, err = f.keyFile.GetPrevPos(pos)
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

// Read iterates over all non-deleted records in the key file.
func (f *IndexKeyFile) Read(handler func(key []byte, data []byte)) error {
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

// ReadWithDataReader iterates and resolves data positions to actual data.
func (f *IndexKeyFile) ReadWithDataReader(handler func(key []byte, data []byte), reader func(int64) []byte) error {
	pos := f.keyFile.GetFirstPos()
	length := f.keyFile.GetLength()
	for pos < length && pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return err
		}
		if !r.Deleted {
			dataPos := protocol.ToLong5(r.DataPos, 0)
			handler(r.TimeKey, reader(dataPos))
		}
		pos = r.Offset
	}
	return nil
}

func (f *IndexKeyFile) GetStat() map[string]interface{} {
	deleted := 0
	count := 0
	pos := f.keyFile.GetFirstPos()
	length := f.keyFile.GetLength()
	for pos < length && pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			break
		}
		if r.Deleted {
			deleted++
		} else {
			count++
		}
		pos = r.Offset
	}
	scatter := f.hashBlock.GetCount()

	out := map[string]interface{}{
		"count":   count,
		"scatter": scatter,
		"deleted": deleted,
	}
	if scatter > 0 {
		out["scan"] = float64(count+deleted) / float64(scatter)
	}
	return out
}

func (f *IndexKeyFile) Close() {
	f.hashBlock.Close()
	f.keyFile.Close()
}
