package io

import (
	"bytes"
	"errors"
	"log/slog"
	"time"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/protocol"
	"github.com/zbum/scouter-server-go/internal/util"
)

// IndexKeyFile2 is a composite hash-based key-value index with TTL support,
// combining MemHashBlock + RealKeyFile2.
type IndexKeyFile2 struct {
	path      string
	hashBlock *MemHashBlock
	keyFile   *RealKeyFile2
}

func NewIndexKeyFile2(path string, hashSizeMB int) (*IndexKeyFile2, error) {
	if hashSizeMB <= 0 {
		hashSizeMB = defaultHashSizeMB
	}
	hb, err := NewMemHashBlock(path, hashSizeMB*MB)
	if err != nil {
		return nil, err
	}
	kf, err := NewRealKeyFile2(path)
	if err != nil {
		hb.Close()
		return nil, err
	}
	return &IndexKeyFile2{
		path:      path,
		hashBlock: hb,
		keyFile:   kf,
	}, nil
}

// Put inserts a key-value pair with infinite TTL.
func (f *IndexKeyFile2) Put(indexKey []byte, dataOffset []byte) error {
	return f.PutTTL(indexKey, dataOffset, -1)
}

// PutTTL inserts a key-value pair with TTL.
func (f *IndexKeyFile2) PutTTL(indexKey []byte, dataOffset []byte, ttl int64) error {
	if indexKey == nil || dataOffset == nil {
		return errors.New("invalid key/value")
	}
	keyHash := util.HashBytes(indexKey)
	prevKeyPos := f.hashBlock.Get(keyHash)
	newKeyPos, err := f.keyFile.AppendTTL(prevKeyPos, ttl, indexKey, dataOffset)
	if err != nil {
		return err
	}
	f.hashBlock.Put(keyHash, newKeyPos)
	return nil
}

// UpdateOrPut updates an existing key's value or inserts a new entry with infinite TTL.
func (f *IndexKeyFile2) UpdateOrPut(key []byte, value []byte) (bool, error) {
	return f.UpdateOrPutTTL(key, value, -1)
}

// UpdateOrPutTTL updates an existing key's value or inserts a new entry with TTL.
func (f *IndexKeyFile2) UpdateOrPutTTL(key []byte, value []byte, ttl int64) (bool, error) {
	if key == nil || value == nil {
		return false, errors.New("invalid key/value")
	}
	keyHash := util.HashBytes(key)
	realKeyPos := f.hashBlock.Get(keyHash)

	looping := 0
	for realKeyPos > 0 {
		oKey, err := f.keyFile.GetKey(realKeyPos)
		if err != nil {
			return false, err
		}
		if bytes.Equal(oKey, key) {
			ok, err := f.keyFile.Update(realKeyPos, ttl, key, value)
			if err != nil {
				return false, err
			}
			if !ok {
				return true, f.PutTTL(key, value, ttl)
			}
			return true, nil
		}
		realKeyPos, err = f.keyFile.GetPrevPos(realKeyPos)
		if err != nil {
			return false, err
		}
		looping++
	}
	warnCount := 100
	if cfg := config.Get(); cfg != nil {
		warnCount = cfg.LogIndexTraversalWarningCount()
	}
	if looping > warnCount {
		slog.Warn("Too many index deep searching", "looping", looping)
	}

	return true, f.PutTTL(key, value, ttl)
}

// SetTTL modifies the TTL of an existing key.
func (f *IndexKeyFile2) SetTTL(key []byte, ttl int64) (bool, error) {
	if key == nil {
		return false, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	realKeyPos := f.hashBlock.Get(keyHash)

	looping := 0
	for realKeyPos > 0 {
		oKey, err := f.keyFile.GetKey(realKeyPos)
		if err != nil {
			return false, err
		}
		if bytes.Equal(oKey, key) {
			delOrExp, err := f.keyFile.IsDeletedOrExpired(realKeyPos)
			if err != nil {
				return false, err
			}
			if delOrExp {
				return false, nil
			}
			return true, f.keyFile.SetTTL(realKeyPos, ttl)
		}
		realKeyPos, err = f.keyFile.GetPrevPos(realKeyPos)
		if err != nil {
			return false, err
		}
		looping++
	}
	warnCount := 100
	if cfg := config.Get(); cfg != nil {
		warnCount = cfg.LogIndexTraversalWarningCount()
	}
	if looping > warnCount {
		slog.Warn("Too many index deep searching", "looping", looping)
	}
	return false, nil
}

// Get retrieves the data position for a key, skipping deleted or expired entries.
func (f *IndexKeyFile2) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	realKeyPos := f.hashBlock.Get(keyHash)

	looping := 0
	for realKeyPos > 0 {
		oKey, err := f.keyFile.GetKey(realKeyPos)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(oKey, key) {
			delOrExp, err := f.keyFile.IsDeletedOrExpired(realKeyPos)
			if err != nil {
				return nil, err
			}
			if delOrExp {
				return nil, nil
			}
			return f.keyFile.GetDataPos(realKeyPos)
		}
		realKeyPos, err = f.keyFile.GetPrevPos(realKeyPos)
		if err != nil {
			return nil, err
		}
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

// HasKey checks if a non-deleted, non-expired entry exists for the key.
func (f *IndexKeyFile2) HasKey(key []byte) (bool, error) {
	if key == nil {
		return false, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	for pos > 0 {
		oKey, err := f.keyFile.GetKey(pos)
		if err != nil {
			return false, err
		}
		if bytes.Equal(oKey, key) {
			delOrExp, err := f.keyFile.IsDeletedOrExpired(pos)
			if err != nil {
				return false, err
			}
			return !delOrExp, nil
		}
		pos, err = f.keyFile.GetPrevPos(pos)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// GetAll returns all non-deleted data positions for a key (does not filter expired).
func (f *IndexKeyFile2) GetAll(key []byte) ([][]byte, error) {
	if key == nil {
		return nil, errors.New("invalid key")
	}
	var out [][]byte
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	for pos > 0 {
		isDel, err := f.keyFile.IsDeleted(pos)
		if err != nil {
			return nil, err
		}
		if !isDel {
			oKey, err := f.keyFile.GetKey(pos)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(oKey, key) {
				dp, err := f.keyFile.GetDataPos(pos)
				if err != nil {
					return nil, err
				}
				out = append(out, dp)
			}
		}
		pos, err = f.keyFile.GetPrevPos(pos)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Delete marks all entries matching the key as deleted.
func (f *IndexKeyFile2) Delete(key []byte) (int, error) {
	if key == nil {
		return 0, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	pos := f.hashBlock.Get(keyHash)
	for pos > 0 {
		oKey, err := f.keyFile.GetKey(pos)
		if err != nil {
			return 0, err
		}
		if bytes.Equal(oKey, key) {
			isDel, err := f.keyFile.IsDeleted(pos)
			if err != nil {
				return 0, err
			}
			if isDel {
				return 0, nil
			}
			if err := f.keyFile.SetDelete(pos, true); err != nil {
				return 0, err
			}
			return 1, nil
		}
		pos, err = f.keyFile.GetPrevPos(pos)
		if err != nil {
			return 0, err
		}
	}
	return 0, nil
}

// Read iterates over all non-deleted records in the key file.
func (f *IndexKeyFile2) Read(handler func(key []byte, data []byte)) error {
	pos := f.keyFile.FirstPos()
	length := f.keyFile.Length()
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

// GetAndRefreshTTL retrieves data for a key and refreshes its TTL in a single chain traversal.
// More efficient than calling Get() followed by SetTTL() separately, avoiding double traversal.
func (f *IndexKeyFile2) GetAndRefreshTTL(key []byte, ttl int64) ([]byte, error) {
	if key == nil {
		return nil, errors.New("invalid key")
	}
	keyHash := util.HashBytes(key)
	realKeyPos := f.hashBlock.Get(keyHash)

	looping := 0
	for realKeyPos > 0 {
		oKey, err := f.keyFile.GetKey(realKeyPos)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(oKey, key) {
			delOrExp, err := f.keyFile.IsDeletedOrExpired(realKeyPos)
			if err != nil {
				return nil, err
			}
			if delOrExp {
				return nil, nil
			}
			dataPos, err := f.keyFile.GetDataPos(realKeyPos)
			if err != nil {
				return nil, err
			}
			// Refresh TTL using the already-found position — no second traversal
			f.keyFile.SetTTL(realKeyPos, ttl)
			return dataPos, nil
		}
		realKeyPos, err = f.keyFile.GetPrevPos(realKeyPos)
		if err != nil {
			return nil, err
		}
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

// ReadWithDataReader iterates and resolves data positions to actual data.
func (f *IndexKeyFile2) ReadWithDataReader(handler func(key []byte, data []byte), reader func(int64) []byte) error {
	pos := f.keyFile.FirstPos()
	length := f.keyFile.Length()
	for pos < length && pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return err
		}
		if !r.Deleted {
			dataPos := protocol.BigEndian.Int5(r.DataPos)
			handler(r.TimeKey, reader(dataPos))
		}
		pos = r.Offset
	}
	return nil
}

// ReadValidWithDataReader iterates non-deleted, non-expired records and resolves data.
// Used by compaction to copy only live entries.
func (f *IndexKeyFile2) ReadValidWithDataReader(handler func(key []byte, data []byte), reader func(int64) []byte) error {
	now := time.Now().Unix()
	pos := f.keyFile.FirstPos()
	length := f.keyFile.Length()
	for pos < length && pos > 0 {
		r, err := f.keyFile.GetRecord(pos)
		if err != nil {
			return err
		}
		if !r.Deleted && r.Expire > now {
			dataPos := protocol.BigEndian.Int5(r.DataPos)
			handler(r.TimeKey, reader(dataPos))
		}
		pos = r.Offset
	}
	return nil
}

func (f *IndexKeyFile2) Stat() map[string]interface{} {
	deleted := 0
	count := 0
	pos := f.keyFile.FirstPos()
	length := f.keyFile.Length()
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
	scatter := f.hashBlock.Count()

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

func (f *IndexKeyFile2) Close() {
	f.hashBlock.Close()
	f.keyFile.Close()
}
