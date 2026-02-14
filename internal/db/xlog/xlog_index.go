package xlog

import (
	"path/filepath"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// XLogIndex manages triple indexing: time, txid, and gxid.
type XLogIndex struct {
	timeIndex *io.IndexTimeFile // time → data offset
	txidIndex *io.IndexKeyFile  // txid → data offset
	gxidIndex *io.IndexKeyFile  // gxid → data offsets (multi)
}

// NewXLogIndex opens the triple index files for a given directory.
func NewXLogIndex(dir string) (*XLogIndex, error) {
	timeIdx, err := io.NewIndexTimeFile(filepath.Join(dir, "xlog_tim"))
	if err != nil {
		return nil, err
	}

	txidIdx, err := io.NewIndexKeyFile(filepath.Join(dir, "xlog_tid"), 1)
	if err != nil {
		timeIdx.Close()
		return nil, err
	}

	gxidIdx, err := io.NewIndexKeyFile(filepath.Join(dir, "xlog_gid"), 1)
	if err != nil {
		timeIdx.Close()
		txidIdx.Close()
		return nil, err
	}

	return &XLogIndex{
		timeIndex: timeIdx,
		txidIndex: txidIdx,
		gxidIndex: gxidIdx,
	}, nil
}

// SetByTime stores a time → data offset mapping.
func (x *XLogIndex) SetByTime(timeMs int64, dataPos int64) error {
	_, err := x.timeIndex.Put(timeMs, protocol.BigEndian.Bytes5(dataPos))
	return err
}

// SetByTxid stores a txid → data offset mapping.
func (x *XLogIndex) SetByTxid(txid int64, dataPos int64) error {
	return x.txidIndex.Put(protocol.BigEndian.Bytes8(txid), protocol.BigEndian.Bytes5(dataPos))
}

// SetByGxid stores a gxid → data offset mapping. Skips if gxid == 0.
func (x *XLogIndex) SetByGxid(gxid int64, dataPos int64) error {
	if gxid == 0 {
		return nil
	}
	return x.gxidIndex.Put(protocol.BigEndian.Bytes8(gxid), protocol.BigEndian.Bytes5(dataPos))
}

// GetByTxid retrieves the data offset for a given txid. Returns -1 if not found.
func (x *XLogIndex) GetByTxid(txid int64) (int64, error) {
	value, err := x.txidIndex.Get(protocol.BigEndian.Bytes8(txid))
	if err != nil {
		return -1, err
	}
	if value == nil {
		return -1, nil
	}
	return protocol.BigEndian.Int5(value), nil
}

// GetByGxid retrieves all data offsets for a given gxid.
func (x *XLogIndex) GetByGxid(gxid int64) ([]int64, error) {
	values, err := x.gxidIndex.GetAll(protocol.BigEndian.Bytes8(gxid))
	if err != nil {
		return nil, err
	}

	offsets := make([]int64, len(values))
	for i, v := range values {
		offsets[i] = protocol.BigEndian.Int5(v)
	}
	return offsets, nil
}

// Close closes all index files.
func (x *XLogIndex) Close() {
	if x.timeIndex != nil {
		x.timeIndex.Close()
	}
	if x.txidIndex != nil {
		x.txidIndex.Close()
	}
	if x.gxidIndex != nil {
		x.gxidIndex.Close()
	}
}
