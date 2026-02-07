package profile

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// ProfileData stores XLog profile (step trace) data for a single day.
// Indexed by txid via IndexKeyFile. Each txid can have multiple profile blocks
// (appended incrementally as steps complete).
type ProfileData struct {
	mu    sync.Mutex
	dir   string
	index *io.IndexKeyFile // txid → data offset(s)
	data  *io.RealDataFile // profile block storage
}

func NewProfileData(dir string) (*ProfileData, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	index, err := io.NewIndexKeyFile(filepath.Join(dir, "xlog_prof"), 1)
	if err != nil {
		return nil, err
	}

	data, err := io.NewRealDataFile(filepath.Join(dir, "xlog_prof.data"))
	if err != nil {
		index.Close()
		return nil, err
	}

	return &ProfileData{
		dir:   dir,
		index: index,
		data:  data,
	}, nil
}

// Write stores a profile block for a txid. Multiple blocks can be written
// for the same txid (they accumulate).
func (p *ProfileData) Write(txid int64, block []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Write data: [int32:length][bytes:block]
	out := protocol.NewDataOutputX()
	out.WriteInt32(int32(len(block)))
	out.Write(block)

	offset, err := p.data.Write(out.ToByteArray())
	if err != nil {
		return err
	}

	key := protocol.ToBytesLong(txid)
	return p.index.Put(key, protocol.ToBytes5(offset))
}

// Read retrieves all profile blocks for a txid.
// Returns blocks in order they were written, up to maxBlocks (-1 for unlimited).
func (p *ProfileData) Read(txid int64, maxBlocks int) ([][]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := protocol.ToBytesLong(txid)
	offsets, err := p.index.GetAll(key)
	if err != nil {
		return nil, err
	}
	if len(offsets) == 0 {
		return nil, nil
	}

	f, err := os.Open(p.data.Filename())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var blocks [][]byte
	for _, posBytes := range offsets {
		if maxBlocks > 0 && len(blocks) >= maxBlocks {
			break
		}

		offset := protocol.ToLong5(posBytes, 0)
		if _, err := f.Seek(offset, 0); err != nil {
			continue
		}

		// Read length
		lenBuf := make([]byte, 4)
		if _, err := f.Read(lenBuf); err != nil {
			continue
		}
		length := int(binary.BigEndian.Uint32(lenBuf))

		// Read data
		block := make([]byte, length)
		if _, err := f.Read(block); err != nil {
			continue
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

func (p *ProfileData) Flush() error {
	return p.data.Flush()
}

func (p *ProfileData) Close() {
	p.data.Close()
	p.index.Close()
}
