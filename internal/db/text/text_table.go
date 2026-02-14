package text

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/util"
)

// TextTable provides text storage using a single IndexKeyFile.
// Key format: 8 bytes = [4B hash(div)][4B textHash] (big-endian int32 each)
// This matches Java Scouter's TextTable binary format for compatibility.
// Value: UTF-8 text bytes
type TextTable struct {
	mu    sync.Mutex
	path  string
	index *io.IndexKeyFile
}

// NewTextTable opens or creates a text table at the specified directory.
func NewTextTable(dir string) (*TextTable, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &TextTable{
		path: dir,
	}, nil
}

// getIndex returns the single IndexKeyFile, creating it if necessary.
func (t *TextTable) getIndex() (*io.IndexKeyFile, error) {
	if t.index != nil {
		return t.index, nil
	}

	hashSizeMB := 1
	if cfg := config.Get(); cfg != nil {
		hashSizeMB = cfg.MgrTextDbDailyIndexMB()
	}
	indexPath := filepath.Join(t.path, "text")
	idx, err := io.NewIndexKeyFile(indexPath, hashSizeMB)
	if err != nil {
		return nil, err
	}

	t.index = idx
	return idx, nil
}

// Set stores a text string with the given div and hash.
// Checks HasKey first to avoid duplicate entries (matching Java behavior).
func (t *TextTable) Set(div string, hash int32, text string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, err := t.getIndex()
	if err != nil {
		return err
	}

	key := makeHashKey(div, hash)
	exists, err := idx.HasKey(key)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return idx.Put(key, []byte(text))
}

// Get retrieves a text string by div and hash.
// Returns (text, found, error).
func (t *TextTable) Get(div string, hash int32) (string, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, err := t.getIndex()
	if err != nil {
		return "", false, err
	}

	key := makeHashKey(div, hash)
	value, err := idx.Get(key)
	if err != nil {
		return "", false, err
	}
	if value == nil {
		return "", false, nil
	}
	return string(value), true, nil
}

// Close closes the underlying IndexKeyFile.
func (t *TextTable) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.index != nil {
		t.index.Close()
		t.index = nil
	}
}

// makeHashKey builds an 8-byte key: [4B hash(div)][4B textHash].
// This matches Java's: new DataOutputX().writeInt(HashUtil.hash(div)).writeInt(key).toByteArray()
func makeHashKey(div string, hash int32) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[:4], uint32(util.HashString(div)))
	binary.BigEndian.PutUint32(key[4:], uint32(hash))
	return key
}
