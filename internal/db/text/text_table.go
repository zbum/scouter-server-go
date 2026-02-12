package text

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/db/io"
)

// TextTable provides text storage using per-div IndexKeyFiles.
// Each div (e.g. "service", "sql", "method") gets its own file: text_{div}.
// Key format: 4 bytes = hash (big-endian int32)
// Value: UTF-8 text bytes
type TextTable struct {
	mu      sync.Mutex
	path    string
	indexes map[string]*io.IndexKeyFile // div -> IndexKeyFile
}

// NewTextTable opens or creates a text table at the specified directory.
func NewTextTable(dir string) (*TextTable, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &TextTable{
		path:    dir,
		indexes: make(map[string]*io.IndexKeyFile),
	}, nil
}

// getIndex returns the IndexKeyFile for the given div, creating it if necessary.
func (t *TextTable) getIndex(div string) (*io.IndexKeyFile, error) {
	if idx, ok := t.indexes[div]; ok {
		return idx, nil
	}

	indexPath := filepath.Join(t.path, "text_"+div)
	idx, err := io.NewIndexKeyFile(indexPath, 1)
	if err != nil {
		return nil, err
	}

	t.indexes[div] = idx
	return idx, nil
}

// Set stores a text string with the given div and hash.
// Checks HasKey first to avoid duplicate entries (matching Java behavior).
func (t *TextTable) Set(div string, hash int32, text string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, err := t.getIndex(div)
	if err != nil {
		return err
	}

	key := makeHashKey(hash)
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

	idx, err := t.getIndex(div)
	if err != nil {
		return "", false, err
	}

	key := makeHashKey(hash)
	value, err := idx.Get(key)
	if err != nil {
		return "", false, err
	}
	if value == nil {
		return "", false, nil
	}
	return string(value), true, nil
}

// Close closes all underlying IndexKeyFiles.
func (t *TextTable) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, idx := range t.indexes {
		idx.Close()
	}
	t.indexes = make(map[string]*io.IndexKeyFile)
}

// makeHashKey builds a 4-byte big-endian key from hash.
func makeHashKey(hash int32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(hash))
	return key
}
