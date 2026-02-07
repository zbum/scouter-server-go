package text

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/util"
)

// TextTable provides per-day text storage using IndexKeyFile.
// Key format: 8 bytes = hash(div):4 + hash(text):4
// Value: UTF-8 text bytes
type TextTable struct {
	path  string
	index *io.IndexKeyFile
}

// NewTextTable opens or creates a text table at the specified directory.
// The IndexKeyFile is created at dir/text with 1MB hash size.
func NewTextTable(dir string) (*TextTable, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	indexPath := filepath.Join(dir, "text")
	index, err := io.NewIndexKeyFile(indexPath, 1)
	if err != nil {
		return nil, err
	}

	return &TextTable{
		path:  dir,
		index: index,
	}, nil
}

// Set stores a text string with the given div and hash.
// The composite key is built from hash(div) + hash.
func (t *TextTable) Set(div string, hash int32, text string) error {
	key := makeCompositeKey(div, hash)
	value := []byte(text)
	return t.index.Put(key, value)
}

// Get retrieves a text string by div and hash.
// Returns (text, found, error).
func (t *TextTable) Get(div string, hash int32) (string, bool, error) {
	key := makeCompositeKey(div, hash)
	value, err := t.index.Get(key)
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
	if t.index != nil {
		t.index.Close()
	}
}

// makeCompositeKey builds an 8-byte key from hash(div) + hash.
func makeCompositeKey(div string, hash int32) []byte {
	key := make([]byte, 8)
	divHash := util.HashString(div)
	binary.BigEndian.PutUint32(key[0:4], uint32(divHash))
	binary.BigEndian.PutUint32(key[4:8], uint32(hash))
	return key
}
