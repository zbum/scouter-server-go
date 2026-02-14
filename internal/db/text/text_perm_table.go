package text

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"

	"github.com/zbum/scouter-server-go/internal/config"
	"github.com/zbum/scouter-server-go/internal/db/io"
	"github.com/zbum/scouter-server-go/internal/protocol"
)

// TextPermTable provides permanent text storage using per-div IndexKeyFile + data files.
// This matches Java's TextPermWR/TextPermIndex/TextPermData architecture.
//
// Files per div: text_{div}.kfile, text_{div}.hfile, text_{div}.data
// IndexKeyFile key: 4 bytes = hash (big-endian int32)
// IndexKeyFile value: 5 bytes = data position (LONG5)
// Data file record: [4B length][length bytes of text]
type TextPermTable struct {
	mu        sync.Mutex
	path      string
	indexes   map[string]*io.IndexKeyFile
	dataFiles map[string]*TextPermData
}

// NewTextPermTable opens or creates a permanent text table at the specified directory.
func NewTextPermTable(dir string) (*TextPermTable, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &TextPermTable{
		path:      dir,
		indexes:   make(map[string]*io.IndexKeyFile),
		dataFiles: make(map[string]*TextPermData),
	}, nil
}

// getFiles returns the IndexKeyFile and TextPermData for the given div, creating them if necessary.
func (t *TextPermTable) getFiles(div string) (*io.IndexKeyFile, *TextPermData, error) {
	idx, hasIdx := t.indexes[div]
	data, hasData := t.dataFiles[div]
	if hasIdx && hasData {
		return idx, data, nil
	}

	filePath := filepath.Join(t.path, "text_"+div)

	if !hasIdx {
		hashSizeMB := 1
		if cfg := config.Get(); cfg != nil {
			hashSizeMB = cfg.MgrTextDbIndexMB(div)
		}
		var err error
		idx, err = io.NewIndexKeyFile(filePath, hashSizeMB)
		if err != nil {
			return nil, nil, err
		}
		t.indexes[div] = idx
	}

	if !hasData {
		var err error
		data, err = NewTextPermData(filePath)
		if err != nil {
			if !hasIdx {
				idx.Close()
				delete(t.indexes, div)
			}
			return nil, nil, err
		}
		t.dataFiles[div] = data
	}

	return idx, data, nil
}

// Set stores a text string with the given div and hash.
// Checks HasKey first to avoid duplicate entries (matching Java behavior).
func (t *TextPermTable) Set(div string, hash int32, text string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, data, err := t.getFiles(div)
	if err != nil {
		return err
	}

	key := makePermHashKey(hash)
	exists, err := idx.HasKey(key)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Write text to data file
	dataPos, err := data.Write([]byte(text))
	if err != nil {
		return err
	}

	// Store data position in index
	return idx.Put(key, protocol.BigEndian.Bytes5(dataPos))
}

// Get retrieves a text string by div and hash.
// Returns (text, found, error).
func (t *TextPermTable) Get(div string, hash int32) (string, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, data, err := t.getFiles(div)
	if err != nil {
		return "", false, err
	}

	key := makePermHashKey(hash)
	posBytes, err := idx.Get(key)
	if err != nil {
		return "", false, err
	}
	if posBytes == nil {
		return "", false, nil
	}

	pos := protocol.BigEndian.Int5(posBytes)
	if pos < 0 {
		return "", false, nil
	}

	textBytes, err := data.Read(pos)
	if err != nil {
		return "", false, err
	}
	return string(textBytes), true, nil
}

// HasKey checks if an entry exists for the given div and hash.
func (t *TextPermTable) HasKey(div string, hash int32) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, _, err := t.getFiles(div)
	if err != nil {
		return false, err
	}

	key := makePermHashKey(hash)
	return idx.HasKey(key)
}

// Close closes all underlying files.
func (t *TextPermTable) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, idx := range t.indexes {
		idx.Close()
	}
	for _, data := range t.dataFiles {
		data.Close()
	}
	t.indexes = make(map[string]*io.IndexKeyFile)
	t.dataFiles = make(map[string]*TextPermData)
}

// makePermHashKey builds a 4-byte big-endian key from hash.
// This matches Java's DataOutputX.toBytes(key).
func makePermHashKey(hash int32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(hash))
	return key
}
