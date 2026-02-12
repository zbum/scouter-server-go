package tagcnt

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
)

// Store handles disk persistence for tag counting data.
type Store struct {
	baseDir string
}

// NewStore creates a new tag count store.
func NewStore(baseDir string) *Store {
	return &Store{baseDir: baseDir}
}

// tagCountData is the on-disk format for tag count data.
type tagCountData struct {
	Entries map[string][24]float64 `json:"entries"` // tagValue(as string) → hourly counts
}

// Save writes tag count data to disk.
func (s *Store) Save(date, tagKey string, data map[int32]*hourlyCounter) {
	dir := filepath.Join(s.baseDir, date, "tagcnt")
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("TagCntStore: mkdir failed", "dir", dir, "error", err)
		return
	}

	tcd := &tagCountData{
		Entries: make(map[string][24]float64),
	}
	for k, hc := range data {
		tcd.Entries[itoa(int(k))] = hc.counts
	}

	path := filepath.Join(dir, tagKey+".json")
	f, err := os.Create(path)
	if err != nil {
		slog.Error("TagCntStore: create failed", "path", path, "error", err)
		return
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(tcd); err != nil {
		slog.Error("TagCntStore: encode failed", "path", path, "error", err)
	}
}

// Load reads tag count data from disk.
func (s *Store) Load(date, tagKey string) map[int32]*hourlyCounter {
	path := filepath.Join(s.baseDir, date, "tagcnt", tagKey+".json")
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var tcd tagCountData
	if err := json.NewDecoder(f).Decode(&tcd); err != nil {
		return nil
	}

	result := make(map[int32]*hourlyCounter)
	for k, counts := range tcd.Entries {
		val := atoi(k)
		result[int32(val)] = &hourlyCounter{counts: counts}
	}
	return result
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf) - 1
	for i > 0 {
		buf[pos] = byte('0' + i%10)
		pos--
		i /= 10
	}
	if neg {
		buf[pos] = '-'
		pos--
	}
	return string(buf[pos+1:])
}

func atoi(s string) int {
	neg := false
	start := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		start = 1
	}
	result := 0
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		result = result*10 + int(s[i]-'0')
	}
	if neg {
		return -result
	}
	return result
}
