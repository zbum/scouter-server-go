package io

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// RealDataFile is an append-only data file with buffered writes.
type RealDataFile struct {
	mu       sync.Mutex
	filename string
	offset   int64
	file     *os.File
	writer   *bufio.Writer
}

func NewRealDataFile(filename string) (*RealDataFile, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &RealDataFile{
		filename: filename,
		offset:   fi.Size(),
		file:     f,
		writer:   bufio.NewWriterSize(f, 8192),
	}, nil
}

// Filename returns the file path.
func (f *RealDataFile) Filename() string {
	return f.filename
}

func (f *RealDataFile) GetOffset() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.offset
}

func (f *RealDataFile) WriteShort(s int16) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.offset
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(s))
	if _, err := f.writer.Write(buf[:]); err != nil {
		return 0, err
	}
	f.offset += 2
	return idx, nil
}

func (f *RealDataFile) WriteInt(i int32) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.offset
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(i))
	if _, err := f.writer.Write(buf[:]); err != nil {
		return 0, err
	}
	f.offset += 4
	return idx, nil
}

func (f *RealDataFile) Write(data []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.offset
	if _, err := f.writer.Write(data); err != nil {
		return 0, err
	}
	f.offset += int64(len(data))
	return idx, nil
}

func (f *RealDataFile) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writer.Flush()
}

func (f *RealDataFile) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writer != nil {
		f.writer.Flush()
	}
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
}
