package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// enc defines the encoding that we persist the record sizes and index entries in
	enc = binary.BigEndian
)

const (
	// number of bytes used to store the records length
	lenWidth = 8
)

/*
Simple wrapper around file with two APIs to append and read bytes to and from the file
*/
type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// Get file info especially size
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// Get file size in uint64
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

/*
Append adds
*/
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// Write to buffered writer instead of file directly to reduce the number of system calls and improve performance
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

/*
Read returns the record stored at the given position
*/
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// First flush write buffer, in case we're about to try to read a record
	// that the buffer hasn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

/*
ReadAt reads len(p) bytes into p beginning at the off offset in the store's file.
*/
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	// defer causes mu.Unlock() to be executed when the current scope is executed ( e.g. a function that returns )
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

