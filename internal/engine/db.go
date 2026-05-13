package engine

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// KeyDirEntry points to the exact byte location of a value on disk
// Acts like file offset lookup table
type KeyDirEntry struct {
	ValuePos  int64
	ValueSize uint32
}

// DO represents Bitcask database instance
type DB struct {
	file   *os.File
	mu     sync.RWMutex
	keydir map[string]KeyDirEntry
}

// Open initializes the database file and builds the inmemory index
func Open(path string) (*DB, error) {
	// os.O_Append ensures all os-level writes are forced to the end of the file.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	db := &DB{
		file:   file,
		keydir: make(map[string]KeyDirEntry),
	}
	// Parse the binary log to rebuild our in-memory hashmap
	if err := db.loadIndex(); err != nil {
		return nil, err

	}
	return db, nil
}

// Put appends a new key-value record to the disk log and updates the index.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	buf := Encode(key, value)

	// Find the current end of file to know our starting offset

	offset, err := db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err := db.file.Write(buf); err != nil {
		return err
	}
	// Calculate exactly where the value payload starts in this frame
	valPos := offset + int64(HeaderSize) + int64(len(key))
	//Store the pointer in our index. If the key already existed,
	// this instantly overwrites the old pointer. The old data becomes "dead wood" on disk.
	db.keydir[string(key)] = KeyDirEntry{
		ValuePos:  valPos,
		ValueSize: uint32(len(value)),
	}

	return nil
}

// Get Fetches a value directly from disk using the memory index.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	entry, exists := db.keydir[string(key)]
	db.mu.RUnlock() // We Unlock early because we do not need to block other Threads during disk I/O.

	if !exists {
		return nil, errors.New("key not found")
	}
	val := make([]byte, entry.ValueSize)

	if _, err := db.file.ReadAt(val, entry.ValuePos); err != nil {
		return nil, err
	}
	return val, nil
}

// Close shuts down the file descriptor
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.file.Sync()
	return db.file.Close()
}

func (db *DB) loadIndex() error {
	var offset int64 = 0
	headerbuf := make([]byte, HeaderSize)

	for {
		// Read just th efixed sixe header first
		_, err := db.file.ReadAt(headerbuf, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		keySz := binary.LittleEndian.Uint32(headerbuf[12:16])
		valSz := binary.LittleEndian.Uint32(headerbuf[16:20])

		keyBuf := make([]byte, keySz)
		if _, err := db.file.ReadAt(keyBuf, offset+int64(HeaderSize)); err != nil {
			return err
		}
		valPos := offset + int64(HeaderSize) + int64(keySz)

		db.keydir[string(keyBuf)] = KeyDirEntry{
			ValuePos:  valPos,
			ValueSize: valSz,
		}

		offset += int64(HeaderSize) + int64(keySz) + int64(valSz)
	}
	return nil
}
