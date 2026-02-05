// Core database type and lifecycle operations.
//
// DB provides the main interface for document storage. It manages file handles,
// tracks state for concurrency control, and coordinates all read/write operations.
package folio

import (
	"os"
	"sync"
	"sync/atomic"
)

// State constants for concurrency control.
const (
	StateAll    = 0 // Readers and writers allowed
	StateRead   = 1 // Only readers allowed (during compaction)
	StateNone   = 2 // Nothing allowed (during rehash)
	StateClosed = 3 // Database closed
)

// Config holds database configuration options.
type Config struct {
	HashAlgorithm int  // 1=xxHash3, 2=FNV1a, 3=Blake2b
	ReadBuffer    int  // Buffer size for reading (default 64KB)
	MaxRecordSize int  // Maximum single record size (default 16MB)
	SyncWrites    bool // Call fsync after writes
}

// DB represents an open database.
type DB struct {
	root   *os.Root  // Sandboxed filesystem access
	name   string    // Database filename
	reader *os.File  // Read handle (O_RDONLY)
	writer *os.File  // Write handle (O_RDWR)
	lock   *fileLock // OS-level file lock
	header *Header   // Cached header
	config Config    // Configuration
	tail   int64     // Append offset (end of file)
	state  atomic.Int32
	cond   *sync.Cond
	mu     sync.RWMutex
}

// Open opens or creates a database file.
func Open(dir, name string, config Config) (*DB, error) {
	// Default config values
	if config.HashAlgorithm == 0 {
		config.HashAlgorithm = AlgXXHash3
	}
	if config.ReadBuffer == 0 {
		config.ReadBuffer = 64 * 1024
	}
	if config.MaxRecordSize == 0 {
		config.MaxRecordSize = 16 * 1024 * 1024
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}

	// Check if file exists
	_, err = root.Stat(name)
	if os.IsNotExist(err) {
		// Create new database
		file, err := root.Create(name)
		if err != nil {
			root.Close()
			return nil, err
		}
		hdr := Header{
			Version:   2,
			Timestamp: now(),
			Algorithm: config.HashAlgorithm,
			History:   0,
			Data:      0,
			Index:     0,
			Error:     0,
		}
		buf, _ := hdr.encode()
		file.Write(buf)
		file.Sync()
		file.Close()
	}

	// Open reader handle
	reader, err := root.OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		root.Close()
		return nil, err
	}

	// Open writer handle
	writer, err := root.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		reader.Close()
		root.Close()
		return nil, err
	}

	// Open lock handle (using the writer file descriptor)
	flock := &fileLock{f: writer}

	info, _ := writer.Stat()
	hdr, err := header(reader)
	if err != nil {
		reader.Close()
		writer.Close()
		root.Close()
		return nil, err
	}

	db := &DB{
		root:   root,
		name:   name,
		reader: reader,
		writer: writer,
		lock:   flock,
		header: hdr,
		config: config,
		tail:   info.Size(),
		cond:   sync.NewCond(&sync.Mutex{}),
	}

	// Crash detection
	_, tmpErr := root.Stat(name + ".tmp")
	tmpExists := tmpErr == nil
	needsRepair := tmpExists || db.header.Error == 1

	if needsRepair {
		if tmpExists {
			root.Remove(name + ".tmp")
		}
		// Attempt to acquire exclusive lock for repair
		if err := db.lock.Lock(LockExclusive); err == nil {
			defer db.lock.Unlock()
			db.Repair(&CompactOptions{BlockReaders: true})
		}
	}

	return db, nil
}

// Close closes the database and releases resources.
func (db *DB) Close() error {
	db.cond.L.Lock()
	db.state.Store(StateClosed)
	db.cond.Broadcast()
	db.cond.L.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	// Ensure lock is released
	if db.lock != nil {
		db.lock.Unlock()
	}

	// Mark clean shutdown
	if db.header.Error == 1 {
		db.header.Error = 0
		dirty(db.writer, false)
		db.writer.Sync()
	}

	var errs []error
	if err := db.reader.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := db.writer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := db.root.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Section boundary methods

func (db *DB) indexStart() int64 {
	return db.header.Data
}

func (db *DB) indexEnd() int64 {
	return db.header.Index
}

func (db *DB) historyStart() int64 {
	if db.header.History == 0 {
		return HeaderSize
	}
	return db.header.History
}

func (db *DB) sparseStart() int64 {
	if db.header.Index == 0 {
		return HeaderSize
	}
	return db.header.Index
}

// Blocking methods for concurrency control

func (db *DB) blockWrite() error {
	// Check closed state before acquiring OS lock
	if db.state.Load() == StateClosed {
		return ErrClosed
	}

	// Acquire OS lock
	if err := db.lock.Lock(LockExclusive); err != nil {
		return err
	}

	db.cond.L.Lock()
	for db.state.Load() != StateAll {
		if db.state.Load() == StateClosed {
			db.cond.L.Unlock()
			db.lock.Unlock()
			return ErrClosed
		}
		db.cond.Wait()
	}
	db.mu.Lock()
	db.cond.L.Unlock()
	return nil
}

func (db *DB) blockRead() error {
	// Check closed state before acquiring OS lock
	if db.state.Load() == StateClosed {
		return ErrClosed
	}

	// Acquire OS lock
	if err := db.lock.Lock(LockShared); err != nil {
		return err
	}

	db.cond.L.Lock()
	for db.state.Load() == StateNone || db.state.Load() == StateClosed {
		if db.state.Load() == StateClosed {
			db.cond.L.Unlock()
			db.lock.Unlock()
			return ErrClosed
		}
		db.cond.Wait()
	}
	db.mu.RLock()
	db.cond.L.Unlock()
	return nil
}
