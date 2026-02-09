// Database lifecycle: open, close, crash recovery.
//
// Concurrency is managed in three layers, each serving a different scope:
//
//  1. Atomic state machine (db.state): gates whether new operations are
//     allowed at all. Transitions: StateAll → StateRead → StateNone →
//     StateClosed. Checked at the top of every public method.
//
//  2. sync.RWMutex (db.mu): coordinates in-process readers and writers.
//     Readers hold RLock; writers and Repair hold Lock.
//
//  3. OS file lock (db.lock): coordinates across processes via flock(2)
//     or LockFileEx. See lock.go for lifetime management.
//
// When an operation starts, it waits (via db.cond) until the state allows
// it, then acquires the appropriate lock level at layers 2 and 3.
package folio

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// State machine values. Transitions are monotonic during shutdown
// (All→Closed) but cycle during maintenance (All→Read→All for Compact,
// All→None→All for Rehash). blockRead/blockWrite wait on db.cond until
// the state allows their operation.
const (
	StateAll    = 0 // reads and writes permitted
	StateRead   = 1 // reads only (Compact in progress, Phase 1)
	StateNone   = 2 // nothing permitted (Rehash / crash recovery)
	StateClosed = 3 // terminal
)

// Config tunes the memory/disk trade-off. Zero values use safe defaults.
type Config struct {
	HashAlgorithm int  // 1=xxHash3 (default), 2=FNV1a, 3=Blake2b
	ReadBuffer    int  // scanner buffer (default 64KB)
	MaxRecordSize int  // largest allowed record (default 16MB)
	SyncWrites    bool // fsync after every write (durability vs throughput)
	BloomFilter   bool // maintain bloom filter over the sparse region
}

// DB is an open database handle. Two separate file descriptors are held:
// reader (O_RDONLY) for concurrent reads and writer (O_RDWR) for appends
// and in-place patches. The OS file lock is taken on the writer fd.
type DB struct {
	root   *os.Root
	name   string
	reader *os.File  // read-only fd, shared by concurrent readers
	writer *os.File  // read-write fd, used for appends and patches
	lock   *fileLock // OS-level flock on the writer fd (see lock.go)
	header *Header   // cached, rewritten on Repair/Rehash
	config Config
	bloom  *bloom // nil unless Config.BloomFilter is set
	tail   int64  // next append position (current end of file)
	state  atomic.Int32
	cond   *sync.Cond   // waiters blocked by state transitions
	mu     sync.RWMutex // in-process read/write coordination
}

// Open opens or creates a database. If a previous session crashed (dirty
// flag set, or .tmp file left behind), an automatic Repair is attempted
// under an exclusive lock to restore consistency before returning.
func Open(dir, name string, config Config) (*DB, error) {
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

	_, err = root.Stat(name)
	if os.IsNotExist(err) {
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
		buf, err := hdr.encode()
		if err != nil {
			file.Close()
			root.Close()
			return nil, fmt.Errorf("encode header: %w", err)
		}
		if _, err := file.Write(buf); err != nil {
			file.Close()
			root.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
		file.Sync()
		file.Close()
	}

	reader, err := root.OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		root.Close()
		return nil, err
	}

	writer, err := root.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		reader.Close()
		root.Close()
		return nil, err
	}

	flock := &fileLock{f: writer}

	info, err := writer.Stat()
	if err != nil {
		reader.Close()
		writer.Close()
		root.Close()
		return nil, fmt.Errorf("stat: %w", err)
	}
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

	if config.BloomFilter {
		db.bloom = newBloom()
		entries := scanm(reader, db.sparseStart(), info.Size(), TypeIndex)
		for _, e := range entries {
			db.bloom.Add(e.ID)
		}
	}

	// A leftover .tmp file or a dirty header means the previous session
	// crashed mid-write. Repair rebuilds the file from its surviving records.
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

// Close flushes state, clears the dirty flag if set, and releases all
// file handles. Any blocked operations wake up and receive ErrClosed.
func (db *DB) Close() error {
	db.cond.L.Lock()
	db.state.Store(StateClosed)
	db.cond.Broadcast()
	db.cond.L.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	// Drain in-flight flock calls before closing the fd (see lock.go)
	if db.lock != nil {
		db.lock.setFile(nil)
	}

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

	return errors.Join(errs...)
}

// Section boundary helpers. These translate header offsets into the ranges
// passed to scan (binary search) and sparse (linear scan). A zero header
// offset means the section is empty — we fall back to HeaderSize so scans
// start at the first possible record position.
//
// File layout after compaction:
//   [Header][Records][History][Indexes][Sparse→EOF]

func (db *DB) indexStart() int64 { return db.header.Data }
func (db *DB) indexEnd() int64   { return db.header.Index }

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

// blockWrite and blockRead acquire all three concurrency layers (state
// check → OS flock → RWMutex) before allowing an operation to proceed.
// On return the caller holds db.mu (Lock or RLock) and db.lock; both
// must be released in the defer of the calling method.

func (db *DB) blockWrite() error {
	if db.state.Load() == StateClosed {
		return ErrClosed
	}

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
	if db.state.Load() == StateClosed {
		return ErrClosed
	}

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
