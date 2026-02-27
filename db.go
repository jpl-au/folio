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
	"path/filepath"
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
	AutoCompact   int  // compact every N writes; persisted to header, 0 = leave stored value unchanged
}

// DB is an open database handle. Two separate file descriptors are held
// so that concurrent reads never contend with writes. ReadAt on the
// O_RDONLY fd is position-independent and safe for concurrent readers;
// a single O_RDWR fd would have reads and appends fighting over the
// shared file position. Splitting eliminates that contention entirely.
type DB struct {
	root   *os.Root
	name   string
	reader *os.File  // read-only fd, shared by concurrent readers (ReadAt is position-independent)
	writer *os.File  // read-write fd, used for appends and patches
	lock   *fileLock // OS-level flock on the writer fd (see lock.go)
	header *Header   // cached, rewritten on Repair/Rehash
	config Config
	bloom  *bloom // nil unless Config.BloomFilter is set
	tail   int64  // next append position (current end of file)
	count  atomic.Uint64
	state  atomic.Int32
	// cond uses its own mutex, not db.mu, because sync.Cond requires a
	// plain Locker (Lock/Unlock). Using db.mu.Lock() would block all
	// readers during state waits. The separate mutex ensures state
	// transitions don't hold the RWMutex.
	cond *sync.Cond
	mu   sync.RWMutex // in-process read/write coordination
}

// Open opens or creates a database at the given path. If a previous
// session crashed (dirty flag set, or .tmp file left behind), an automatic
// Repair is attempted under an exclusive lock to restore consistency
// before returning.
func Open(path string, config Config) (*DB, error) {
	dir := filepath.Dir(path)
	name := filepath.Base(path)
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
			Version:   1,
			Timestamp: now(),
			Algorithm: config.HashAlgorithm,
		}
		hdr.State[stThreshold] = uint64(config.AutoCompact)
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
		if err := file.Sync(); err != nil {
			file.Close()
			root.Close()
			return nil, fmt.Errorf("sync header: %w", err)
		}
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
	db.count.Store(hdr.State[stCount])

	// A non-zero AutoCompact is a deliberate change — persist it to the
	// header so it survives future opens without needing to be repeated.
	if config.AutoCompact > 0 && uint64(config.AutoCompact) != hdr.State[stThreshold] {
		db.header.State[stThreshold] = uint64(config.AutoCompact)
		hdrBytes, err := db.header.encode()
		if err != nil {
			reader.Close()
			writer.Close()
			root.Close()
			return nil, fmt.Errorf("encode header: %w", err)
		}
		if _, err := writer.WriteAt(hdrBytes, 0); err != nil {
			reader.Close()
			writer.Close()
			root.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
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

	var errs []error

	if db.header.Error == 1 {
		db.header.Error = 0
		db.header.State[stCount] = db.count.Load()
		hdrBytes, err := db.header.encode()
		if err != nil {
			errs = append(errs, err)
		} else if _, err := db.writer.WriteAt(hdrBytes, 0); err != nil {
			errs = append(errs, err)
		}
		if err := db.writer.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
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
//   [Header][Heap: data+history by ID,TS][Indexes][Sparse→EOF]

// Count returns the current document count. This is a best-guess value
// maintained incrementally by Set and Delete. It is corrected to an
// accurate count during Compact or Repair.
func (db *DB) Count() int { return int(db.count.Load()) }

func (db *DB) heapEnd() int64 { return int64(db.header.State[stHeap]) }

func (db *DB) indexStart() int64 { return int64(db.header.State[stHeap]) }
func (db *DB) indexEnd() int64   { return int64(db.header.State[stIndex]) }

func (db *DB) sparseStart() int64 {
	if db.header.State[stIndex] == 0 {
		return HeaderSize
	}
	return int64(db.header.State[stIndex])
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
