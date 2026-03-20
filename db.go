// Core type definitions and structural helpers.
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
//     or LockFileEx. See internal/flock for lifetime management.
//
// When an operation starts, it waits (via db.cond) until the state allows
// it, then acquires the appropriate lock level at layers 2 and 3.
package folio

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jpl-au/folio/internal/bloom"
	"github.com/jpl-au/folio/internal/flock"
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
	MMap          bool // memory-map the file for reads (unix only)
	Index         bool // maintain in-memory index for O(1) lookups
}

// DB is an open database handle. Two separate file descriptors are held
// so that concurrent reads never contend with writes. ReadAt on the
// O_RDONLY fd is position-independent and safe for concurrent readers;
// a single O_RDWR fd would have reads and appends fighting over the
// shared file position. Splitting eliminates that contention entirely.
type DB struct {
	root   *os.Root
	name   string
	reader *os.File    // read-only fd, shared by concurrent readers (ReadAt is position-independent)
	writer *os.File    // read-write fd, used for appends and patches
	mapped []byte      // mmap'd read region; nil when Config.MMap is false
	lock   *flock.Lock // OS-level flock on the writer fd (see internal/flock)
	header *Header     // cached, rewritten on Repair/Rehash
	config Config
	bloom  *bloom.Filter    // nil unless Config.BloomFilter is set
	index  map[string]int64 // nil unless Config.Index is set
	tail   int64            // next append position (current end of file)
	count  atomic.Uint64
	state  atomic.Int32
	// cond uses its own mutex, not db.mu, because sync.Cond requires a
	// plain Locker (Lock/Unlock). Using db.mu.Lock() would block all
	// readers during state waits. The separate mutex ensures state
	// transitions don't hold the RWMutex.
	cond *sync.Cond
	mu   sync.RWMutex // in-process read/write coordination
}

// Section boundary helpers. These translate header offsets into the ranges
// passed to scan (binary search) and sparse (linear scan). A zero header
// offset means the section is empty - we fall back to HeaderSize so scans
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

// src returns a source for read-path operations. When mmap is enabled
// it reads from the mapped region; otherwise from the read-only fd.
func (db *DB) src() source {
	if db.mapped != nil {
		return source{mapped(db.mapped), int64(len(db.mapped))}
	}
	return source{db.reader, db.tail}
}

// remap replaces the current memory mapping with one covering the
// full file. Must be called with db.mu held for writing - readers
// hold references to the mapped slice via src(), and munmap would
// invalidate them.
func (db *DB) remap() error {
	if !db.config.MMap {
		return nil
	}
	if err := munmapFile(db.mapped); err != nil {
		return fmt.Errorf("remap: munmap: %w", err)
	}
	data, err := mmapFile(db.reader, db.tail)
	if err != nil {
		db.mapped = nil
		return fmt.Errorf("remap: mmap: %w", err)
	}
	db.mapped = data
	return nil
}
