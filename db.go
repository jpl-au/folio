// Core database type and lifecycle operations.
//
// DB provides the main interface for document storage. It manages file handles,
// tracks state for concurrency control, and coordinates all read/write operations.
package folio

import (
	"bytes"
	"cmp"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
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

// CRUD Operations

// Get retrieves the current content of a document.
func (db *DB) Get(label string) (string, error) {
	if err := db.blockRead(); err != nil {
		return "", err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	// Binary search sorted index
	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			content, _ := line(db.reader, idx.Offset)
			record, _ := decode(content)
			return record.Data, nil
		}
	}

	// Linear scan sparse (reverse for newest)
	results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, _ := decodeIndex(results[i].Data)
		if idx.Label == label {
			content, _ := line(db.reader, idx.Offset)
			record, _ := decode(content)
			return record.Data, nil
		}
	}

	return "", ErrNotFound
}

// Set creates or updates a document.
func (db *DB) Set(label, content string) error {
	if len(label) > MaxLabelSize {
		return ErrLabelTooLong
	}
	if strings.Contains(label, `"`) {
		return ErrInvalidLabel
	}
	if content == "" {
		return ErrEmptyContent
	}

	if err := db.blockWrite(); err != nil {
		return err
	}
	defer func() {
		db.mu.Unlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	// Find old entry if exists
	var old *Result
	var oldIdx *Index

	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			old = result
			oldIdx = idx
		}
	}

	if old == nil {
		results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
		for i := len(results) - 1; i >= 0; i-- {
			idx, _ := decodeIndex(results[i].Data)
			if idx.Label == label {
				old = &results[i]
				oldIdx = idx
				break
			}
		}
	}

	// Prepare records
	newRecord := &Record{
		Type:      TypeRecord,
		ID:        id,
		Label:     label,
		Timestamp: now(),
		Data:      content,
		History:   compress([]byte(content)),
	}

	newIndex := &Index{
		Type:      TypeIndex,
		ID:        id,
		Label:     label,
		Timestamp: now(),
	}

	// Atomic append
	dataOff, err := db.append(newRecord, newIndex)
	if err != nil {
		return err
	}
	_ = dataOff

	// Blank old records
	if old != nil {
		// Convert old data to history: idx 2 → 3
		db.writeAt(oldIdx.Offset+7, []byte("3"))

		// Blank _d content
		record, _ := line(db.reader, oldIdx.Offset)
		dStart := strings.Index(string(record), `"_d":"`) + 6
		dEnd := strings.Index(string(record), `","_h":"`)
		if dStart > 5 && dEnd > dStart {
			db.writeAt(oldIdx.Offset+int64(dStart), bytes.Repeat([]byte(" "), dEnd-dStart))
		}

		// Invalidate old index
		db.writeAt(old.Offset, bytes.Repeat([]byte(" "), old.Length))
	}

	return nil
}

// Delete removes a document (soft delete, preserves history).
func (db *DB) Delete(label string) error {
	if err := db.blockWrite(); err != nil {
		return err
	}
	defer func() {
		db.mu.Unlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	// Binary search sorted index
	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			// Convert to history
			db.writeAt(idx.Offset+7, []byte("3"))

			// Blank _d content
			record, _ := line(db.reader, idx.Offset)
			dStart := strings.Index(string(record), `"_d":"`) + 6
			dEnd := strings.Index(string(record), `","_h":"`)
			if dStart > 5 && dEnd > dStart {
				db.writeAt(idx.Offset+int64(dStart), bytes.Repeat([]byte(" "), dEnd-dStart))
			}

			// Blank index
			db.writeAt(result.Offset, bytes.Repeat([]byte(" "), result.Length))
			return nil
		}
	}

	// Linear scan sparse
	results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			// Convert to history
			db.writeAt(idx.Offset+7, []byte("3"))

			// Blank _d content
			record, _ := line(db.reader, idx.Offset)
			dStart := strings.Index(string(record), `"_d":"`) + 6
			dEnd := strings.Index(string(record), `","_h":"`)
			if dStart > 5 && dEnd > dStart {
				db.writeAt(idx.Offset+int64(dStart), bytes.Repeat([]byte(" "), dEnd-dStart))
			}

			// Blank index
			db.writeAt(result.Offset, bytes.Repeat([]byte(" "), result.Length))
			return nil
		}
	}

	return ErrNotFound
}

// Exists checks if a document exists.
func (db *DB) Exists(label string) (bool, error) {
	if err := db.blockRead(); err != nil {
		return false, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			return true, nil
		}
	}

	results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, _ := decodeIndex(results[i].Data)
		if idx.Label == label {
			return true, nil
		}
	}

	return false, nil
}

// List returns all document labels.
func (db *DB) List() ([]string, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	seen := make(map[string]bool)
	var labels []string

	results := sparse(db.reader, "", HeaderSize, size(db.reader), TypeIndex)
	for _, result := range results {
		idx, _ := decodeIndex(result.Data)
		if !seen[idx.Label] {
			seen[idx.Label] = true
			labels = append(labels, idx.Label)
		}
	}

	return labels, nil
}

// Version represents a historical version of a document.
type Version struct {
	Data string
	TS   int64
}

// History retrieves all versions of a document.
func (db *DB) History(label string) ([]Version, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	// Collect with offset for stable ordering
	type versionWithOffset struct {
		Version
		offset int64
	}
	var versions []versionWithOffset

	for _, t := range []int{TypeRecord, TypeHistory} {
		results := sparse(db.reader, id, HeaderSize, size(db.reader), t)
		for _, result := range results {
			record, _ := decode(result.Data)
			if record.Label != label {
				continue
			}
			content := decompress(record.History)
			versions = append(versions, versionWithOffset{
				Version: Version{string(content), record.Timestamp},
				offset:  result.Offset,
			})
		}
	}

	// Sort by file offset (chronological write order)
	slices.SortFunc(versions, func(a, b versionWithOffset) int {
		return cmp.Compare(a.offset, b.offset)
	})

	out := make([]Version, len(versions))
	for i, v := range versions {
		out[i] = v.Version
	}
	return out, nil
}

// SearchOptions configures search behaviour.
type SearchOptions struct {
	CaseSensitive bool
	Limit         int
}

// Match represents a search result.
type Match struct {
	Label  string
	Offset int64
}

// Search performs regex search on file content.
func (db *DB) Search(pattern string, opts SearchOptions) ([]Match, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	if !opts.CaseSensitive {
		pattern = "(?i)" + pattern
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, ErrInvalidPattern
	}

	data, _ := io.ReadAll(io.NewSectionReader(db.reader, 0, size(db.reader)))
	matches := re.FindAllIndex(data, -1)

	var results []Match
	for _, m := range matches {
		results = append(results, Match{Offset: int64(m[0])})
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}
	}

	return results, nil
}

// MatchLabel performs regex search on document labels.
func (db *DB) MatchLabel(pattern string) ([]Match, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	re, err := regexp.Compile(`(?i){"idx":1[^}]*"_l":"[^"]*` + pattern + `[^"]*"`)
	if err != nil {
		return nil, ErrInvalidPattern
	}

	data, _ := io.ReadAll(io.NewSectionReader(db.reader, 0, size(db.reader)))
	indices := re.FindAllIndex(data, -1)

	var results []Match
	for _, m := range indices {
		lbl := label(data[m[0]:m[1]])
		results = append(results, Match{Label: lbl, Offset: int64(m[0])})
	}

	return results, nil
}
