// Repair rebuilds the database file with all records in sorted order.
//
// Over time, appends accumulate in the sparse region and lookups degrade
// toward linear scans. Repair reads every record, sorts by ID, and writes
// a new file with a contiguous heap (data + history sorted by ID then
// timestamp) followed by sorted indexes — restoring O(log n) binary
// search. It also serves as crash recovery: on Open, if a .tmp file or
// dirty flag is found, Repair is run automatically to restore consistency.
//
// A temporary file (.tmp) is used instead of rewriting in place because
// in-place rewrite risks total data loss on crash: if the process dies
// mid-rewrite, both the old and new data are gone. Writing to a temp
// file, syncing, then atomically renaming means the original file is
// intact until the rename succeeds. A crash during the write phase at
// worst orphans the .tmp file, which is cleaned up on next Open.
//
// The operation proceeds in two phases to minimise the time readers are
// blocked:
//
//   - Phase 1 (read lock): scan the old file and write the new .tmp file.
//     Concurrent readers continue using the old file.
//   - Phase 2 (write lock): swap file handles from the old file to the new
//     one. This is a brief exclusive lock for the atomic rename.
//
// When called for crash recovery (BlockReaders=true), a write lock is held
// for the entire operation since the file may be inconsistent.
package folio

import (
	"fmt"
	"io"
	"maps"
	"os"
	"slices"

	json "github.com/goccy/go-json"
)

type CompactOptions struct {
	BlockReaders bool // hold write lock for entire operation (crash recovery)
	PurgeHistory bool // drop history records from the output
}

// Repair rebuilds the file. See the package comment for phase details.
func (db *DB) Repair(opts *CompactOptions) error {
	if opts == nil {
		opts = &CompactOptions{}
	}

	// Restrict concurrent access for the duration of the rebuild
	if opts.BlockReaders {
		db.state.Store(StateNone)
	} else {
		db.state.Store(StateRead)
	}

	defer func() {
		if db.state.Load() != StateAll {
			db.cond.L.Lock()
			db.state.Store(StateAll)
			db.cond.Broadcast()
			db.cond.L.Unlock()
		}
	}()

	tmp, err := db.root.Create(db.name + ".tmp")
	if err != nil {
		return fmt.Errorf("repair: create temp: %w", err)
	}

	// Phase 1: scan old file, write new file.
	// The read lock (or write lock for crash recovery) is held for the
	// duration of Phase 1 and released before Phase 2 upgrades to write.
	if opts.BlockReaders {
		db.mu.Lock()
	} else {
		db.mu.RLock()
	}

	indexEnd, err := db.rebuild(tmp, opts)
	if err != nil {
		db.cond.L.Lock()
		db.state.Store(StateAll)
		db.cond.Broadcast()
		db.cond.L.Unlock()
		if opts.BlockReaders {
			db.mu.Unlock()
		} else {
			db.mu.RUnlock()
		}
		tmp.Close()
		return err
	}

	// Phase 2: swap file handles — brief exclusive lock
	if !opts.BlockReaders {
		db.mu.RUnlock()
		db.mu.Lock()
	}
	defer db.mu.Unlock()

	// Drain in-flight flock calls before closing the fd (see lock.go)
	db.lock.setFile(nil)

	db.reader.Close()
	db.writer.Close()

	if err := db.root.Rename(db.name+".tmp", db.name); err != nil {
		return fmt.Errorf("repair: rename: %w", err)
	}

	reader, err := db.root.OpenFile(db.name, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("repair: reopen reader: %w", err)
	}
	writer, err := db.root.OpenFile(db.name, os.O_RDWR, 0644)
	if err != nil {
		reader.Close()
		return fmt.Errorf("repair: reopen writer: %w", err)
	}
	hdrParsed, err := header(reader)
	if err != nil {
		reader.Close()
		writer.Close()
		return fmt.Errorf("repair: read header: %w", err)
	}

	db.reader = reader
	db.writer = writer
	db.lock.setFile(db.writer)
	db.header = hdrParsed
	db.count.Store(int64(hdrParsed.Count))
	db.tail = indexEnd

	if db.bloom != nil {
		db.bloom.Reset()
	}

	return nil
}

// rebuild writes the sorted output to tmp. Called with db.mu held (read or
// write depending on BlockReaders). On success it syncs and closes tmp, and
// returns the byte offset of the sparse region start for db.tail.
func (db *DB) rebuild(tmp *os.File, opts *CompactOptions) (int64, error) {
	info, err := db.reader.Stat()
	if err != nil {
		return 0, fmt.Errorf("repair: stat: %w", err)
	}
	entries := scanm(db.reader, HeaderSize, info.Size(), 0)

	// Split into heap (data+history) and indexes.
	exclude := []int{}
	if opts.PurgeHistory {
		exclude = append(exclude, TypeHistory)
	}
	heap, indexes := unpack(entries, exclude...)

	// Sort heap by ID then timestamp so all versions of a document are
	// contiguous, oldest first. History records (idx=3) for an ID precede
	// the current data record (idx=2) because they have earlier timestamps.
	slices.SortFunc(heap, byIDThenTS)

	// Keyed by label so each document keeps exactly one index in the output.
	// As records are written below, each index's DstOff is updated to the
	// record's new position in the output file.
	indexMap := map[string]*Entry{}
	for i := range indexes {
		indexMap[indexes[i].Label] = &indexes[i]
	}

	if _, err := tmp.Write(make([]byte, HeaderSize)); err != nil {
		return 0, fmt.Errorf("repair: write header placeholder: %w", err)
	}
	ow := &offsetWriter{w: tmp, off: HeaderSize}

	// Write heap: interleaved data + history sorted by ID then timestamp.
	for i := range heap {
		entry := &heap[i]
		record, err := line(db.reader, entry.SrcOff)
		if err != nil {
			if opts.BlockReaders {
				continue // crash recovery: salvage what we can
			}
			return 0, fmt.Errorf("repair: read record at %d: %w", entry.SrcOff, err)
		}

		entry.DstOff = ow.off
		if _, err := ow.Write(record); err != nil {
			return 0, fmt.Errorf("repair: write record: %w", err)
		}
		if _, err := ow.Write([]byte{'\n'}); err != nil {
			return 0, fmt.Errorf("repair: write newline: %w", err)
		}

		// Only update index offsets for current data records (not history).
		if entry.Type == TypeRecord {
			lbl := label(record)
			if idx, ok := indexMap[lbl]; ok {
				idx.DstOff = entry.DstOff
			}
		}
	}

	heapEnd := ow.off

	// Indexes are rewritten with updated offsets pointing to the records'
	// new positions in the output file.
	sorted := slices.SortedFunc(maps.Values(indexMap), byID)
	for _, idx := range sorted {
		indexRecord, err := json.Marshal(Index{
			Type:      TypeIndex,
			ID:        idx.ID,
			Offset:    idx.DstOff,
			Label:     idx.Label,
			Timestamp: now(),
		})
		if err != nil {
			return 0, fmt.Errorf("repair: marshal index: %w", err)
		}
		if _, err := ow.Write(indexRecord); err != nil {
			return 0, fmt.Errorf("repair: write index: %w", err)
		}
		if _, err := ow.Write([]byte{'\n'}); err != nil {
			return 0, fmt.Errorf("repair: write newline: %w", err)
		}
	}

	indexEnd := ow.off

	// Now that all sections are written, we know their boundary offsets.
	hdr := Header{
		Version:   2,
		Timestamp: now(),
		Algorithm: db.header.Algorithm,
		Heap:      heapEnd,
		Index:     indexEnd,
		Error:     0,
		Count:     len(indexMap),
	}
	hdrBytes, err := hdr.encode()
	if err != nil {
		return 0, fmt.Errorf("repair: encode header: %w", err)
	}
	if _, err := tmp.WriteAt(hdrBytes, 0); err != nil {
		return 0, fmt.Errorf("repair: write header: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return 0, fmt.Errorf("repair: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("repair: close temp: %w", err)
	}

	return indexEnd, nil
}

// offsetWriter adapts WriterAt to sequential writes. Repair needs WriterAt
// (to backfill the header at offset 0 after all sections are written) but
// also needs to track the current position for section boundary offsets.
type offsetWriter struct {
	w   io.WriterAt
	off int64
}

func (ow *offsetWriter) Write(p []byte) (int, error) {
	n, err := ow.w.WriteAt(p, ow.off)
	ow.off += int64(n)
	return n, err
}
