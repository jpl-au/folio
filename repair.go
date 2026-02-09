// Repair rebuilds the database file with all records in sorted order.
//
// Over time, appends accumulate in the sparse region and lookups degrade
// toward linear scans. Repair reads every record, sorts by ID, and writes
// a new file with contiguous sorted sections — restoring O(log n) binary
// search. It also serves as crash recovery: on Open, if a .tmp file or
// dirty flag is found, Repair is run automatically to restore consistency.
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
	json "github.com/goccy/go-json"
	"io"
	"maps"
	"os"
	"slices"
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
		db.cond.L.Lock()
		db.state.Store(StateAll)
		db.cond.Broadcast()
		db.cond.L.Unlock()
	}()

	tmp, err := db.root.Create(db.name + ".tmp")
	if err != nil {
		return fmt.Errorf("repair: create temp: %w", err)
	}

	// Phase 1: scan old file, write new file
	if opts.BlockReaders {
		db.mu.Lock()
	} else {
		db.mu.RLock()
	}

	info, err := db.reader.Stat()
	if err != nil {
		if opts.BlockReaders {
			db.mu.Unlock()
		} else {
			db.mu.RUnlock()
		}
		tmp.Close()
		return fmt.Errorf("repair: stat: %w", err)
	}
	entries := scanm(db.reader, HeaderSize, info.Size(), 0)

	var records, history, indexes []Entry

	for _, e := range entries {
		switch e.Type {
		case TypeRecord:
			records = append(records, e)
		case TypeHistory:
			if !opts.PurgeHistory {
				history = append(history, e)
			}
		case TypeIndex:
			indexes = append(indexes, e)
		}
	}

	// Sorting by ID restores binary search; by TS within ID ensures the
	// newest record for each ID is written last (and wins during lookup).
	slices.SortFunc(records, byIDThenTS)
	slices.SortFunc(history, byIDThenTS)

	// Keyed by label so each document keeps exactly one index in the output.
	// As records are written below, each index's DstOff is updated to the
	// record's new position in the output file.
	indexMap := map[string]*Entry{}
	for i := range indexes {
		indexMap[indexes[i].Label] = &indexes[i]
	}

	tmp.Write(make([]byte, HeaderSize)) // placeholder, overwritten at the end
	ow := &offsetWriter{w: tmp, off: HeaderSize}

	// Write sections in order: records, history, indexes.
	// This matches the layout described in the Header type so that the
	// offset fields in the final header accurately delimit each section.

	for i := range records {
		entry := &records[i]
		record, err := line(db.reader, entry.SrcOff)
		if err != nil {
			continue // skip unreadable records
		}

		entry.DstOff = ow.off
		ow.Write(record)
		ow.Write([]byte{'\n'})

		lbl := label(record)
		if idx, ok := indexMap[lbl]; ok {
			idx.DstOff = entry.DstOff
		}
	}

	historyStart := ow.off

	for i := range history {
		entry := &history[i]
		record, err := line(db.reader, entry.SrcOff)
		if err != nil {
			continue // skip unreadable records
		}

		entry.DstOff = ow.off
		ow.Write(record)
		ow.Write([]byte{'\n'})
	}

	dataEnd := ow.off

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
			continue // skip unserializable indexes
		}
		ow.Write(indexRecord)
		ow.Write([]byte{'\n'})
	}

	indexEnd := ow.off

	// Now that all sections are written, we know their boundary offsets.
	hdr := Header{
		Version:   2,
		Timestamp: now(),
		Algorithm: db.header.Algorithm,
		History:   historyStart,
		Data:      dataEnd,
		Index:     indexEnd,
		Error:     0,
	}
	hdrBytes, err := hdr.encode()
	if err != nil {
		if opts.BlockReaders {
			db.mu.Unlock()
		} else {
			db.mu.RUnlock()
		}
		tmp.Close()
		return fmt.Errorf("repair: encode header: %w", err)
	}
	tmp.WriteAt(hdrBytes, 0)
	tmp.Sync()
	tmp.Close()

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
	db.tail = indexEnd

	if db.bloom != nil {
		db.bloom.Reset()
	}

	return nil
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
