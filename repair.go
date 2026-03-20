// Repair rebuilds the database file with all records in sorted order.
//
// Over time, appends accumulate in the sparse region and lookups degrade
// toward linear scans. Repair reads every record, sorts by ID, and writes
// a new file with a contiguous heap (data + history sorted by ID then
// timestamp) followed by sorted indexes - restoring O(log n) binary
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
	"cmp"
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

	// Phase 2: swap file handles - brief exclusive lock
	if !opts.BlockReaders {
		db.mu.RUnlock()
		db.mu.Lock()
	}
	defer db.mu.Unlock()

	// Release the old mapping before closing its backing fd.
	munmapFile(db.mapped)
	db.mapped = nil

	// Drain in-flight flock calls before closing the fd (see lock.go)
	db.lock.SetFile(nil)

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
	db.lock.SetFile(db.writer)
	db.header = hdrParsed
	db.count.Store(hdrParsed.State[stCount])

	db.tail = indexEnd

	// Best effort: re-establish mmap over the new file. On failure,
	// reads fall back to file I/O for the rest of the session.
	if err := db.remap(); err != nil {
		db.config.MMap = false
		db.mapped = nil
	}

	if db.bloom != nil {
		db.bloom.Reset()
	}

	if db.index != nil {
		s := source{db.reader, db.tail}
		entries := scanm(s, db.indexStart(), db.indexEnd(), TypeIndex)
		clear(db.index)
		for _, e := range entries {
			db.index[e.ID] = e.SrcOff
		}
	}

	return nil
}

// rebuild writes the sorted output to tmp. Called with db.mu held (read or
// write depending on BlockReaders). On success it syncs and closes tmp, and
// returns the byte offset of the sparse region start for db.tail.
//
// Indexes in the input file are ignored entirely - they may be stale or
// orphaned after a crash. Output indexes are derived from the type 2
// records actually written to the heap. See audit.md for details.
func (db *DB) rebuild(tmp *os.File, opts *CompactOptions) (int64, error) {
	s := source{db.reader, db.tail}
	entries := scanm(s, HeaderSize, s.sz, 0)

	// Collect data and history records only - input indexes are discarded.
	records := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if e.Type == TypeIndex {
			continue
		}
		if opts.PurgeHistory && e.Type == TypeHistory {
			continue
		}
		records = append(records, e)
	}

	// Sort by ID then timestamp so all versions of a document (and hash
	// collisions) are contiguous, oldest first.
	slices.SortFunc(records, byIDThenTS)

	if _, err := tmp.Write(make([]byte, HeaderSize)); err != nil {
		return 0, fmt.Errorf("repair: write header placeholder: %w", err)
	}
	ow := &offsetWriter{w: tmp, off: HeaderSize}

	// current tracks the latest type 2 record per label. After the heap
	// is written, these become the output indexes.
	type current struct {
		id     string
		label  string
		offset int64
	}
	live := map[string]*current{} // keyed by label

	// Process records in ID-then-timestamp order. Within each ID group,
	// sub-group by label to handle hash collisions correctly.
	for i := 0; i < len(records); {
		// Find the extent of consecutive records sharing the same ID.
		j := i + 1
		for j < len(records) && records[j].ID == records[i].ID {
			j++
		}
		group := records[i:j]
		i = j

		// Sub-group by label. Multiple labels can share an ID (collision).
		byLabel := map[string][]int{} // label → indices into group
		for k := range group {
			byLabel[group[k].Label] = append(byLabel[group[k].Label], k)
		}

		for lbl, idxs := range byLabel {
			// idxs is sorted by timestamp (inherited from the outer sort).
			// The last entry is the latest version.
			latest := group[idxs[len(idxs)-1]]
			deleted := latest.Type == TypeHistory

			for _, k := range idxs {
				e := &group[k]
				isLast := k == idxs[len(idxs)-1]

				// Determine the correct type for the output.
				targetType := TypeHistory
				if isLast && !deleted {
					targetType = TypeRecord
				}

				if opts.PurgeHistory && targetType == TypeHistory {
					continue
				}

				data, err := line(s, e.SrcOff)
				if err != nil {
					if opts.BlockReaders {
						continue // crash recovery: salvage what we can
					}
					return 0, fmt.Errorf("repair: read record at %d: %w", e.SrcOff, err)
				}

				newOff := ow.off
				if int(data[TypePos]-'0') != targetType {
					data[TypePos] = byte('0' + targetType)
				}

				if _, err := ow.Write(data); err != nil {
					return 0, fmt.Errorf("repair: write record: %w", err)
				}
				if _, err := ow.Write([]byte{'\n'}); err != nil {
					return 0, fmt.Errorf("repair: write newline: %w", err)
				}

				if targetType == TypeRecord {
					live[lbl] = &current{id: e.ID, label: lbl, offset: newOff}
				}
			}
		}
	}

	heapEnd := ow.off

	// Write output indexes derived from the type 2 records we just wrote.
	// Sort by ID for binary search after compaction.
	labels := slices.Sorted(maps.Keys(live))
	slices.SortFunc(labels, func(a, b string) int {
		return cmp.Compare(live[a].id, live[b].id)
	})
	for _, lbl := range labels {
		cur := live[lbl]
		buf, err := json.Marshal(Index{
			Type:      TypeIndex,
			ID:        cur.id,
			Timestamp: now(),
			Offset:    cur.offset,
			Label:     cur.label,
		})
		if err != nil {
			return 0, fmt.Errorf("repair: marshal index: %w", err)
		}
		if _, err := ow.Write(buf); err != nil {
			return 0, fmt.Errorf("repair: write index: %w", err)
		}
		if _, err := ow.Write([]byte{'\n'}); err != nil {
			return 0, fmt.Errorf("repair: write newline: %w", err)
		}
	}

	indexEnd := ow.off

	hdr := Header{
		Version:   1,
		Timestamp: now(),
		Algorithm: db.header.Algorithm,
		State: [6]uint64{
			uint64(heapEnd),              // stHeap
			uint64(indexEnd),             // stIndex
			0,                            // stReserved
			uint64(len(live)),            // stCount
			0,                            // stWrites (reset after compaction)
			db.header.State[stThreshold], // stThreshold (preserve setting)
		},
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
