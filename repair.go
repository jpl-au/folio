// Core repair operation for database maintenance.
//
// Repair reorganises the file into sorted sections for efficient binary search.
// It handles crash recovery and is the foundation for Compact and Purge.
package folio

import (
	json "github.com/goccy/go-json"
	"io"
	"maps"
	"os"
	"slices"
)

// CompactOptions configures repair behaviour.
type CompactOptions struct {
	BlockReaders bool // true = block all operations (crash recovery)
	PurgeHistory bool // true = remove history records
}

// Repair reorganises the database file.
func (db *DB) Repair(opts *CompactOptions) error {
	if opts == nil {
		opts = &CompactOptions{}
	}

	// Set state
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

	// Create temp file
	tmp, err := db.root.Create(db.name + ".tmp")
	if err != nil {
		return err
	}

	// Phase 1: Heavy work
	if opts.BlockReaders {
		db.mu.Lock()
	} else {
		db.mu.RLock()
	}

	info, _ := db.reader.Stat()
	entries := scanm(db.reader, HeaderSize, info.Size(), 0)

	var records, history, indexes []Entry

	// Separate entries by type
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

	// Sort Records and History by ID then TS
	slices.SortFunc(records, byIDThenTS)
	slices.SortFunc(history, byIDThenTS)

	// Build index map by label
	indexMap := map[string]*Entry{}
	for i := range indexes {
		indexMap[indexes[i].Label] = &indexes[i]
	}

	// Write header placeholder
	tmp.Write(make([]byte, HeaderSize))

	// Track write position
	ow := &offsetWriter{w: tmp, off: HeaderSize}

	// 1. Write Data Records
	for i := range records {
		entry := &records[i]
		record, _ := line(db.reader, entry.SrcOff)

		entry.DstOff = ow.off
		ow.Write(record)
		ow.Write([]byte{'\n'})

		// Update index pointer
		lbl := label(record)
		if idx, ok := indexMap[lbl]; ok {
			idx.DstOff = entry.DstOff
		}
	}

	historyStart := ow.off

	// 2. Write History Records
	for i := range history {
		entry := &history[i]
		record, _ := line(db.reader, entry.SrcOff)

		// History doesn't need index updates (it's pointed to by the record's _h field usually,
		// but actually folio history is self-contained in the record _h field usually?
		// Wait, DESIGN.md says History Record is a separate record type `idx:3`.
		// So we just write them block-by-block.

		entry.DstOff = ow.off
		ow.Write(record)
		ow.Write([]byte{'\n'})
	}

	dataEnd := ow.off

	// 3. Write Index Records
	sorted := slices.SortedFunc(maps.Values(indexMap), byID)
	for _, idx := range sorted {
		indexRecord, _ := json.Marshal(Index{
			Type:      TypeIndex,
			ID:        idx.ID,
			Offset:    idx.DstOff, // Points to the new location of the Data Record
			Label:     idx.Label,
			Timestamp: now(),
		})
		ow.Write(indexRecord)
		ow.Write([]byte{'\n'})
	}

	indexEnd := ow.off

	// Write final header
	hdr := Header{
		Version:   2, // New version
		Timestamp: now(),
		Algorithm: db.header.Algorithm,
		History:   historyStart,
		Data:      dataEnd,
		Index:     indexEnd,
		Error:     0,
	}
	hdrBytes, _ := hdr.encode()
	tmp.WriteAt(hdrBytes, 0)
	tmp.Sync()
	tmp.Close()

	// Phase 2: Swap handles
	if !opts.BlockReaders {
		db.mu.RUnlock()
		db.mu.Lock()
	}
	defer db.mu.Unlock()

	db.reader.Close()
	db.writer.Close()
	db.root.Rename(db.name+".tmp", db.name)
	db.reader, _ = db.root.OpenFile(db.name, os.O_RDONLY, 0644)
	db.writer, _ = db.root.OpenFile(db.name, os.O_RDWR, 0644)
	db.lock.f = db.writer
	db.header, _ = header(db.reader)
	db.tail = indexEnd

	if db.bloom != nil {
		db.bloom.Reset()
	}

	return nil
}

// offsetWriter tracks write position for sequential writes.
type offsetWriter struct {
	w   io.WriterAt
	off int64
}

func (ow *offsetWriter) Write(p []byte) (int, error) {
	n, err := ow.w.WriteAt(p, ow.off)
	ow.off += int64(n)
	return n, err
}
