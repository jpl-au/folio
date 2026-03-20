// In-place hash algorithm migration.
//
// All three algorithms produce a 16 hex character (8 byte) _id, and the _id
// field sits at a fixed byte offset in every record. This means Rehash can
// overwrite each _id in place without moving or resizing any records  - 
// no temp file, no rewrite, just a linear scan with targeted byte patches.
//
// The dirty flag is set before any patches begin and cleared after the
// header is updated. A crash mid-rehash leaves the flag set, so the next
// Open triggers automatic Repair - which rebuilds all IDs from labels,
// restoring consistency regardless of how many patches completed.
package folio

import "fmt"

// Rehash migrates all records to a new hash algorithm. Blocks all readers
// and writers because every _id in the file is being rewritten.
func (db *DB) Rehash(newAlg int) error {
	db.state.Store(StateNone)
	defer func() {
		db.cond.L.Lock()
		db.state.Store(StateAll)
		db.cond.Broadcast()
		db.cond.L.Unlock()
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	s := source{db.reader, db.tail}
	entries := scanm(s, HeaderSize, s.sz, 0)

	// Set dirty flag so a crash mid-patch triggers automatic Repair.
	if err := dirty(db.writer, true); err != nil {
		return fmt.Errorf("rehash: set dirty: %w", err)
	}
	db.header.Error = 1

	cache := map[string]string{} // label→newID, avoids rehashing the same label twice

	for _, entry := range entries {
		lbl := entry.Label
		if cache[lbl] == "" {
			cache[lbl] = hash(lbl, newAlg)
		}
		if _, err := db.writer.WriteAt([]byte(cache[lbl]), entry.SrcOff+IDStart); err != nil {
			return fmt.Errorf("rehash: write id: %w", err)
		}
	}

	db.header.Algorithm = newAlg
	db.header.Timestamp = now()
	hdrBytes, err := db.header.encode()
	if err != nil {
		return fmt.Errorf("rehash: encode header: %w", err)
	}
	if _, err := db.writer.WriteAt(hdrBytes, 0); err != nil {
		return fmt.Errorf("rehash: write header: %w", err)
	}
	if err := db.writer.Sync(); err != nil {
		return fmt.Errorf("rehash: sync: %w", err)
	}

	// All patches and the header are on disk - clear the dirty flag.
	if err := dirty(db.writer, false); err != nil {
		return fmt.Errorf("rehash: clear dirty: %w", err)
	}
	db.header.Error = 0

	// Rebuild bloom and in-memory index with the new IDs.
	if db.bloom != nil {
		db.bloom.Reset()
		s = source{db.reader, db.tail}
		idxEntries := scanm(s, db.sparseStart(), s.sz, TypeIndex)
		for _, e := range idxEntries {
			db.bloom.Add(e.ID)
		}
	}

	if db.index != nil {
		s = source{db.reader, db.tail}
		idxEntries := scanm(s, HeaderSize, s.sz, TypeIndex)
		clear(db.index)
		for _, e := range idxEntries {
			db.index[e.ID] = e.SrcOff
		}
	}

	return nil
}
