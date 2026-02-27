// Write primitives for the append-only file.
//
// New records are always appended at db.tail (the current end of file).
// The dirty flag is set on the first write of a session so that an unclean
// shutdown can be detected on next Open and trigger automatic repair.
// It is cleared during Close once all data has been flushed.
package folio

import (
	json "github.com/goccy/go-json"
)

// raw appends bytes at db.tail and advances the tail. The dirty flag is
// set on the first write so that a crash before Close triggers repair.
func (db *DB) raw(line []byte) (int64, error) {
	if db.header.Error == 0 {
		db.header.Error = 1
		dirty(db.writer, true)
	}
	// Every raw write increments the write counter so shouldCompact()
	// can fire auto-compaction when the counter hits the threshold modulus.
	// The counter resets to 0 after each compaction (see rebuild).
	db.header.State[stWrites]++

	offset := db.tail
	data := append(line, '\n')
	if _, err := db.writer.WriteAt(data, offset); err != nil {
		return 0, err
	}
	db.tail += int64(len(data))

	if db.config.SyncWrites {
		if err := db.writer.Sync(); err != nil {
			return 0, err
		}
	}
	return offset, nil
}

// append writes a data Record and its Index as a single batch. Both are
// concatenated into one buffer so a single WriteAt call places them
// adjacently — if the process crashes mid-write, repair will discard
// any incomplete trailing line.
func (db *DB) append(record *Record, idx *Index) (int64, error) {
	rData, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}

	dataOffset := db.tail
	idx.Offset = dataOffset // index points back to the record we are about to write

	iData, err := json.Marshal(idx)
	if err != nil {
		return 0, err
	}

	combined := make([]byte, 0, len(rData)+1+len(iData)+1)
	combined = append(combined, rData...)
	combined = append(combined, '\n')
	combined = append(combined, iData...)
	// raw() appends the final newline

	if _, err := db.raw(combined); err != nil {
		return 0, err
	}

	return dataOffset, nil
}

// shouldCompact reports whether the write counter has hit the auto-
// compaction threshold. A threshold of 0 disables auto-compaction.
// When enabled, compaction fires every N writes (where N is the
// threshold) using modular arithmetic: writes % threshold == 0.
// The caller must hold the write lock because this reads mutable
// header state.
func (db *DB) shouldCompact() bool {
	t := db.header.State[stThreshold]
	return t > 0 && db.header.State[stWrites]%t == 0
}

// writeAt patches bytes at an existing offset without moving the tail.
// Used for in-place modifications: toggling the type byte (2→3), blanking
// content, and overwriting invalidated index records with spaces.
func (db *DB) writeAt(offset int64, data []byte) error {
	if _, err := db.writer.WriteAt(data, offset); err != nil {
		return err
	}
	if db.config.SyncWrites {
		if err := db.writer.Sync(); err != nil {
			return err
		}
	}
	return nil
}
