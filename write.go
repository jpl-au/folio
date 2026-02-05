// Write operations for appending and modifying records.
//
// All write operations use the writer handle and track the tail offset.
// The dirty flag is set on first write and cleared on clean shutdown.
package folio

import (
	"encoding/json"
)

// raw writes raw bytes to end of file. Sets dirty flag on first write.
func (db *DB) raw(line []byte) (int64, error) {
	if db.header.Error == 0 {
		db.header.Error = 1
		dirty(db.writer, true)
	}

	offset := db.tail
	data := append(line, '\n')
	if _, err := db.writer.WriteAt(data, offset); err != nil {
		return 0, err
	}
	db.tail += int64(len(data))

	if db.config.SyncWrites {
		db.writer.Sync()
	}
	return offset, nil
}

// append marshals and writes a Data Record and Index Record atomically.
// Both records are concatenated and written in a single syscall to ensure cohesion.
func (db *DB) append(record *Record, idx *Index) (int64, error) {
	// Serialize Data Record
	rData, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}

	// Calculate Data offset (current tail)
	dataOffset := db.tail

	// Update Index with correct offset
	idx.Offset = dataOffset

	// Serialize Index Record
	iData, err := json.Marshal(idx)
	if err != nil {
		return 0, err
	}

	// Concatenate: Record + \n + Index + \n
	// Pre-allocate complete buffer to avoid reallocations
	totalLen := len(rData) + 1 + len(iData) + 1
	combined := make([]byte, 0, totalLen)

	combined = append(combined, rData...)
	combined = append(combined, '\n')
	combined = append(combined, iData...)
	// newline added by raw()

	// Write atomic batch
	// Note: raw() adds the final newline to the batch
	if _, err := db.raw(combined); err != nil {
		return 0, err
	}

	return dataOffset, nil
}

// writeAt overwrites at a specific position. Does not affect tail.
func (db *DB) writeAt(offset int64, data []byte) error {
	if _, err := db.writer.WriteAt(data, offset); err != nil {
		return err
	}
	if db.config.SyncWrites {
		db.writer.Sync()
	}
	return nil
}
