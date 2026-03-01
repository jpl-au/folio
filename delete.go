// Soft deletion — the record is converted to history so its compressed
// snapshot survives for version retrieval, but it no longer appears in
// lookups or listings because its index is erased.
package folio

import (
	"bytes"
	"fmt"
	"strings"
)

// Delete soft-removes a document. The record's compressed history snapshot
// is preserved; only Purge permanently removes it.
func (db *DB) Delete(label string) error {
	if err := db.blockWrite(); err != nil {
		return err
	}

	err := db.delete(label)
	if err == nil {
		db.remap()
	}

	// Check threshold under lock, compact after release (see set.go).
	compact := err == nil && db.shouldCompact()
	db.mu.Unlock()
	db.lock.Unlock()

	if compact {
		db.Compact()
	}
	return err
}

// delete performs the soft-removal. The write lock must be held.
func (db *DB) delete(label string) error {
	id := hash(label, db.header.Algorithm)
	s := source{db.reader, db.tail}

	idxResult, idx, err := db.findIndex(id, label, s)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if idxResult == nil {
		return ErrNotFound
	}

	if err := blank(db, idx.Offset, idxResult); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if db.index != nil {
		delete(db.index, id)
	}

	db.count.Add(^uint64(0)) // unsigned decrement
	return nil
}

// blank retires a record: patches its type from Record to History (2→3),
// overwrites _d with spaces so it doesn't appear in content searches,
// and erases the index line so the document is no longer discoverable.
// The _h field is left intact for version retrieval.
func blank(db *DB, dataOff int64, idx *Result) error {
	if err := db.writeAt(dataOff+TypePos, []byte("3")); err != nil {
		return fmt.Errorf("retype record: %w", err)
	}

	s := source{db.reader, db.tail}
	record, err := line(s, dataOff)
	if err != nil {
		return fmt.Errorf("read record: %w", err)
	}
	dStart := strings.Index(string(record), `"_d":"`) + 6
	dEnd := strings.Index(string(record), `","_h":"`)
	if dStart > 5 && dEnd > dStart {
		if err := db.writeAt(dataOff+int64(dStart), bytes.Repeat([]byte(" "), dEnd-dStart)); err != nil {
			return fmt.Errorf("blank content: %w", err)
		}
	}

	if err := db.writeAt(idx.Offset, bytes.Repeat([]byte(" "), idx.Length)); err != nil {
		return fmt.Errorf("erase index: %w", err)
	}
	return nil
}
