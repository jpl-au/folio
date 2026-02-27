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

	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		if idx.Label == label {
			if err := blank(db, idx.Offset, result); err != nil {
				return fmt.Errorf("delete: %w", err)
			}
			db.count.Add(^uint64(0)) // unsigned decrement: ^uint64(0) == max uint64 == -1 in twos-complement
			return nil
		}
	}

	sz, err := size(db.reader)
	if err != nil {
		return fmt.Errorf("delete: stat: %w", err)
	}
	// Reverse iterate: newest version is at the highest offset (see set.go).
	results := sparse(db.reader, id, db.sparseStart(), sz, TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		result := results[i]
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		if idx.Label == label {
			if err := blank(db, idx.Offset, &result); err != nil {
				return fmt.Errorf("delete: %w", err)
			}
			db.count.Add(^uint64(0)) // unsigned decrement
			return nil
		}
	}

	return ErrNotFound
}

// blank retires a record: patches its type from Record to History (2→3),
// overwrites _d with spaces so it doesn't appear in content searches,
// and erases the index line so the document is no longer discoverable.
// The _h field is left intact for version retrieval.
func blank(db *DB, dataOff int64, idx *Result) error {
	if err := db.writeAt(dataOff+TypePos, []byte("3")); err != nil {
		return fmt.Errorf("retype record: %w", err)
	}

	record, err := line(db.reader, dataOff)
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
