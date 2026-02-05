// Document deletion.
package folio

import (
	"bytes"
	"strings"
)

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
