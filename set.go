// Document creation and update using append-then-blank.
//
// A Set always appends a new Record+Index pair at the tail. If an older
// version exists, it is then patched in place: its type byte is changed
// from Record (2) to History (3), its _d content is blanked with spaces,
// and its index line is overwritten with spaces. The compressed snapshot
// in _h is preserved so History can still retrieve the old content.
// This approach avoids rewriting the file on every update while keeping
// the latest version immediately accessible via the newest index.
package folio

import (
	"bytes"
	"fmt"
	"strings"
)

// Set creates or updates a document. See the package comment for the
// append-then-blank strategy.
func (db *DB) Set(label, content string) error {
	if label == "" {
		return ErrInvalidLabel
	}
	if len(label) > MaxLabelSize {
		return ErrLabelTooLong
	}
	if strings.Contains(label, `"`) {
		return ErrInvalidLabel
	}
	if content == "" {
		return ErrEmptyContent
	}

	if err := db.blockWrite(); err != nil {
		return err
	}
	defer func() {
		db.mu.Unlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	var old *Result
	var oldIdx *Index

	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return fmt.Errorf("set: %w", err)
		}
		if idx.Label == label {
			old = result
			oldIdx = idx
		}
	}

	if old == nil {
		sz, err := size(db.reader)
		if err != nil {
			return fmt.Errorf("set: stat: %w", err)
		}
		// Reverse iterate: the sparse region is append-only, so the newest
		// version is at the highest offset. Walking backwards finds the
		// latest version first and breaks immediately.
		results := sparse(db.reader, id, db.sparseStart(), sz, TypeIndex)
		for i := len(results) - 1; i >= 0; i-- {
			idx, err := decodeIndex(results[i].Data)
			if err != nil {
				return fmt.Errorf("set: %w", err)
			}
			if idx.Label == label {
				old = &results[i]
				oldIdx = idx
				break
			}
		}
	}

	newRecord := &Record{
		Type:      TypeRecord,
		ID:        id,
		Label:     label,
		Timestamp: now(),
		Data:      content,
		History:   compress([]byte(content)),
	}

	newIndex := &Index{
		Type:      TypeIndex,
		ID:        id,
		Label:     label,
		Timestamp: now(),
	}

	if _, err := db.append(newRecord, newIndex); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	if db.bloom != nil {
		db.bloom.Add(id)
	}

	// Retire the previous version: retype to history, blank _d, erase index
	if old != nil {
		if err := db.writeAt(oldIdx.Offset+7, []byte("3")); err != nil {
			return fmt.Errorf("set: retype record: %w", err)
		}

		record, err := line(db.reader, oldIdx.Offset)
		if err != nil {
			return fmt.Errorf("set: read old record: %w", err)
		}
		dStart := strings.Index(string(record), `"_d":"`) + 6
		dEnd := strings.Index(string(record), `","_h":"`)
		if dStart > 5 && dEnd > dStart {
			if err := db.writeAt(oldIdx.Offset+int64(dStart), bytes.Repeat([]byte(" "), dEnd-dStart)); err != nil {
				return fmt.Errorf("set: blank content: %w", err)
			}
		}

		if err := db.writeAt(old.Offset, bytes.Repeat([]byte(" "), old.Length)); err != nil {
			return fmt.Errorf("set: erase index: %w", err)
		}
	}

	return nil
}
