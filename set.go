// Document creation and update.
package folio

import (
	"bytes"
	"strings"
)

// Set creates or updates a document.
func (db *DB) Set(label, content string) error {
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

	// Find old entry if exists
	var old *Result
	var oldIdx *Index

	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			old = result
			oldIdx = idx
		}
	}

	if old == nil {
		results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
		for i := len(results) - 1; i >= 0; i-- {
			idx, _ := decodeIndex(results[i].Data)
			if idx.Label == label {
				old = &results[i]
				oldIdx = idx
				break
			}
		}
	}

	// Prepare records
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

	// Atomic append
	dataOff, err := db.append(newRecord, newIndex)
	if err != nil {
		return err
	}
	_ = dataOff

	if db.bloom != nil {
		db.bloom.Add(id)
	}

	// Blank old records
	if old != nil {
		// Convert old data to history: idx 2 → 3
		db.writeAt(oldIdx.Offset+7, []byte("3"))

		// Blank _d content
		record, _ := line(db.reader, oldIdx.Offset)
		dStart := strings.Index(string(record), `"_d":"`) + 6
		dEnd := strings.Index(string(record), `","_h":"`)
		if dStart > 5 && dEnd > dStart {
			db.writeAt(oldIdx.Offset+int64(dStart), bytes.Repeat([]byte(" "), dEnd-dStart))
		}

		// Invalidate old index
		db.writeAt(old.Offset, bytes.Repeat([]byte(" "), old.Length))
	}

	return nil
}
