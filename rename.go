// Label renaming with in-place patching when possible.
//
// When old and new labels have the same byte length, Rename patches
// _id and _l directly in the data record and index record — no new
// version is created and no history entry is added. When lengths
// differ, it falls back to appending a new record+index and blanking
// the old ones (equivalent to Set+Delete but under a single lock hold).
//
// History records are not patched in either path: they retain the old
// ID and become unreachable via History(newLabel). This matches the
// behaviour callers would get from the manual Get+Set+Delete approach.
package folio

import (
	"bytes"
	"fmt"
	"strings"
)

// Rename changes a document's label. Returns ErrNotFound if old does
// not exist, or ErrExists if new already exists.
func (db *DB) Rename(old, new string) error {
	if old == "" || new == "" {
		return ErrInvalidLabel
	}
	if len(new) > MaxLabelSize {
		return ErrLabelTooLong
	}
	if strings.Contains(new, `"`) {
		return ErrInvalidLabel
	}
	if old == new {
		return nil
	}

	if err := db.blockWrite(); err != nil {
		return err
	}
	defer func() {
		db.mu.Unlock()
		db.lock.Unlock()
	}()

	sz, err := size(db.reader)
	if err != nil {
		return fmt.Errorf("rename: stat: %w", err)
	}

	// Find old document's index.
	oldID := hash(old, db.header.Algorithm)
	idxResult, idx, err := db.findIndex(oldID, old, sz)
	if err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	if idxResult == nil {
		return ErrNotFound
	}

	// Ensure new label doesn't already exist.
	newID := hash(new, db.header.Algorithm)
	newResult, _, err := db.findIndex(newID, new, sz)
	if err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	if newResult != nil {
		return ErrExists
	}

	// Same-length labels: patch _id and _l in place.
	if len(old) == len(new) {
		return db.patchRename(idx.Offset, idxResult.Offset, newID, new)
	}

	// Different-length: append new record+index, blank old.
	content, err := line(db.reader, idx.Offset)
	if err != nil {
		return fmt.Errorf("rename: read record: %w", err)
	}
	record, err := decode(content)
	if err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	ts := now()
	newRecord := &Record{
		Type:      TypeRecord,
		ID:        newID,
		Label:     new,
		Timestamp: ts,
		Data:      record.Data,
		History:   compress([]byte(record.Data)),
	}
	newIndex := &Index{
		Type:      TypeIndex,
		ID:        newID,
		Label:     new,
		Timestamp: ts,
	}

	if _, err := db.append(newRecord, newIndex); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	if db.bloom != nil {
		db.bloom.Add(newID)
	}

	if err := blank(db, idx.Offset, idxResult); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// findIndex locates the current index record for a label. Returns nil
// Result if the document doesn't exist.
func (db *DB) findIndex(id, label string, sz int64) (*Result, *Index, error) {
	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return nil, nil, err
		}
		if idx.Label == label {
			return result, idx, nil
		}
	}

	results := sparse(db.reader, id, db.sparseStart(), sz, TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, err := decodeIndex(results[i].Data)
		if err != nil {
			return nil, nil, err
		}
		if idx.Label == label {
			r := results[i]
			return &r, idx, nil
		}
	}

	return nil, nil, nil
}

// patchRename patches _id and _l in the data record at dataOff and the
// index record at idxOff. Only valid when old and new labels have the
// same byte length.
func (db *DB) patchRename(dataOff, idxOff int64, newID, newLabel string) error {
	marker := []byte(`"_l":"`)

	// Patch data record: _id then _l.
	if err := db.writeAt(dataOff+IDStart, []byte(newID)); err != nil {
		return fmt.Errorf("rename: patch data id: %w", err)
	}
	record, err := line(db.reader, dataOff)
	if err != nil {
		return fmt.Errorf("rename: read data: %w", err)
	}
	lPos := bytes.Index(record, marker)
	if lPos >= 0 {
		if err := db.writeAt(dataOff+int64(lPos+len(marker)), []byte(newLabel)); err != nil {
			return fmt.Errorf("rename: patch data label: %w", err)
		}
	}

	// Patch index record: _id then _l.
	if err := db.writeAt(idxOff+IDStart, []byte(newID)); err != nil {
		return fmt.Errorf("rename: patch index id: %w", err)
	}
	idxLine, err := line(db.reader, idxOff)
	if err != nil {
		return fmt.Errorf("rename: read index: %w", err)
	}
	lPos = bytes.Index(idxLine, marker)
	if lPos >= 0 {
		if err := db.writeAt(idxOff+int64(lPos+len(marker)), []byte(newLabel)); err != nil {
			return fmt.Errorf("rename: patch index label: %w", err)
		}
	}

	if db.bloom != nil {
		db.bloom.Add(newID)
	}
	return nil
}
