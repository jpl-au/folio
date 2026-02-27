// Document creation and update using append-then-blank.
//
// A Set always appends a new Record+Index pair at the tail. If an older
// version exists, it is then patched in place: its type byte is changed
// from Record (2) to History (3), its _d content is blanked with spaces,
// and its index line is overwritten with spaces. The compressed snapshot
// in _h is preserved so History can still retrieve the old content.
// This approach avoids rewriting the file on every update while keeping
// the latest version immediately accessible via the newest index.
//
// Batch amortises lock acquisition across multiple documents. All
// inputs are validated before any writes begin — if validation fails,
// no documents are written.
package folio

import (
	"fmt"
	"strings"
)

// Set creates or updates a document. See the package comment for the
// append-then-blank strategy.
func (db *DB) Set(label, content string) error {
	if err := validateDoc(label, content); err != nil {
		return err
	}

	if err := db.blockWrite(); err != nil {
		return err
	}
	defer func() {
		db.mu.Unlock()
		db.lock.Unlock()
	}()

	return db.setOne(label, content)
}

// Batch creates or updates multiple documents under a single lock
// hold. All inputs are validated before any writes begin. Documents
// are processed in slice order.
func (db *DB) Batch(docs ...Document) error {
	for _, d := range docs {
		if err := validateDoc(d.Label, d.Data); err != nil {
			return err
		}
	}

	if err := db.blockWrite(); err != nil {
		return err
	}
	defer func() {
		db.mu.Unlock()
		db.lock.Unlock()
	}()

	for _, d := range docs {
		if err := db.setOne(d.Label, d.Data); err != nil {
			return err
		}
	}
	return nil
}

// validateDoc checks label and content constraints before any write.
func validateDoc(label, content string) error {
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
	return nil
}

// setOne writes a single document. The write lock must be held.
func (db *DB) setOne(label, content string) error {
	id := hash(label, db.header.Algorithm)

	sz, err := size(db.reader)
	if err != nil {
		return fmt.Errorf("set: stat: %w", err)
	}

	idxResult, idx, err := db.findIndex(id, label, sz)
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}

	ts := now()
	newRecord := &Record{
		Type:      TypeRecord,
		ID:        id,
		Label:     label,
		Timestamp: ts,
		Data:      content,
		History:   compress([]byte(content)),
	}

	newIndex := &Index{
		Type:      TypeIndex,
		ID:        id,
		Label:     label,
		Timestamp: ts,
	}

	if _, err := db.append(newRecord, newIndex); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	if db.bloom != nil {
		db.bloom.Add(id)
	}

	if idxResult == nil {
		db.count.Add(1)
	}

	// Retire the previous version.
	if idxResult != nil {
		if err := blank(db, idx.Offset, idxResult); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}

	return nil
}
