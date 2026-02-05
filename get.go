// Document retrieval operations.
package folio

// Get retrieves the current content of a document.
func (db *DB) Get(label string) (string, error) {
	if err := db.blockRead(); err != nil {
		return "", err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	// Binary search sorted index
	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			content, _ := line(db.reader, idx.Offset)
			record, _ := decode(content)
			return record.Data, nil
		}
	}

	// Linear scan sparse (reverse for newest)
	results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, _ := decodeIndex(results[i].Data)
		if idx.Label == label {
			content, _ := line(db.reader, idx.Offset)
			record, _ := decode(content)
			return record.Data, nil
		}
	}

	return "", ErrNotFound
}

// Exists checks if a document exists.
func (db *DB) Exists(label string) (bool, error) {
	if err := db.blockRead(); err != nil {
		return false, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	result := scan(db.reader, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, _ := decodeIndex(result.Data)
		if idx.Label == label {
			return true, nil
		}
	}

	results := sparse(db.reader, id, db.sparseStart(), size(db.reader), TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, _ := decodeIndex(results[i].Data)
		if idx.Label == label {
			return true, nil
		}
	}

	return false, nil
}
