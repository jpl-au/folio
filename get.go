// Document retrieval using the two-region lookup strategy.
//
// Lookups check the sorted index section first (binary search, O(log n)),
// then fall back to the sparse region (linear scan) for records written
// since the last compaction. The optional bloom filter can skip the sparse
// scan entirely when an ID is definitively absent.
package folio

import "fmt"

// Get returns the current content of a document identified by label.
// The lookup follows the index (not the data records directly) because
// the index is smaller and faster to binary search, then a single seek
// to the data record's offset retrieves the content.
func (db *DB) Get(label string) (string, error) {
	if err := db.blockRead(); err != nil {
		return "", err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)
	s := db.src()

	// In-memory index — O(1) lookup
	if db.index != nil {
		if off, ok := db.index[id]; ok {
			data, err := line(s, off)
			if err != nil {
				return "", fmt.Errorf("get: read index: %w", err)
			}
			idx, err := decodeIndex(data)
			if err != nil {
				return "", fmt.Errorf("get: %w", err)
			}
			if idx.Label == label {
				content, err := line(s, idx.Offset)
				if err != nil {
					return "", fmt.Errorf("get: read record: %w", err)
				}
				record, err := decode(content)
				if err != nil {
					return "", fmt.Errorf("get: %w", err)
				}
				return record.Data, nil
			}
		}
		return "", ErrNotFound
	}

	// Sorted index section — fast path after compaction
	result := scan(s, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return "", fmt.Errorf("get: %w", err)
		}
		if idx.Label == label {
			content, err := line(s, idx.Offset)
			if err != nil {
				return "", fmt.Errorf("get: read record: %w", err)
			}
			record, err := decode(content)
			if err != nil {
				return "", fmt.Errorf("get: %w", err)
			}
			return record.Data, nil
		}
	}

	if db.bloom != nil && !db.bloom.Contains(id) {
		return "", ErrNotFound
	}

	// Sparse region — reverse scan so the newest matching index wins
	results := sparse(s, id, db.sparseStart(), s.sz, TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, err := decodeIndex(results[i].Data)
		if err != nil {
			return "", fmt.Errorf("get: %w", err)
		}
		if idx.Label == label {
			content, err := line(s, idx.Offset)
			if err != nil {
				return "", fmt.Errorf("get: read record: %w", err)
			}
			record, err := decode(content)
			if err != nil {
				return "", fmt.Errorf("get: %w", err)
			}
			return record.Data, nil
		}
	}

	return "", ErrNotFound
}

// Exists performs the same two-region lookup as Get but returns as soon
// as a matching index is found, without reading the data record.
func (db *DB) Exists(label string) (bool, error) {
	if err := db.blockRead(); err != nil {
		return false, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)
	s := db.src()

	// In-memory index — single read to verify label on collision.
	if db.index != nil {
		off, ok := db.index[id]
		if !ok {
			return false, nil
		}
		data, err := line(s, off)
		if err != nil {
			return false, fmt.Errorf("exists: read index: %w", err)
		}
		idx, err := decodeIndex(data)
		if err != nil {
			return false, fmt.Errorf("exists: %w", err)
		}
		return idx.Label == label, nil
	}

	result := scan(s, id, db.indexStart(), db.indexEnd(), TypeIndex)
	if result != nil {
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return false, fmt.Errorf("exists: %w", err)
		}
		if idx.Label == label {
			return true, nil
		}
	}

	if db.bloom != nil && !db.bloom.Contains(id) {
		return false, nil
	}

	results := sparse(s, id, db.sparseStart(), s.sz, TypeIndex)
	for i := len(results) - 1; i >= 0; i-- {
		idx, err := decodeIndex(results[i].Data)
		if err != nil {
			return false, fmt.Errorf("exists: %w", err)
		}
		if idx.Label == label {
			return true, nil
		}
	}

	return false, nil
}
