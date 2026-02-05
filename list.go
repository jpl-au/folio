// Document label enumeration.
package folio

// List returns all document labels.
func (db *DB) List() ([]string, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	seen := make(map[string]bool)
	var labels []string

	results := sparse(db.reader, "", HeaderSize, size(db.reader), TypeIndex)
	for _, result := range results {
		idx, _ := decodeIndex(result.Data)
		if !seen[idx.Label] {
			seen[idx.Label] = true
			labels = append(labels, idx.Label)
		}
	}

	return labels, nil
}
