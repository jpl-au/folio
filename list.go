// Label enumeration across the entire file.
package folio

import "fmt"

// List returns labels for all current documents. It scans the entire file
// (both sorted and sparse regions) for index records because a document
// may only exist in the sparse region if it was created since the last
// compaction. Results are deduplicated but not sorted.
func (db *DB) List() ([]string, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	sz, err := size(db.reader)
	if err != nil {
		return nil, fmt.Errorf("list: stat: %w", err)
	}

	seen := make(map[string]bool)
	var labels []string

	results := sparse(db.reader, "", HeaderSize, sz, TypeIndex)
	for _, result := range results {
		idx, err := decodeIndex(result.Data)
		if err != nil {
			return nil, fmt.Errorf("list: %w", err)
		}
		if !seen[idx.Label] {
			seen[idx.Label] = true
			labels = append(labels, idx.Label)
		}
	}

	return labels, nil
}
