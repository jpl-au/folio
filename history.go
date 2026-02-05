// Version history retrieval.
package folio

import (
	"cmp"
	"slices"
)

// Version represents a historical version of a document.
type Version struct {
	Data string
	TS   int64
}

// History retrieves all versions of a document.
func (db *DB) History(label string) ([]Version, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	// Collect with offset for stable ordering
	type versionWithOffset struct {
		Version
		offset int64
	}
	var versions []versionWithOffset

	for _, t := range []int{TypeRecord, TypeHistory} {
		results := sparse(db.reader, id, HeaderSize, size(db.reader), t)
		for _, result := range results {
			record, _ := decode(result.Data)
			if record.Label != label {
				continue
			}
			content := decompress(record.History)
			versions = append(versions, versionWithOffset{
				Version: Version{string(content), record.Timestamp},
				offset:  result.Offset,
			})
		}
	}

	// Sort by file offset (chronological write order)
	slices.SortFunc(versions, func(a, b versionWithOffset) int {
		return cmp.Compare(a.offset, b.offset)
	})

	out := make([]Version, len(versions))
	for i, v := range versions {
		out[i] = v.Version
	}
	return out, nil
}
