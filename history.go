// Version history retrieval from compressed _h snapshots.
//
// Both current Records (idx=2) and retired History records (idx=3) carry a
// compressed snapshot in _h. History collects all of them for a given label,
// decompresses each, and returns them in chronological write order.
//
// After compaction, all versions of a document are contiguous in the heap
// (sorted by ID then timestamp). History uses group() to binary-search the
// heap for the ID group, then linearly scans the sparse region for any
// records appended since the last compaction.
package folio

import (
	"cmp"
	"fmt"
	"slices"
)

// Version is a single point-in-time snapshot of a document's content.
type Version struct {
	Data string
	TS   int64
}

// History returns every version of a document in chronological order.
// It searches the heap via binary search (O(log n) + group size), then
// scans the sparse region for records appended since the last compaction.
func (db *DB) History(label string) ([]Version, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	id := hash(label, db.header.Algorithm)

	sz, err := size(db.reader)
	if err != nil {
		return nil, fmt.Errorf("history: stat: %w", err)
	}

	type versionWithOffset struct {
		Version
		offset int64
	}
	var versions []versionWithOffset

	// Heap: binary search for the ID group, collect all contiguous records.
	heapResults := group(db.reader, id, HeaderSize, db.heapEnd())

	// Sparse: linear scan for matching records of any data/history type.
	for _, t := range []int{TypeRecord, TypeHistory} {
		sparseResults := sparse(db.reader, id, db.sparseStart(), sz, t)
		heapResults = append(heapResults, sparseResults...)
	}

	for _, result := range heapResults {
		record, err := decode(result.Data)
		if err != nil {
			return nil, fmt.Errorf("history: %w", err)
		}
		if record.Type != TypeRecord && record.Type != TypeHistory {
			continue
		}
		if record.Label != label {
			continue
		}
		content, err := decompress(record.History)
		if err != nil {
			return nil, fmt.Errorf("history: %w", err)
		}
		versions = append(versions, versionWithOffset{
			Version: Version{string(content), record.Timestamp},
			offset:  result.Offset,
		})
	}

	// File offset reflects write order, which is the true chronology
	slices.SortFunc(versions, func(a, b versionWithOffset) int {
		return cmp.Compare(a.offset, b.offset)
	})

	out := make([]Version, len(versions))
	for i, v := range versions {
		out[i] = v.Version
	}
	return out, nil
}
