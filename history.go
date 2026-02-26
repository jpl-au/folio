// Version history retrieval from compressed _h snapshots.
//
// Both current Records (idx=2) and retired History records (idx=3) carry a
// compressed snapshot in _h. History collects all of them for a given label,
// decompresses each, and yields them in chronological write order.
//
// After compaction, all versions of a document are contiguous in the heap
// (sorted by ID then timestamp). History uses group() to binary-search the
// heap for the ID group, then linearly scans the sparse region for any
// records appended since the last compaction.
//
// Because results must be sorted by file offset (the ground truth for write
// order), all versions are collected and sorted before yielding. The
// iterator API provides consistency with Search, MatchLabel, and List even
// though this method buffers internally.
package folio

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
)

// Version is a single point-in-time snapshot of a document's content.
type Version struct {
	Data string
	TS   int64
}

// History yields every version of a document in chronological order.
// It searches the heap via binary search (O(log n) + group size), then
// scans the sparse region for records appended since the last compaction.
func (db *DB) History(label string) iter.Seq2[Version, error] {
	return func(yield func(Version, error) bool) {
		if err := db.blockRead(); err != nil {
			yield(Version{}, err)
			return
		}
		defer func() {
			db.mu.RUnlock()
			db.lock.Unlock()
		}()

		id := hash(label, db.header.Algorithm)

		sz, err := size(db.reader)
		if err != nil {
			yield(Version{}, fmt.Errorf("history: stat: %w", err))
			return
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
				yield(Version{}, fmt.Errorf("history: %w", err))
				return
			}
			if record.Type != TypeRecord && record.Type != TypeHistory {
				continue
			}
			if record.Label != label {
				continue
			}
			content, err := decompress(record.History)
			if err != nil {
				yield(Version{}, fmt.Errorf("history: %w", err))
				return
			}
			versions = append(versions, versionWithOffset{
				Version: Version{string(content), record.Timestamp},
				offset:  result.Offset,
			})
		}

		// Sort by file offset, not timestamp. Timestamps can collide (same
		// millisecond) but file offsets are strictly ordered — the append
		// position is the ground truth for write order. Do not "fix" this
		// to sort by timestamp; it would silently reorder concurrent writes.
		slices.SortFunc(versions, func(a, b versionWithOffset) int {
			return cmp.Compare(a.offset, b.offset)
		})

		for _, v := range versions {
			if !yield(v.Version, nil) {
				return
			}
		}
	}
}
