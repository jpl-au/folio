// Label enumeration across the entire file.
package folio

import (
	"bufio"
	"fmt"
	"io"
	"iter"
)

// List yields labels for all current documents. It scans the entire file
// (both sorted and sparse regions) for index records because a document
// may only exist in the sparse region if it was created since the last
// compaction. Labels are deduplicated but not sorted. Callers consume
// results lazily via range and can break early to stop the scan.
func (db *DB) List() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if err := db.blockRead(); err != nil {
			yield("", err)
			return
		}
		defer func() {
			db.mu.RUnlock()
			db.lock.Unlock()
		}()

		sz, err := size(db.reader)
		if err != nil {
			yield("", fmt.Errorf("list: stat: %w", err))
			return
		}

		seen := make(map[string]bool)

		section := io.NewSectionReader(db.reader, HeaderSize, sz-HeaderSize)
		scanner := bufio.NewScanner(section)
		scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

		for scanner.Scan() {
			data := scanner.Bytes()

			if valid(data) && len(data) >= MinRecordSize && data[TypePos] == byte('0'+TypeIndex) {
				lbl := label(data)
				if !seen[lbl] {
					seen[lbl] = true
					if !yield(lbl, nil) {
						return
					}
				}
			}
		}

		if err := scanner.Err(); err != nil {
			yield("", err)
		}
	}
}
