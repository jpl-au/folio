// Full document enumeration in a single pass.
//
// All scans data records (_r=2) across the heap and sparse regions,
// extracting label and content by byte scanning. Unlike the List+Get
// pattern, it never follows index pointers — content is read directly
// from the data record, avoiding per-document seek overhead.
//
// The scan uses the same sorted-region awareness as Search: only the
// heap and sparse regions are visited, skipping the index section
// entirely. Records retired by Set or Delete have their type byte
// patched from 2 to 3 (history), so the type check at TypePos
// naturally excludes them.
package folio

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"
)

// Document is a label–content pair yielded by All.
type Document struct {
	Label string
	Data  string
}

// All yields every current document as a label–content pair. It scans
// data records directly, avoiding the N+1 cost of List followed by
// Get for each label. Callers consume results lazily via range and
// can break early to stop the scan.
func (db *DB) All() iter.Seq2[Document, error] {
	return func(yield func(Document, error) bool) {
		if err := db.blockRead(); err != nil {
			yield(Document{}, err)
			return
		}
		defer func() {
			db.mu.RUnlock()
			db.lock.Unlock()
		}()

		sz, err := size(db.reader)
		if err != nil {
			yield(Document{}, fmt.Errorf("all: stat: %w", err))
			return
		}

		dTag := []byte(`"_d":"`)
		hTag := []byte(`","_h":"`)
		seen := make(map[string]bool)

		// scanRegion scans [start, end) for data records, extracting
		// label and content. Returns false if the caller broke out.
		scanRegion := func(start, end int64) bool {
			if start >= end {
				return true
			}
			section := io.NewSectionReader(db.reader, start, end-start)
			scanner := bufio.NewScanner(section)
			scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

			for scanner.Scan() {
				ln := scanner.Bytes()

				if valid(ln) && len(ln) >= MinRecordSize && ln[TypePos] == byte('0'+TypeRecord) {
					lbl := label(ln)
					if lbl != "" && !seen[lbl] {
						seen[lbl] = true
						di := bytes.Index(ln, dTag)
						if di >= 0 {
							s := di + len(dTag)
							hi := bytes.Index(ln[s:], hTag)
							if hi >= 0 {
								content := string(unescape(ln[s : s+hi]))
								if !yield(Document{Label: lbl, Data: content}, nil) {
									return false
								}
							}
						}
					}
				}
			}

			if err := scanner.Err(); err != nil {
				yield(Document{}, err)
				return false
			}
			return true
		}

		// Heap: data records. Skip the index section.
		if !scanRegion(HeaderSize, db.heapEnd()) {
			return
		}
		// Sparse: unsorted appends since last compaction.
		scanRegion(db.sparseStart(), sz)
	}
}
