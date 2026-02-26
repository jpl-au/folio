// Search over document content and labels.
//
// Search scans data records (idx=2) and matches against the _d field.
// Literal patterns (no regex metacharacters) take a fast path: the query
// is JSON-escaped and matched with bytes.Contains, avoiding both regex
// overhead and the need to unescape record content. Patterns containing
// regex metacharacters fall back to regexp.Match with optional decode.
//
// The literal path works by escaping the search term into the same JSON
// representation used on disk (via json.Marshal), then matching raw bytes
// directly. This avoids per-record unescape overhead entirely. However,
// it assumes the on-disk encoding matches what json.Marshal produces. If
// Decode is set, the caller explicitly wants unescape-then-match semantics
// (e.g. to handle non-standard encodings like \u0041 for 'A'), so the
// literal path is bypassed to guarantee equivalent results.
//
// Case-insensitive literal search uses bytes.ToLower on both needle and
// content. This allocates a copy of the _d slice per record. A zero-alloc
// alternative (sliding bytes.EqualFold) would trade O(n) for O(n*m) but
// eliminate GC pressure. We keep ToLower for now because search terms are
// typically short and the allocation is bounded to the _d field, not the
// full record line. Revisit if profiling shows GC pressure from search.
//
// MatchLabel scans index records (idx=1) and matches against _l.
// Both stream through the file line-by-line to avoid loading it into memory.
// Callers consume results lazily via range and can break early to stop the
// scan without reading the rest of the file.
package folio

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"
	"regexp"

	json "github.com/goccy/go-json"
)

// SearchOptions configures Search behaviour. Callers control result count
// by breaking out of the range loop — no Limit field is needed.
type SearchOptions struct {
	CaseSensitive bool
	Decode        bool // unescape JSON string escapes in _d before matching; bypasses literal fast path
}

// Match is a single search result: a label and the byte offset of the
// matching record in the file.
type Match struct {
	Label  string
	Offset int64
}

// Search matches a pattern against the _d field of current data records.
// Results are yielded lazily; break from the range loop to stop early.
func (db *DB) Search(pattern string, opts SearchOptions) iter.Seq2[Match, error] {
	return func(yield func(Match, error) bool) {
		if err := db.blockRead(); err != nil {
			yield(Match{}, err)
			return
		}
		defer func() {
			db.mu.RUnlock()
			db.lock.Unlock()
		}()

		var match func([]byte) bool
		var decode bool

		if !opts.Decode && regexp.QuoteMeta(pattern) == pattern {
			raw, _ := json.Marshal(pattern)
			needle := raw[1 : len(raw)-1]
			if opts.CaseSensitive {
				match = func(content []byte) bool {
					return bytes.Contains(content, needle)
				}
			} else {
				lower := bytes.ToLower(needle)
				match = func(content []byte) bool {
					return bytes.Contains(bytes.ToLower(content), lower)
				}
			}
		} else {
			if !opts.CaseSensitive {
				pattern = "(?i)" + pattern
			}
			re, err := regexp.Compile(pattern)
			if err != nil {
				yield(Match{}, ErrInvalidPattern)
				return
			}
			match = re.Match
			decode = opts.Decode
		}

		sz, err := size(db.reader)
		if err != nil {
			yield(Match{}, fmt.Errorf("search: stat: %w", err))
			return
		}
		section := io.NewSectionReader(db.reader, HeaderSize, sz-HeaderSize)
		scanner := bufio.NewScanner(section)
		scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

		offset := int64(HeaderSize)

		dTag := []byte(`"_d":"`)
		hTag := []byte(`","_h":"`)

		for scanner.Scan() {
			ln := scanner.Bytes()

			if valid(ln) && len(ln) >= MinRecordSize && ln[7] == byte('0'+TypeRecord) {
				di := bytes.Index(ln, dTag)
				if di >= 0 {
					start := di + len(dTag)
					hi := bytes.Index(ln[start:], hTag)
					if hi >= 0 {
						content := ln[start : start+hi]
						if decode {
							content = unescape(content)
						}
						if match(content) {
							if !yield(Match{Label: label(ln), Offset: offset}, nil) {
								return
							}
						}
					}
				}
			}

			offset += int64(len(ln)) + 1
		}

		if err := scanner.Err(); err != nil {
			yield(Match{}, err)
		}
	}
}

// MatchLabel matches a regex against the _l field of index records.
// Only index lines (idx=1) are checked, so the scan skips data records
// entirely using the type byte at position 7. Results are yielded lazily.
func (db *DB) MatchLabel(pattern string) iter.Seq2[Match, error] {
	return func(yield func(Match, error) bool) {
		if err := db.blockRead(); err != nil {
			yield(Match{}, err)
			return
		}
		defer func() {
			db.mu.RUnlock()
			db.lock.Unlock()
		}()

		fullPattern := `(?i){"idx":1.*"_l":"[^"]*` + pattern + `[^"]*"`
		re, err := regexp.Compile(fullPattern)
		if err != nil {
			yield(Match{}, ErrInvalidPattern)
			return
		}

		sz, err := size(db.reader)
		if err != nil {
			yield(Match{}, fmt.Errorf("matchlabel: stat: %w", err))
			return
		}
		section := io.NewSectionReader(db.reader, 0, sz)
		scanner := bufio.NewScanner(section)
		scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

		var offset int64

		for scanner.Scan() {
			line := scanner.Bytes()

			if len(line) > 8 && line[7] == '1' {
				loc := re.FindIndex(line)
				if loc != nil {
					lbl := label(line)
					if !yield(Match{Label: lbl, Offset: offset + int64(loc[0])}, nil) {
						return
					}
				}
			}

			offset += int64(len(line)) + 1
		}

		if err := scanner.Err(); err != nil {
			yield(Match{}, err)
		}
	}
}
