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
package folio

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"

	json "github.com/goccy/go-json"
)

type SearchOptions struct {
	CaseSensitive bool
	Limit         int
	Decode        bool // unescape JSON string escapes in _d before matching; bypasses literal fast path
}

type Match struct {
	Label  string
	Offset int64
}

// Search matches a pattern against the _d field of current data records.
func (db *DB) Search(pattern string, opts SearchOptions) ([]Match, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	var match func([]byte) bool
	var decode bool

	if !opts.Decode && regexp.QuoteMeta(pattern) == pattern {
		// Literal fast path: JSON-escape the query to match raw _d bytes.
		// Skipped when Decode is set because the caller wants full unescape
		// semantics, which may resolve non-standard sequences (e.g. \u0041)
		// that the escaped-needle approach would miss.
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
		// Regex path: used for patterns with metacharacters or when Decode
		// is set (requiring content unescape before matching).
		if !opts.CaseSensitive {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, ErrInvalidPattern
		}
		match = re.Match
		decode = opts.Decode
	}

	sz, err := size(db.reader)
	if err != nil {
		return nil, fmt.Errorf("search: stat: %w", err)
	}
	section := io.NewSectionReader(db.reader, HeaderSize, sz-HeaderSize)
	scanner := bufio.NewScanner(section)
	scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

	var results []Match
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
						results = append(results, Match{Label: label(ln), Offset: offset})
						if opts.Limit > 0 && len(results) >= opts.Limit {
							break
						}
					}
				}
			}
		}

		offset += int64(len(ln)) + 1
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// MatchLabel matches a regex against the _l field of index records.
// Only index lines (idx=1) are checked, so the scan skips data records
// entirely using the type byte at position 7.
func (db *DB) MatchLabel(pattern string) ([]Match, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	fullPattern := `(?i){"idx":1.*"_l":"[^"]*` + pattern + `[^"]*"`
	re, err := regexp.Compile(fullPattern)
	if err != nil {
		return nil, ErrInvalidPattern
	}

	sz, err := size(db.reader)
	if err != nil {
		return nil, fmt.Errorf("matchlabel: stat: %w", err)
	}
	section := io.NewSectionReader(db.reader, 0, sz)
	scanner := bufio.NewScanner(section)
	scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

	var results []Match
	var offset int64

	for scanner.Scan() {
		line := scanner.Bytes()

		if len(line) > 8 && line[7] == '1' { // idx=1 → index record
			loc := re.FindIndex(line)
			if loc != nil {
				lbl := label(line)
				results = append(results, Match{Label: lbl, Offset: offset + int64(loc[0])})
			}
		}

		offset += int64(len(line)) + 1
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
