// Regex search over document content and labels.
//
// Search scans data records (idx=2) and matches against the _d field.
// MatchLabel scans index records (idx=1) and matches against _l.
// Both stream through the file line-by-line to avoid loading it into memory.
package folio

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
)

type SearchOptions struct {
	CaseSensitive bool
	Limit         int
	Decode        bool // unescape JSON string escapes in _d before matching
}

type Match struct {
	Label  string
	Offset int64
}

// Search matches a regex against the _d field of current data records.
// Content is extracted by byte-scanning for the _d and _h field delimiters
// rather than JSON-parsing each line, keeping memory proportional to the
// read buffer rather than the largest record.
func (db *DB) Search(pattern string, opts SearchOptions) ([]Match, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	if !opts.CaseSensitive {
		pattern = "(?i)" + pattern
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, ErrInvalidPattern
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
					if opts.Decode {
						content = unescape(content)
					}
					if re.Match(content) {
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
