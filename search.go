// Search operations for content and label matching.
package folio

import (
	"bufio"
	"bytes"
	"io"
	"regexp"
)

// SearchOptions configures search behaviour.
type SearchOptions struct {
	CaseSensitive bool
	Limit         int
	Decode        bool // true = unescape _d content before matching
}

// Match represents a search result.
type Match struct {
	Label  string
	Offset int64
}

// Search performs regex search within document content (_d fields only).
// Only current data records (idx=2) are searched.
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

	section := io.NewSectionReader(db.reader, HeaderSize, size(db.reader)-HeaderSize)
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

// MatchLabel performs regex search on document labels.
func (db *DB) MatchLabel(pattern string) ([]Match, error) {
	if err := db.blockRead(); err != nil {
		return nil, err
	}
	defer func() {
		db.mu.RUnlock()
		db.lock.Unlock()
	}()

	// Regex to match label within an Index record
	// Expects format: {"idx":1...,"_l":"...pattern..."
	fullPattern := `(?i){"idx":1.*"_l":"[^"]*` + pattern + `[^"]*"`
	re, err := regexp.Compile(fullPattern)
	if err != nil {
		return nil, ErrInvalidPattern
	}

	section := io.NewSectionReader(db.reader, 0, size(db.reader))
	scanner := bufio.NewScanner(section)
	scanner.Buffer(make([]byte, db.config.ReadBuffer), db.config.MaxRecordSize)

	var results []Match
	var offset int64

	for scanner.Scan() {
		line := scanner.Bytes()

		// Optimization: Only check lines that look like Index records
		// TypeIndex is 1, so {"idx":1
		if len(line) > 8 && line[7] == '1' {
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
