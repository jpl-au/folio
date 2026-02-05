// Scan operations for finding records in the database file.
//
// Binary search (scan) works on sorted sections. Linear search (sparse)
// works on unsorted append-only sections. Minimal scan (scanm) extracts
// only metadata for compaction without full JSON parsing.
package folio

import (
	"bufio"
	"cmp"
	"io"
	"os"
	"slices"
	"strconv"
)

// scan performs binary search on a sorted section for a record matching id and type.
// Returns nil if not found.
func scan(f *os.File, id string, start, end int64, recordType int) *Result {
	if start >= end {
		return nil
	}

	mid := (start + end) / 2

	// Find a valid record to use as pivot
	var pivot *Result
	var pivotEnd int64

	// Try forward first - find record starting after mid
	newlinePos, _ := align(f, mid)
	if newlinePos >= 0 && newlinePos+1 < end {
		recordStart := newlinePos + 1
		data, err := line(f, recordStart)
		if err == nil && len(data) > 0 && valid(data) {
			if len(data) >= MinRecordSize && data[7] == byte('0'+recordType) {
				id := string(data[16:32])
				pivot = &Result{recordStart, len(data), data, id}
				pivotEnd = recordStart + int64(len(data)) + 1
			}
		}
	}

	// If no valid forward pivot, try backward
	if pivot == nil {
		pivot = scanBack(f, mid, start, recordType)
		if pivot != nil {
			pivotEnd = pivot.Offset + int64(pivot.Length) + 1
		}
	}

	// No valid records in range
	if pivot == nil {
		return nil
	}

	if id == pivot.ID {
		return pivot
	}
	if id < pivot.ID {
		return scan(f, id, start, pivot.Offset, recordType)
	}
	return scan(f, id, pivotEnd, end, recordType)
}

// scanBack scans backwards from pos to find the first valid record of the given type.
func scanBack(f *os.File, pos, start int64, recordType int) *Result {
	for pos > start {
		pos--
		// Find previous newline
		for pos > start {
			buf := make([]byte, 1)
			f.ReadAt(buf, pos)
			if buf[0] == '\n' {
				break
			}
			pos--
		}

		recordStart := pos + 1
		if pos == start {
			recordStart = start
		}

		data, err := line(f, recordStart)
		if err != nil || !valid(data) {
			continue
		}

		if len(data) >= MinRecordSize && data[7] == byte('0'+recordType) {
			id := string(data[16:32])
			return &Result{recordStart, len(data), data, id}
		}
	}
	return nil
}

// scanFwd scans forwards from pos to find the first valid record of the given type.
func scanFwd(f *os.File, pos, end int64, recordType int) *Result {
	for pos < end {
		data, err := line(f, pos)
		if err != nil || len(data) == 0 {
			break
		}

		if valid(data) {
			if len(data) >= MinRecordSize && data[7] == byte('0'+recordType) {
				id := string(data[16:32])
				return &Result{pos, len(data), data, id}
			}
		}

		pos += int64(len(data)) + 1 // +1 for newline
	}
	return nil
}

// sparse performs linear scan for records matching id and type in an unsorted section.
// If id is empty, returns all records of the given type.
func sparse(f *os.File, id string, start, end int64, recordType int) []Result {
	var results []Result

	section := io.NewSectionReader(f, start, end-start)
	scanner := bufio.NewScanner(section)
	scanner.Buffer(make([]byte, 64*1024), MaxRecordSize)
	offset := start

	for scanner.Scan() {
		data := scanner.Bytes()
		length := len(data)

		if valid(data) {
			record, err := decode(data)
			if err == nil && record.Type == recordType {
				if id == "" || record.ID == id {
					// Copy data since scanner reuses buffer
					dataCopy := make([]byte, length)
					copy(dataCopy, data)
					results = append(results, Result{offset, length, dataCopy, record.ID})
				}
			}
		}

		offset += int64(length) + 1 // +1 for newline
	}

	return results
}

// scanm performs minimal scan extracting only metadata without full JSON parsing.
// Used by compaction. Pass recordType=0 to get all types.
func scanm(f *os.File, start, end int64, recordType int) []Entry {
	var entries []Entry

	section := io.NewSectionReader(f, start, end-start)
	scanner := bufio.NewScanner(section)
	scanner.Buffer(make([]byte, 64*1024), MaxRecordSize)
	offset := start

	for scanner.Scan() {
		ln := scanner.Bytes()
		length := len(ln)

		if valid(ln) && length >= MinRecordSize {
			// Fixed positions: {"idx":N,"_id":"XXXXXXXXXXXXXXXX","_ts":NNNNNNNNNNNNN
			t := int(ln[7] - '0')
			if recordType == 0 || t == recordType {
				id := string(ln[16:32])
				ts, _ := strconv.ParseInt(string(ln[40:53]), 10, 64)
				lbl := ""
				if t == TypeIndex {
					lbl = label(ln)
				}
				entries = append(entries, Entry{id, ts, t, offset, 0, length, lbl})
			}
		}

		offset += int64(length) + 1
	}

	return entries
}

// unpack separates entries into data and index slices for compaction.
// The exclude slice specifies record types to omit from the data slice.
func unpack(entries []Entry, exclude ...int) (data, indexes []Entry) {
	for _, e := range entries {
		if e.Type == TypeIndex {
			indexes = append(indexes, e)
		} else if !slices.Contains(exclude, e.Type) {
			data = append(data, e)
		}
	}
	return data, indexes
}

// byIDThenTS is a comparator for sorting entries by ID, then by timestamp (older first).
func byIDThenTS(a, b Entry) int {
	if c := cmp.Compare(a.ID, b.ID); c != 0 {
		return c
	}
	return cmp.Compare(a.TS, b.TS)
}

// byID is a comparator for sorting entries by ID only.
func byID(a, b *Entry) int {
	return cmp.Compare(a.ID, b.ID)
}
