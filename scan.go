// Scan strategies for the two-region file layout.
//
// After compaction the file contains a sorted heap (data + history
// interleaved by ID then timestamp) followed by sorted indexes. New writes
// are appended after the indexes into a sparse (unsorted) region. Lookups
// therefore need two strategies:
//
//   - scan: binary search over a sorted section. O(log n) seeks.
//     Pass recordType=0 to match any type (type-agnostic).
//   - sparse: linear scan over the unsorted region. O(n) but bounded to
//     records written since the last compaction.
//
// scanm is a compaction-only variant that extracts metadata at fixed byte
// positions without JSON parsing, since compaction must touch every record
// but only needs ID, type, timestamp, and label.
package folio

import (
	"bufio"
	"cmp"
	"io"
	"os"
	"slices"
	"strconv"
)

// scan performs binary search between start and end for a record whose ID
// matches id. Because records are variable-length, the midpoint may land
// inside a record, so we align to the nearest newline to find a valid pivot.
// If the forward alignment fails (e.g. lands past end), we fall back to
// scanning backwards for a pivot.
func scan(f *os.File, id string, start, end int64, recordType int) *Result {
	if start >= end {
		return nil
	}

	mid := (start + end) / 2

	// Find a valid record to use as pivot
	var pivot *Result
	var pivotEnd int64

	newlinePos, _ := align(f, mid)
	if newlinePos >= 0 && newlinePos+1 < end {
		recordStart := newlinePos + 1
		data, err := line(f, recordStart)
		if err == nil && len(data) > 0 && valid(data) {
			if len(data) >= MinRecordSize && (recordType == 0 || data[7] == byte('0'+recordType)) {
				id := string(data[16:32])
				pivot = &Result{recordStart, len(data), data, id}
				pivotEnd = recordStart + int64(len(data)) + 1
			}
		}
	}

	if pivot == nil {
		pivot = scanBack(f, mid, start, recordType)
		if pivot != nil {
			pivotEnd = pivot.Offset + int64(pivot.Length) + 1
		}
	}

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

// scanBack walks backwards byte-by-byte to find a valid pivot when the
// forward alignment in scan lands outside the search range.
func scanBack(f *os.File, pos, start int64, recordType int) *Result {
	var buf [1]byte
	for pos > start {
		pos--
		for pos > start {
			if _, err := f.ReadAt(buf[:], pos); err != nil {
				return nil
			}
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

		if len(data) >= MinRecordSize && (recordType == 0 || data[7] == byte('0'+recordType)) {
			id := string(data[16:32])
			return &Result{recordStart, len(data), data, id}
		}
	}
	return nil
}

// scanFwd walks forward line-by-line. Used when we need the first record
// of a given type in a region (e.g. finding the start of the index section).
func scanFwd(f *os.File, pos, end int64, recordType int) *Result {
	for pos < end {
		data, err := line(f, pos)
		if err != nil || len(data) == 0 {
			break
		}

		if valid(data) {
			if len(data) >= MinRecordSize && (recordType == 0 || data[7] == byte('0'+recordType)) {
				id := string(data[16:32])
				return &Result{pos, len(data), data, id}
			}
		}

		pos += int64(len(data)) + 1 // +1 for newline
	}
	return nil
}

// group binary-searches a sorted region for any record with the given ID
// (type-agnostic), then forward-scans to collect all contiguous records
// sharing that ID. Returns them in file order (oldest first after
// compaction). Used by History to collect all versions from the heap.
func group(f *os.File, id string, start, end int64) []Result {
	if start >= end {
		return nil
	}

	hit := scan(f, id, start, end, 0)
	if hit == nil {
		return nil
	}

	// Walk backwards from the hit to find the first record in this ID group.
	first := hit.Offset
	for first > start {
		// Find previous newline
		prev := first - 1
		var buf [1]byte
		for prev > start {
			if _, err := f.ReadAt(buf[:], prev-1); err != nil {
				break
			}
			if buf[0] == '\n' {
				break
			}
			prev--
		}
		recordStart := prev
		if prev > start {
			recordStart = prev // byte after newline
		}

		data, err := line(f, recordStart)
		if err != nil || !valid(data) || len(data) < MinRecordSize {
			break
		}
		rid := string(data[16:32])
		if rid != id {
			break
		}
		first = recordStart
	}

	// Forward-scan from first, collecting all records with this ID.
	var results []Result
	pos := first
	for pos < end {
		data, err := line(f, pos)
		if err != nil || len(data) == 0 {
			break
		}
		if !valid(data) || len(data) < MinRecordSize {
			pos += int64(len(data)) + 1
			continue
		}
		rid := string(data[16:32])
		if rid != id {
			break
		}
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		results = append(results, Result{pos, len(data), dataCopy, rid})
		pos += int64(len(data)) + 1
	}

	return results
}

// sparse linearly scans an unsorted region. Every record is JSON-parsed
// because IDs are not in sorted order — there is no way to short-circuit.
// Pass an empty id to collect all records of the given type (used by List).
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
					dataCopy := make([]byte, length) // scanner reuses its buffer
					copy(dataCopy, data)
					results = append(results, Result{offset, length, dataCopy, record.ID})
				}
			}
		}

		offset += int64(length) + 1 // +1 for newline
	}

	return results
}

// scanm extracts metadata at fixed byte positions without JSON parsing.
// This is safe because every record starts with {"idx":N,"_id":"...","_ts":N
// and these fields are always serialised in the same order and width.
// Pass recordType=0 to collect all types. Used by compaction and bloom
// filter construction where only ID, type, and timestamp are needed.
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
			t := int(ln[7] - '0')  // {"idx":N — type at byte 7
			if recordType == 0 || t == recordType {
				id := string(ln[16:32])  // _id at bytes 16..31
				ts, _ := strconv.ParseInt(string(ln[40:53]), 10, 64) // _ts at bytes 40..52
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

// unpack splits entries for compaction: indexes go to one slice, everything
// else to data. The exclude list lets callers drop specific types (e.g.
// TypeHistory during purge).
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

// byIDThenTS sorts entries for compaction output. Records with the same ID
// are ordered oldest-first so that the last entry wins during deduplication.
func byIDThenTS(a, b Entry) int {
	if c := cmp.Compare(a.ID, b.ID); c != 0 {
		return c
	}
	return cmp.Compare(a.TS, b.TS)
}

func byID(a, b *Entry) int {
	return cmp.Compare(a.ID, b.ID)
}
