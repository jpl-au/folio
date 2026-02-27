// Unit tests for low-level parsing primitives against malformed input.
//
// These verify that the functions which parse raw bytes — decode,
// decodeIndex, valid, label, header — reject garbage gracefully rather
// than panicking or returning zero-value structs that look valid. The
// inputs are hand-crafted byte slices, not data from a real database
// file. For tests that corrupt a live database and exercise full API
// error paths, see corrupt_test.go.
package folio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestDecodeMalformed ensures that decode returns ErrCorruptRecord for
// inputs that are not valid JSON objects. This matters because scan
// functions pass raw bytes to decode without pre-validating JSON — if
// decode returned a zero-value Record instead of an error, callers would
// silently treat empty strings as document content.
func TestDecodeMalformed(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"not json", []byte("not json")},
		{"incomplete", []byte(`{"_r":1`)},
		{"wrong type", []byte(`["array"]`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decode(tt.data)
			if err == nil {
				t.Error("expected error for malformed data")
			}
		})
	}
}

// TestDecodeIndexMalformed ensures that decodeIndex returns ErrCorruptIndex
// for inputs that are not valid JSON objects. Index records carry the byte
// offset (_o) that Get uses to seek to a data record — if decodeIndex
// returned a zero-value Index, Get would read from byte 0 (the header)
// and return header JSON as document content.
func TestDecodeIndexMalformed(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"not json", []byte("not json")},
		{"incomplete", []byte(`{"_r":1`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeIndex(tt.data)
			if err == nil {
				t.Error("expected error for malformed index")
			}
		})
	}
}

// TestValidMalformed exercises the fast pre-check that scan functions use
// to skip blanked records and non-JSON lines without attempting a full
// parse. Only lines starting with '{' are candidates — blanked records
// start with spaces, and the header line starts with '{' but is at a
// known offset so it's never passed to valid(). Getting this wrong would
// cause scan to attempt JSON parsing on every blanked record, turning
// O(log n) lookups into O(n).
func TestValidMalformed(t *testing.T) {
	tests := []struct {
		data []byte
		want bool
	}{
		{[]byte(`{"_r":1}`), true},
		{[]byte(`{`), true},           // starts with brace
		{[]byte(` {"_r":1}`), false},  // starts with space
		{[]byte(`          `), false}, // all spaces (blanked)
		{[]byte(``), false},           // empty
		{[]byte(`null`), false},       // not a brace
	}

	for _, tt := range tests {
		got := valid(tt.data)
		if got != tt.want {
			t.Errorf("valid(%q) = %v, want %v", tt.data, got, tt.want)
		}
	}
}

// TestLabelMalformed exercises the byte-scanning label extractor that
// avoids a full JSON parse in hot paths (compaction, search). It must
// handle missing _l fields, empty labels, and truncated input by
// returning "" rather than panicking — callers treat "" as "no label"
// and skip the record, which is the correct behaviour for damaged data
// during compaction where we want to salvage what we can.
func TestLabelMalformed(t *testing.T) {
	tests := []struct {
		data []byte
		want string
	}{
		{[]byte(`{"_r":1,"_l":"test"}`), "test"},
		{[]byte(`{"_r":1}`), ""},               // no _l field
		{[]byte(`{"_r":1,"_l":""}`), ""},       // empty label
		{[]byte(`not json`), ""},               // not json
		{[]byte(`{"_r":1,"_l":"unclosed`), ""}, // unclosed quote
	}

	for _, tt := range tests {
		got := label(tt.data)
		if got != tt.want {
			t.Errorf("label(%q) = %q, want %q", tt.data, got, tt.want)
		}
	}
}

// TestHeaderMalformed ensures that header() returns ErrCorruptHeader
// when the first 128 bytes of a file contain invalid JSON. This is the
// first thing Open does — if it accepted a corrupt header, every
// subsequent operation would use wrong section boundaries and read
// garbage from arbitrary file offsets.
func TestHeaderMalformed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	invalid := make([]byte, HeaderSize)
	copy(invalid, []byte("not a valid header"))
	os.WriteFile(path, invalid, 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err != ErrCorruptHeader {
		t.Errorf("got %v, want ErrCorruptHeader", err)
	}
}

// TestHeaderTooShort ensures that header() returns an error when the
// file is shorter than the 128-byte header. This catches truncated files
// (e.g. a crash during initial creation) before any read operations are
// attempted.
func TestHeaderTooShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	os.WriteFile(path, []byte(`{"_e":0}`), 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err == nil {
		t.Error("header(short) should return error")
	}
}

// TestMalformedRecordSkippedInSparse ensures that sparse() silently
// skips lines that fail JSON decoding rather than aborting the scan.
// In the sparse region, records from different documents are interleaved,
// so one corrupt line must not prevent reading the rest. This is the
// correct trade-off: sparse is a read-path function called by Get and
// List, and partial results are better than a total failure for a linear
// scan that may contain thousands of valid records after the bad one.
func TestMalformedRecordSkippedInSparse(t *testing.T) {
	db := openTestDB(t)

	db.Set("valid", "content")
	db.raw([]byte("not valid json"))
	db.raw([]byte(`{"_r":2,"_id":"0000000000000000","_ts":1234567890123,"_l":"another","_d":"data","_h":"hist"}`))

	sz, err := size(db.reader)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	results := sparse(db.reader, "", HeaderSize, sz, TypeRecord)

	if len(results) < 1 {
		t.Error("sparse should find valid records despite malformed line")
	}
}

// TestMalformedRecordSkippedInScanm ensures that scanm() — the
// compaction scanner that reads metadata at fixed byte positions —
// skips lines that are too short to contain the required fields.
// During compaction, every line in the file is visited. A short line
// (e.g. from a crash mid-write) must be skipped rather than causing
// an out-of-bounds panic on the fixed-position field extraction.
func TestMalformedRecordSkippedInScanm(t *testing.T) {
	db := openTestDB(t)

	db.Set("valid", "content")
	db.raw([]byte(`{short}`))

	sz, err := size(db.reader)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	entries := scanm(db.reader, HeaderSize, sz, 0)

	if len(entries) < 1 {
		t.Error("scanm should find valid entries despite short line")
	}
}

// TestBlankedRecordSkipped verifies the normal update path: when a
// document is updated, the old index is overwritten with spaces. The
// blanked line must be invisible to subsequent lookups — valid() returns
// false for lines starting with spaces, so binary search and linear scan
// both skip it. If blanking failed to make the old index invisible,
// Get would return stale content.
func TestBlankedRecordSkipped(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	data, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}

// TestCompressDecompressErrorPath exercises the ascii85 error branch in
// decompress (compress.go line 59). "not valid base85" contains characters
// outside the ascii85 range (33–117), so the ascii85 decoder returns an
// error before zstd is ever invoked. The zstd error branch (line 63) is
// tested separately in corrupt_test.go with valid ascii85 that decodes
// to invalid zstd.
func TestCompressDecompressErrorPath(t *testing.T) {
	_, err := decompress("not valid base85")

	if err == nil {
		t.Error("decompress(invalid) should return error")
	}
	if !errors.Is(err, ErrDecompress) {
		t.Errorf("got %v, want ErrDecompress", err)
	}
}
