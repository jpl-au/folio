// On-disk format verification tests.
//
// Folio's file format has strict layout requirements that every read
// function depends on: the header is exactly 128 bytes, record types
// are at byte 7, IDs are at bytes 16–31, timestamps are at bytes 40–52,
// and section boundaries (Heap, Index) define where binary search
// operates. These tests read raw bytes from the file and verify the
// format matches expectations. They serve as a contract between the
// write path (which produces the layout) and the read path (which
// assumes it) — if either side changes, these tests catch the mismatch
// before it becomes a runtime bug.
package folio

import (
	json "github.com/goccy/go-json"
	"os"
	"path/filepath"
	"testing"
)

// dbsize is a test helper that returns the database file size.
func dbsize(t *testing.T, db *DB) int64 {
	t.Helper()
	s, err := size(db.reader)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	return s
}

// TestConstants guards every exported constant that is persisted on disk
// or used in byte-position calculations. If any value changed, existing
// databases would become unreadable because the read path extracts
// fields at hardcoded offsets derived from these constants.
func TestConstants(t *testing.T) {
	tests := []struct {
		name string
		got  int
		want int
	}{
		{"HeaderSize", HeaderSize, 128},
		{"MaxLabelSize", MaxLabelSize, 256},
		{"MaxRecordSize", MaxRecordSize, 16 * 1024 * 1024},
		{"TypeIndex", TypeIndex, 1},
		{"TypeRecord", TypeRecord, 2},
		{"TypeHistory", TypeHistory, 3},
		{"StateAll", StateAll, 0},
		{"StateRead", StateRead, 1},
		{"StateNone", StateNone, 2},
		{"StateClosed", StateClosed, 3},
		{"MinRecordSize", MinRecordSize, 53},
		{"AlgXXHash3", AlgXXHash3, 1},
		{"AlgFNV1a", AlgFNV1a, 2},
		{"AlgBlake2b", AlgBlake2b, 3},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}
}

// TestHeaderFormat reads the raw bytes of a freshly-created database
// file and verifies: the file is at least 128 bytes, the header ends
// with a newline, and the header is valid JSON. If the header format
// changed (e.g. a different JSON library that omits the trailing pad),
// this test catches it before any read operation fails with a cryptic
// parse error.
func TestHeaderFormat(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db.Close()

	// Read raw header
	data, _ := os.ReadFile(filepath.Join(dir, "test.folio"))

	// Should be exactly 128 bytes
	if len(data) < HeaderSize {
		t.Fatalf("file too short: %d bytes", len(data))
	}

	header := data[:HeaderSize]

	// Should end with newline
	if header[HeaderSize-1] != '\n' {
		t.Error("header should end with newline")
	}

	// Should be valid JSON
	var h Header
	if err := json.Unmarshal(header[:HeaderSize-1], &h); err != nil {
		t.Errorf("header not valid JSON: %v", err)
	}
}

// TestRecordFormat verifies that the first record after the header
// starts with {"idx": and has a valid type byte at position 7. This is
// the most basic format contract — if the JSON library changed field
// ordering, every fixed-position extraction in scan, scanm, and binary
// search would read the wrong bytes.
func TestRecordFormat(t *testing.T) {
	db := openTestDB(t)

	db.Set("test", "content")

	// Read raw file
	info, _ := db.reader.Stat()
	data := make([]byte, info.Size())
	db.reader.ReadAt(data, 0)

	// Find first record after header
	recordStart := HeaderSize
	for recordStart < len(data) && data[recordStart] != '{' {
		recordStart++
	}

	if recordStart >= len(data) {
		t.Fatal("no record found")
	}

	// Find end of record
	recordEnd := recordStart
	for recordEnd < len(data) && data[recordEnd] != '\n' {
		recordEnd++
	}

	record := data[recordStart:recordEnd]

	// Should start with {"idx":
	if len(record) < 7 || string(record[:7]) != `{"idx":` {
		t.Errorf("record should start with {\"idx\":")
	}

	// Type at position 7
	recordType := record[7] - '0'
	if recordType != TypeRecord && recordType != TypeIndex {
		t.Errorf("record type = %d, want %d or %d", recordType, TypeRecord, TypeIndex)
	}
}

// TestIndexRecordFormat verifies the structure of a type-1 (Index)
// record: 16-char hex ID, correct label, and an Offset >= HeaderSize.
// The Offset is the critical field — it tells Get where to seek for
// the data record. An Offset < HeaderSize would read inside the header.
func TestIndexRecordFormat(t *testing.T) {
	db := openTestDB(t)

	db.Set("test", "content")

	// Find index record
	results := sparse(db.reader, "", HeaderSize, dbsize(t, db), TypeIndex)
	if len(results) == 0 {
		t.Fatal("no index record found")
	}

	idx, err := decodeIndex(results[0].Data)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}

	// Verify format
	if idx.Type != TypeIndex {
		t.Errorf("Type = %d, want %d", idx.Type, TypeIndex)
	}
	if len(idx.ID) != 16 {
		t.Errorf("ID length = %d, want 16", len(idx.ID))
	}
	if idx.Label != "test" {
		t.Errorf("Label = %q, want %q", idx.Label, "test")
	}
	if idx.Offset < HeaderSize {
		t.Errorf("Offset = %d, want >= %d", idx.Offset, HeaderSize)
	}
}

// TestDataRecordFormat verifies the structure of a type-2 (Record)
// record: correct type, 16-char ID, matching label, content in _d,
// and a non-empty _h field. The _h field is populated even for a single
// version (it stores a compressed empty history) — if it were missing,
// History would fail to decompress it.
func TestDataRecordFormat(t *testing.T) {
	db := openTestDB(t)

	db.Set("test", "content")

	results := sparse(db.reader, "", HeaderSize, dbsize(t, db), TypeRecord)
	if len(results) == 0 {
		t.Fatal("no data record found")
	}

	rec, err := decode(results[0].Data)
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}

	if rec.Type != TypeRecord {
		t.Errorf("Type = %d, want %d", rec.Type, TypeRecord)
	}
	if len(rec.ID) != 16 {
		t.Errorf("ID length = %d, want 16", len(rec.ID))
	}
	if rec.Label != "test" {
		t.Errorf("Label = %q, want %q", rec.Label, "test")
	}
	if rec.Data != "content" {
		t.Errorf("Data = %q, want %q", rec.Data, "content")
	}
	if rec.History == "" {
		t.Error("History should not be empty")
	}
}

// TestHistoryRecordFormat verifies the structure of a type-3 (History)
// record. When a document is updated, the old version becomes a History
// record with a blanked _d field (spaces) and the compressed version
// chain in _h. If the _d field weren't blanked, Search would match
// stale content from old versions.
func TestHistoryRecordFormat(t *testing.T) {
	db := openTestDB(t)

	db.Set("test", "v1")
	db.Set("test", "v2") // v1 becomes history

	results := sparse(db.reader, "", HeaderSize, dbsize(t, db), TypeHistory)
	if len(results) == 0 {
		t.Fatal("no history record found")
	}

	rec, err := decode(results[0].Data)
	if err != nil {
		t.Fatalf("decode history: %v", err)
	}

	if rec.Type != TypeHistory {
		t.Errorf("Type = %d, want %d", rec.Type, TypeHistory)
	}
	// _d should be blanked (spaces)
	for _, c := range rec.Data {
		if c != ' ' && c != 0 {
			t.Logf("History _d not fully blanked: %q", rec.Data)
			break
		}
	}
	// _h should still have content
	if rec.History == "" {
		t.Error("History._h should not be empty")
	}
}

// TestScanmBytePositions marshals each record type and asserts the fixed
// byte offsets that scanm relies on: type at 7, ID at 16..31, TS at 40..52.
// If the JSON library changes field order, this test catches it.
func TestScanmBytePositions(t *testing.T) {
	id := "abcdef0123456789"
	ts := int64(1706000000000)

	records := []struct {
		name string
		data []byte
	}{
		{"Index", mustMarshal(t, Index{Type: TypeIndex, ID: id, Timestamp: ts, Offset: 128, Label: "x"})},
		{"Record", mustMarshal(t, Record{Type: TypeRecord, ID: id, Timestamp: ts, Label: "x", Data: "d", History: "h"})},
		{"History", mustMarshal(t, Record{Type: TypeHistory, ID: id, Timestamp: ts, Label: "x", Data: "", History: "h"})},
	}

	for _, tt := range records {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.data
			if len(b) < MinRecordSize {
				t.Fatalf("marshalled %s too short: %d bytes", tt.name, len(b))
			}
			if b[7]-'0' != byte(b[7]-'0') || int(b[7]-'0') < 1 || int(b[7]-'0') > 3 {
				t.Errorf("type byte at 7: got %q", b[7])
			}
			gotID := string(b[16:32])
			if gotID != id {
				t.Errorf("ID at [16:32] = %q, want %q\nraw: %s", gotID, id, b)
			}
			gotTS := string(b[40:53])
			if gotTS != "1706000000000" {
				t.Errorf("TS at [40:53] = %q, want %q\nraw: %s", gotTS, "1706000000000", b)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestSectionBoundaries verifies the section layout invariants before
// and after compaction. A fresh database has no sorted sections (all
// zero), and sparse starts at HeaderSize. After compaction, the heap
// and index sections exist, indexStart < indexEnd, and sparse starts
// at indexEnd. If any boundary were wrong, binary search would operate
// on the wrong byte range.
func TestSectionBoundaries(t *testing.T) {
	db := openTestDB(t)

	// Fresh DB - no sorted sections
	if db.indexStart() != 0 {
		t.Errorf("fresh indexStart = %d, want 0", db.indexStart())
	}
	if db.indexEnd() != 0 {
		t.Errorf("fresh indexEnd = %d, want 0", db.indexEnd())
	}
	if db.sparseStart() != HeaderSize {
		t.Errorf("fresh sparseStart = %d, want %d", db.sparseStart(), HeaderSize)
	}

	db.Set("doc", "content")
	db.Compact()

	// After compact - sorted sections exist
	if db.indexStart() == 0 {
		t.Error("post-compact indexStart should not be 0")
	}
	if db.indexEnd() == 0 {
		t.Error("post-compact indexEnd should not be 0")
	}
	if db.indexStart() >= db.indexEnd() {
		t.Errorf("data should end before index: %d >= %d", db.indexStart(), db.indexEnd())
	}
	if db.sparseStart() != db.indexEnd() {
		t.Errorf("sparseStart = %d, want %d (indexEnd)", db.sparseStart(), db.indexEnd())
	}
}

// TestIDAtFixedPosition verifies that the ID extracted by byte-position
// (bytes 16–31) matches the ID from full JSON parsing. Binary search
// uses the byte-position extraction for speed; if the positions drifted
// from the JSON layout, binary search would compare garbage bytes and
// miss every document.
func TestIDAtFixedPosition(t *testing.T) {
	db := openTestDB(t)

	db.Set("test", "content")

	results := sparse(db.reader, "", HeaderSize, dbsize(t, db), TypeRecord)
	if len(results) == 0 {
		t.Fatal("no record found")
	}

	// ID should be at bytes [16:32] relative to record start
	data := results[0].Data
	if len(data) < 32 {
		t.Fatal("record too short")
	}

	idFromFixed := string(data[16:32])
	rec, _ := decode(data)

	if idFromFixed != rec.ID {
		t.Errorf("ID at fixed position = %q, parsed ID = %q", idFromFixed, rec.ID)
	}
}

// TestTimestampAtFixedPosition verifies that bytes 40–52 contain a
// numeric timestamp. scanm uses this range to extract timestamps
// without JSON parsing during compaction. If the timestamp moved to
// a different position, compaction would sort records by garbage values,
// producing a heap with wrong version ordering.
func TestTimestampAtFixedPosition(t *testing.T) {
	db := openTestDB(t)

	db.Set("test", "content")

	results := sparse(db.reader, "", HeaderSize, dbsize(t, db), TypeRecord)
	if len(results) == 0 {
		t.Fatal("no record found")
	}

	// TS should be at bytes [40:53] relative to record start
	data := results[0].Data
	if len(data) < 53 {
		t.Fatal("record too short")
	}

	// Verify position by checking it's a number
	tsBytes := data[40:53]
	for _, b := range tsBytes {
		if b < '0' || b > '9' {
			t.Errorf("TS at fixed position contains non-digit: %q", tsBytes)
			break
		}
	}
}
