// Scan function tests.
//
// The scan layer implements the core lookup algorithms: binary search
// over the sorted section (scan), linear scan over the sparse section
// (sparse), forward and backward linear scans (scanFwd, scanBack), and
// the metadata extractor used by compaction (scanm). Each function reads
// raw bytes from the file and uses fixed-position field extraction to
// compare IDs and filter by record type.
//
// These tests use raw JSONL files (not a full DB) to isolate the scan
// functions from the write path. Each test constructs a minimal file
// with known IDs and record types, then verifies the scan function
// returns the correct results. Also tested: the unpack helper that
// separates indexes from data records, and the sort comparators
// (byIDThenTS, byID) that compaction uses to order the rebuilt heap.
package folio

import (
	"os"
	"path/filepath"
	"testing"
)

// createScanTestFile writes raw content to a temporary file and returns
// an open read handle. Used by scan tests to build files with specific
// record layouts without going through the write layer.
func createScanTestFile(t *testing.T, content string) *os.File {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open test file: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// fsize is a test helper that returns the file size, failing the test
// if stat fails.
func fsize(t *testing.T, f *os.File) int64 {
	t.Helper()
	s, err := size(f)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	return s
}

// Helper to create sorted index records
func makeIndex(id, label string) string {
	return `{"_r":1,"_id":"` + id + `","_ts":1706000000000,"_o":200,"_l":"` + label + `"}`
}

// Helper to create sorted data records
func makeRecord(id, label string) string {
	return `{"_r":2,"_id":"` + id + `","_ts":1706000000000,"_l":"` + label + `","_d":"data","_h":"hist"}`
}

// TestScanFindExisting verifies that binary search finds a record in
// the middle of a three-record file. This is the core lookup path for
// Get after compaction — if binary search compared IDs at the wrong
// byte offset, it would miss the target and return nil.
func TestScanFindExisting(t *testing.T) {
	// Sorted index records
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000002", 0, fsize(t, f), TypeIndex)
	if result == nil {
		t.Fatal("expected to find record")
	}
	if result.ID != "0000000000000002" {
		t.Errorf("ID = %q, want %q", result.ID, "0000000000000002")
	}
}

// TestScanNotFound verifies that binary search returns nil for an ID
// that doesn't exist. If scan returned a near-match instead of nil,
// Get would return a different document's content.
func TestScanNotFound(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000002", 0, fsize(t, f), TypeIndex)
	if result != nil {
		t.Error("expected nil for missing ID")
	}
}

// TestScanEmptyRange verifies that binary search on an empty range
// returns nil. This is the base case for a fresh database (no
// compaction yet, so the sorted section is empty).
func TestScanEmptyRange(t *testing.T) {
	f := createScanTestFile(t, "")
	result := scan(f, "anything", 0, 0, TypeIndex)
	if result != nil {
		t.Error("expected nil for empty range")
	}
}

// TestScanFirstRecord verifies that binary search finds the first
// record in the range. The midpoint calculation must not skip the
// first line — an off-by-one that started at line 2 would miss
// documents whose ID sorts first.
func TestScanFirstRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000001", 0, fsize(t, f), TypeIndex)
	if result == nil || result.ID != "0000000000000001" {
		t.Error("failed to find first record")
	}
}

// TestScanLastRecord verifies that binary search finds the last record.
// The upper-bound logic must not stop one line short — if it did,
// documents whose ID sorts last would be unreachable.
func TestScanLastRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000003", 0, fsize(t, f), TypeIndex)
	if result == nil || result.ID != "0000000000000003" {
		t.Error("failed to find last record")
	}
}

// TestScanWrongType verifies that scan filters by record type. The
// sorted section interleaves data records and index records for the
// same document. Get searches for TypeIndex (to find the byte offset);
// if scan returned a TypeRecord match, Get would try to read _o from
// a record that doesn't have it.
func TestScanWrongType(t *testing.T) {
	// Looking for TypeIndex but file has TypeRecord
	content := makeRecord("0000000000000001", "a") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000001", 0, fsize(t, f), TypeIndex)
	if result != nil {
		t.Error("expected nil when record type doesn't match")
	}
}

// TestScanBackFindRecord verifies that scanBack finds the last record
// when scanning backwards from the end. scanBack is used by the
// sorted-section search when binary search lands past the target — it
// must return the nearest valid record before the position.
func TestScanBackFindRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	// Start from end
	result := scanBack(f, fsize(t, f), 0, TypeIndex)
	if result == nil {
		t.Fatal("expected to find record")
	}
	if result.ID != "0000000000000002" {
		t.Errorf("ID = %q, want last record", result.ID)
	}
}

// TestScanBackNoRecord verifies that scanBack returns nil for an empty
// file. This is the termination condition — without it, scanBack would
// read past offset 0 and panic.
func TestScanBackNoRecord(t *testing.T) {
	f := createScanTestFile(t, "")
	result := scanBack(f, 0, 0, TypeIndex)
	if result != nil {
		t.Error("expected nil for empty file")
	}
}

// TestScanFwdFindRecord verifies that scanFwd returns the first valid
// record when scanning forward. scanFwd is used after binary search
// finds the approximate position — it walks forward to find the exact
// match.
func TestScanFwdFindRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	result := scanFwd(f, 0, fsize(t, f), TypeIndex)
	if result == nil {
		t.Fatal("expected to find record")
	}
	if result.ID != "0000000000000001" {
		t.Errorf("ID = %q, want first record", result.ID)
	}
}

// TestScanFwdNoRecord verifies that scanFwd returns nil for an empty
// range. This is the termination condition for forward scan.
func TestScanFwdNoRecord(t *testing.T) {
	f := createScanTestFile(t, "")
	result := scanFwd(f, 0, 0, TypeIndex)
	if result != nil {
		t.Error("expected nil for empty file")
	}
}

// TestSparseFindByID verifies that sparse() returns all records with a
// matching ID. Unlike binary search (which returns the first match),
// sparse collects every match because the sparse region can contain
// multiple versions of the same document. If sparse stopped after the
// first match, History would only return one version.
func TestSparseFindByID(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000001", "a2") + "\n" // duplicate ID

	f := createScanTestFile(t, content)

	results := sparse(f, "0000000000000001", 0, fsize(t, f), TypeIndex)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

// TestSparseEmptyIDReturnsAll verifies that sparse() with an empty ID
// returns every record. This mode is used by List (which needs all
// labels) and by the compaction scanner (which reads the entire file).
func TestSparseEmptyIDReturnsAll(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	results := sparse(f, "", 0, fsize(t, f), TypeIndex)
	if len(results) != 3 {
		t.Errorf("got %d results, want 3", len(results))
	}
}

// TestSparseFiltersByType verifies that sparse() only returns records of
// the requested type. Get needs TypeIndex; List also needs TypeIndex.
// If sparse returned TypeRecord alongside TypeIndex, callers would try
// to read _o from a data record and get a zero offset.
func TestSparseFiltersByType(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeRecord("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	results := sparse(f, "", 0, fsize(t, f), TypeIndex)
	if len(results) != 1 {
		t.Errorf("got %d TypeIndex results, want 1", len(results))
	}

	results = sparse(f, "", 0, fsize(t, f), TypeRecord)
	if len(results) != 1 {
		t.Errorf("got %d TypeRecord results, want 1", len(results))
	}
}

// TestSparseSkipsBlanked verifies that sparse() ignores lines that have
// been blanked with spaces (from delete or update). valid() returns
// false for lines starting with a space, so sparse must check valid()
// before attempting JSON parsing. Without this, sparse would error on
// the blanked line and potentially abort the scan.
func TestSparseSkipsBlanked(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		"                                                                  \n" + // blanked
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	results := sparse(f, "", 0, fsize(t, f), TypeIndex)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2 (blanked skipped)", len(results))
	}
}

// TestScanmExtractMetadata verifies that scanm extracts the type, ID,
// timestamp, label, and byte offset from fixed positions in the line.
// scanm reads every line in the file during compaction — it uses byte-
// position extraction instead of JSON parsing for speed. If the field
// positions were wrong, compaction would sort records incorrectly and
// generate indexes pointing to the wrong byte offsets.
func TestScanmExtractMetadata(t *testing.T) {
	content := makeIndex("0000000000000001", "label-a") + "\n" +
		makeRecord("0000000000000002", "label-b") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, fsize(t, f), 0) // 0 = all types
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}

	// Check index entry
	if entries[0].Type != TypeIndex {
		t.Errorf("entry[0].Type = %d, want %d", entries[0].Type, TypeIndex)
	}
	if entries[0].ID != "0000000000000001" {
		t.Errorf("entry[0].ID = %q, want %q", entries[0].ID, "0000000000000001")
	}
	if entries[0].Label != "label-a" {
		t.Errorf("entry[0].Label = %q, want %q", entries[0].Label, "label-a")
	}

	// Check data entry (label only populated for index)
	if entries[1].Type != TypeRecord {
		t.Errorf("entry[1].Type = %d, want %d", entries[1].Type, TypeRecord)
	}
	if entries[1].Label != "" {
		t.Errorf("entry[1].Label = %q, want empty (non-index)", entries[1].Label)
	}
}

// TestScanmFilterByType verifies that scanm respects the type filter.
// Compaction calls scanm with a specific type to read only indexes or
// only data records in separate passes. If the filter were ignored,
// compaction would mix indexes and data records in the sort, producing
// a corrupt heap.
func TestScanmFilterByType(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeRecord("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, fsize(t, f), TypeIndex)
	if len(entries) != 1 {
		t.Errorf("got %d entries, want 1", len(entries))
	}
}

// TestScanmSkipsBlanked verifies that scanm ignores blanked (space-
// overwritten) lines. Blanked lines are remnants of deleted or updated
// documents. If scanm included them, compaction would try to extract
// fields from spaces and produce garbage entries in the rebuilt file.
func TestScanmSkipsBlanked(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		"                                                                  \n" +
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, fsize(t, f), TypeIndex)
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}
}

// TestScanmSkipsShortRecords verifies that scanm ignores lines shorter
// than MinRecordSize. A short line (e.g. from a crash mid-write) cannot
// contain all fixed-position fields. Without this length check, scanm
// would read past the end of the line and panic with an index-out-of-
// bounds error.
func TestScanmSkipsShortRecords(t *testing.T) {
	content := "short\n" + makeIndex("0000000000000001", "a") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, fsize(t, f), 0)
	if len(entries) != 1 {
		t.Errorf("got %d entries, want 1 (short skipped)", len(entries))
	}
}

// TestUnpackSeparatesTypes verifies that unpack splits a mixed entry
// list into data+history entries and index entries. Compaction needs
// them separated: data entries are sorted and written as the heap,
// index entries are generated fresh. If unpack mixed them, the rebuilt
// file would have indexes in the heap and data in the index section.
func TestUnpackSeparatesTypes(t *testing.T) {
	entries := []Entry{
		{Type: TypeIndex, ID: "1"},
		{Type: TypeRecord, ID: "2"},
		{Type: TypeHistory, ID: "3"},
		{Type: TypeIndex, ID: "4"},
	}

	data, indexes := unpack(entries)

	if len(indexes) != 2 {
		t.Errorf("got %d indexes, want 2", len(indexes))
	}
	if len(data) != 2 {
		t.Errorf("got %d data, want 2", len(data))
	}
}

// TestUnpackExcludeHistory verifies that unpack can exclude specific
// types. The PurgeHistory option passes TypeHistory to unpack's exclude
// list, stripping old versions from the data set before rebuild. If
// the exclusion logic were inverted, purge would keep only history
// records and discard all current data.
func TestUnpackExcludeHistory(t *testing.T) {
	entries := []Entry{
		{Type: TypeRecord, ID: "1"},
		{Type: TypeHistory, ID: "2"},
		{Type: TypeRecord, ID: "3"},
	}

	data, _ := unpack(entries, TypeHistory)

	if len(data) != 2 {
		t.Errorf("got %d data after excluding history, want 2", len(data))
	}
}

// TestUnpackEmpty verifies that unpack handles nil input without
// panicking. This is the case for an empty database — scanm returns
// no entries, and unpack must return nil slices rather than crashing.
func TestUnpackEmpty(t *testing.T) {
	data, indexes := unpack(nil)
	if data != nil || indexes != nil {
		t.Error("expected nil slices for empty input")
	}
}

// TestByIDThenTS verifies the sort comparator used during compaction.
// Records must be sorted by ID first (for binary search) then by
// timestamp (for version ordering within a document). If the sort
// order were wrong — e.g. timestamp-first — binary search would fail
// because records for the same document would be scattered among
// records from other documents.
func TestByIDThenTS(t *testing.T) {
	tests := []struct {
		name     string
		a, b     Entry
		expected int
	}{
		{"same ID, a older", Entry{ID: "1", TS: 100}, Entry{ID: "1", TS: 200}, -1},
		{"same ID, a newer", Entry{ID: "1", TS: 200}, Entry{ID: "1", TS: 100}, 1},
		{"same ID same TS", Entry{ID: "1", TS: 100}, Entry{ID: "1", TS: 100}, 0},
		{"a.ID < b.ID", Entry{ID: "1", TS: 100}, Entry{ID: "2", TS: 100}, -1},
		{"a.ID > b.ID", Entry{ID: "2", TS: 100}, Entry{ID: "1", TS: 100}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := byIDThenTS(tt.a, tt.b)
			if (result < 0) != (tt.expected < 0) && (result > 0) != (tt.expected > 0) && (result == 0) != (tt.expected == 0) {
				t.Errorf("byIDThenTS = %d, want sign of %d", result, tt.expected)
			}
		})
	}
}

// TestByID verifies the ID-only comparator used for index sorting.
// Indexes are sorted by ID alone (no timestamp) because binary search
// needs exactly one index per document to find.
func TestByID(t *testing.T) {
	a := &Entry{ID: "1"}
	b := &Entry{ID: "2"}

	if byID(a, b) >= 0 {
		t.Error("expected a < b")
	}
	if byID(b, a) <= 0 {
		t.Error("expected b > a")
	}
	if byID(a, a) != 0 {
		t.Error("expected a == a")
	}
}
