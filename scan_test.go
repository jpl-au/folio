package folio

import (
	"os"
	"path/filepath"
	"testing"
)

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

// Helper to create sorted index records
func makeIndex(id, label string) string {
	return `{"idx":1,"_id":"` + id + `","_ts":1706000000000,"_o":200,"_l":"` + label + `"}`
}

// Helper to create sorted data records
func makeRecord(id, label string) string {
	return `{"idx":2,"_id":"` + id + `","_ts":1706000000000,"_l":"` + label + `","_d":"data","_h":"hist"}`
}

func TestScanFindExisting(t *testing.T) {
	// Sorted index records
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000002", 0, size(f), TypeIndex)
	if result == nil {
		t.Fatal("expected to find record")
	}
	if result.ID != "0000000000000002" {
		t.Errorf("ID = %q, want %q", result.ID, "0000000000000002")
	}
}

func TestScanNotFound(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000002", 0, size(f), TypeIndex)
	if result != nil {
		t.Error("expected nil for missing ID")
	}
}

func TestScanEmptyRange(t *testing.T) {
	f := createScanTestFile(t, "")
	result := scan(f, "anything", 0, 0, TypeIndex)
	if result != nil {
		t.Error("expected nil for empty range")
	}
}

func TestScanFirstRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000001", 0, size(f), TypeIndex)
	if result == nil || result.ID != "0000000000000001" {
		t.Error("failed to find first record")
	}
}

func TestScanLastRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000003", 0, size(f), TypeIndex)
	if result == nil || result.ID != "0000000000000003" {
		t.Error("failed to find last record")
	}
}

func TestScanWrongType(t *testing.T) {
	// Looking for TypeIndex but file has TypeRecord
	content := makeRecord("0000000000000001", "a") + "\n"

	f := createScanTestFile(t, content)

	result := scan(f, "0000000000000001", 0, size(f), TypeIndex)
	if result != nil {
		t.Error("expected nil when record type doesn't match")
	}
}

func TestScanBackFindRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	// Start from end
	result := scanBack(f, size(f), 0, TypeIndex)
	if result == nil {
		t.Fatal("expected to find record")
	}
	if result.ID != "0000000000000002" {
		t.Errorf("ID = %q, want last record", result.ID)
	}
}

func TestScanBackNoRecord(t *testing.T) {
	f := createScanTestFile(t, "")
	result := scanBack(f, 0, 0, TypeIndex)
	if result != nil {
		t.Error("expected nil for empty file")
	}
}

func TestScanFwdFindRecord(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	result := scanFwd(f, 0, size(f), TypeIndex)
	if result == nil {
		t.Fatal("expected to find record")
	}
	if result.ID != "0000000000000001" {
		t.Errorf("ID = %q, want first record", result.ID)
	}
}

func TestScanFwdNoRecord(t *testing.T) {
	f := createScanTestFile(t, "")
	result := scanFwd(f, 0, 0, TypeIndex)
	if result != nil {
		t.Error("expected nil for empty file")
	}
}

func TestSparseFindByID(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000001", "a2") + "\n" // duplicate ID

	f := createScanTestFile(t, content)

	results := sparse(f, "0000000000000001", 0, size(f), TypeIndex)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

func TestSparseEmptyIDReturnsAll(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeIndex("0000000000000002", "b") + "\n" +
		makeIndex("0000000000000003", "c") + "\n"

	f := createScanTestFile(t, content)

	results := sparse(f, "", 0, size(f), TypeIndex)
	if len(results) != 3 {
		t.Errorf("got %d results, want 3", len(results))
	}
}

func TestSparseFiltersByType(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeRecord("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	results := sparse(f, "", 0, size(f), TypeIndex)
	if len(results) != 1 {
		t.Errorf("got %d TypeIndex results, want 1", len(results))
	}

	results = sparse(f, "", 0, size(f), TypeRecord)
	if len(results) != 1 {
		t.Errorf("got %d TypeRecord results, want 1", len(results))
	}
}

func TestSparseSkipsBlanked(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		"                                                                  \n" + // blanked
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	results := sparse(f, "", 0, size(f), TypeIndex)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2 (blanked skipped)", len(results))
	}
}

func TestScanmExtractMetadata(t *testing.T) {
	content := makeIndex("0000000000000001", "label-a") + "\n" +
		makeRecord("0000000000000002", "label-b") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, size(f), 0) // 0 = all types
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

func TestScanmFilterByType(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		makeRecord("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, size(f), TypeIndex)
	if len(entries) != 1 {
		t.Errorf("got %d entries, want 1", len(entries))
	}
}

func TestScanmSkipsBlanked(t *testing.T) {
	content := makeIndex("0000000000000001", "a") + "\n" +
		"                                                                  \n" +
		makeIndex("0000000000000002", "b") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, size(f), TypeIndex)
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}
}

func TestScanmSkipsShortRecords(t *testing.T) {
	content := "short\n" + makeIndex("0000000000000001", "a") + "\n"

	f := createScanTestFile(t, content)

	entries := scanm(f, 0, size(f), 0)
	if len(entries) != 1 {
		t.Errorf("got %d entries, want 1 (short skipped)", len(entries))
	}
}

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

func TestUnpackEmpty(t *testing.T) {
	data, indexes := unpack(nil)
	if data != nil || indexes != nil {
		t.Error("expected nil slices for empty input")
	}
}

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
