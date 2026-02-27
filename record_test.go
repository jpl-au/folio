// Record type and parsing primitive tests.
//
// Records are the fundamental unit of storage in a folio file. Every
// line after the 128-byte header is one of three types: Index (type 1,
// carries a byte offset to the data), Record (type 2, carries the
// document content), or History (type 3, carries compressed past
// versions). The type byte lives at a fixed position (byte TypePos) so that
// scan functions can identify it without JSON parsing.
//
// These tests verify the type constants that are persisted on disk, the
// decode/decodeIndex parsers that turn raw bytes into structs, the
// valid() pre-check that filters out blanked records, the label()
// extractor that avoids full JSON parsing in hot paths, the unescape()
// function that handles JSON string escape sequences, and the now()
// timestamp generator that provides ordering for versions.
package folio

import (
	"testing"
	"time"
)

// TestRecordTypeConstants guards the numeric values stored in every
// record's "_r" field. These values are persisted on disk — if
// TypeRecord changed from 2 to something else, existing databases
// would misidentify every data record.
func TestRecordTypeConstants(t *testing.T) {
	if TypeIndex != 1 {
		t.Errorf("TypeIndex = %d, want 1", TypeIndex)
	}
	if TypeRecord != 2 {
		t.Errorf("TypeRecord = %d, want 2", TypeRecord)
	}
	if TypeHistory != 3 {
		t.Errorf("TypeHistory = %d, want 3", TypeHistory)
	}
}

// TestMaxLabelSize guards the label length limit. Labels are stored
// inline in the JSON record; an excessively long label would push the
// record past the line length that line() can buffer, causing read
// failures. The constant is persisted implicitly (existing records
// respect it), so changing it would make old records with long labels
// unreadable.
func TestMaxLabelSize(t *testing.T) {
	if MaxLabelSize != 256 {
		t.Errorf("MaxLabelSize = %d, want 256", MaxLabelSize)
	}
}

// TestMinRecordSize guards the minimum line length used by scanm to
// skip short/corrupt lines. A valid record must be at least 52 bytes
// (the fixed-position fields: type, ID, timestamp). If this constant
// drifted, scanm would either skip valid short records or attempt to
// extract fields from lines too short to contain them, causing an
// index-out-of-bounds panic.
func TestMinRecordSize(t *testing.T) {
	if MinRecordSize != 52 {
		t.Errorf("MinRecordSize = %d, want 52", MinRecordSize)
	}
}

// TestDecodeIndexRecord verifies that decode() correctly parses a type-1
// (Index) record. decode() is used by sparse() which needs to match IDs
// and labels; if the Type field were parsed incorrectly, sparse would
// misidentify indexes as data records and skip them.
func TestDecodeIndexRecord(t *testing.T) {
	data := []byte(`{"_r":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":5000,"_l":"my app"}`)

	r, err := decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if r.Type != TypeIndex {
		t.Errorf("Type = %d, want %d", r.Type, TypeIndex)
	}
	if r.ID != "a1b2c3d4e5f6g7h8" {
		t.Errorf("ID = %q, want %q", r.ID, "a1b2c3d4e5f6g7h8")
	}
}

// TestDecodeDataRecord verifies that decode() correctly parses a type-2
// (Record) line including the Data and History fields. These fields
// carry the actual document content and compressed version history —
// if they were swapped or truncated, Get would return history data as
// content or History would fail to decompress.
func TestDecodeDataRecord(t *testing.T) {
	data := []byte(`{"_r":2,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my app","_d":"content","_h":"compressed"}`)

	r, err := decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if r.Type != TypeRecord {
		t.Errorf("Type = %d, want %d", r.Type, TypeRecord)
	}
	if r.Data != "content" {
		t.Errorf("Data = %q, want %q", r.Data, "content")
	}
	if r.History != "compressed" {
		t.Errorf("History = %q, want %q", r.History, "compressed")
	}
}

// TestDecodeHistoryRecord verifies that decode() correctly parses a
// type-3 (History) record. History records are written during
// compaction and have an empty Data field but a non-empty History
// field. If decode() treated empty Data as an error, compacted
// databases would fail to load their version history.
func TestDecodeHistoryRecord(t *testing.T) {
	data := []byte(`{"_r":3,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my app","_d":"","_h":"compressed"}`)

	r, err := decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if r.Type != TypeHistory {
		t.Errorf("Type = %d, want %d", r.Type, TypeHistory)
	}
	if r.Data != "" {
		t.Errorf("Data = %q, want empty", r.Data)
	}
	if r.History != "compressed" {
		t.Errorf("History = %q, want %q", r.History, "compressed")
	}
}

// TestDecodeIndexRecordWithDecodeIndex verifies that decodeIndex()
// extracts the Offset field that Get uses to seek to the data record.
// decode() maps into a Record struct (no Offset field); decodeIndex()
// maps into an Index struct (with Offset). If decodeIndex failed to
// parse _o, Get would seek to offset 0 and read the header as content.
func TestDecodeIndexRecordWithDecodeIndex(t *testing.T) {
	data := []byte(`{"_r":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":5000,"_l":"my app"}`)

	idx, err := decodeIndex(data)
	if err != nil {
		t.Fatalf("decodeIndex error: %v", err)
	}

	if idx.Offset != 5000 {
		t.Errorf("Offset = %d, want 5000", idx.Offset)
	}
	if idx.Label != "my app" {
		t.Errorf("Label = %q, want %q", idx.Label, "my app")
	}
}

// TestDecodeInvalidJSON verifies that decode() returns ErrCorruptRecord
// for invalid JSON rather than a zero-value Record. A zero-value would
// have Type=0, which doesn't match any valid type — but callers might
// not check for that, leading to silent misclassification.
func TestDecodeInvalidJSON(t *testing.T) {
	_, err := decode([]byte(`{invalid`))
	if err != ErrCorruptRecord {
		t.Errorf("expected ErrCorruptRecord, got %v", err)
	}
}

// TestDecodeEmpty verifies that decode() returns ErrCorruptRecord for
// empty input. Empty bytes can appear when line() reads past the end
// of the file or encounters a zero-length line between two newlines.
func TestDecodeEmpty(t *testing.T) {
	_, err := decode([]byte{})
	if err != ErrCorruptRecord {
		t.Errorf("expected ErrCorruptRecord, got %v", err)
	}
}

// TestValid exercises the fast pre-check that scan functions use to
// skip blanked records without parsing JSON. Only lines starting with
// '{' are candidates. Blanked records start with spaces (from writeAt
// during delete/update). Getting this wrong would cause scan to attempt
// JSON parsing on every blanked record, turning O(log n) binary search
// into O(n) with parse errors.
func TestValid(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected bool
	}{
		{"valid record", []byte(`{"_r":1,...}`), true},
		{"blanked spaces", []byte("          "), false},
		{"empty line", []byte(""), false},
		{"starts with brace", []byte("{"), true},
		{"starts with space", []byte(` {"_r":1}`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valid(tt.input); got != tt.expected {
				t.Errorf("valid(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// TestLabel exercises the byte-scanning label extractor that avoids a
// full JSON parse in hot paths (compaction, search). It must handle
// all three record types, special characters in labels, and empty
// labels. Returning the wrong label would cause compaction to group
// records under the wrong document, mixing unrelated version histories.
func TestLabel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"index record", `{"_r":1,"_id":"abc","_ts":123,"_o":100,"_l":"my label"}`, "my label"},
		{"data record", `{"_r":2,"_id":"abc","_ts":123,"_l":"my label","_d":"data","_h":"hist"}`, "my label"},
		{"history record", `{"_r":3,"_id":"abc","_ts":123,"_l":"my label","_d":"","_h":"hist"}`, "my label"},
		{"special chars", `{"_r":1,"_id":"abc","_ts":123,"_o":100,"_l":"foo/bar:baz"}`, "foo/bar:baz"},
		{"empty label", `{"_r":1,"_id":"abc","_ts":123,"_o":100,"_l":""}`, ""},
		{"path separators", `{"_r":1,"_id":"abc","_ts":123,"_o":100,"_l":"a/b/c"}`, `a/b/c`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := label([]byte(tt.input)); got != tt.expected {
				t.Errorf("label() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestNow verifies that now() returns a millisecond timestamp within
// the current wall-clock window. The timestamp is stored in every
// record's _ts field and used to order versions within a document.
// If now() returned seconds instead of milliseconds, two rapid writes
// would get the same timestamp and their ordering would be ambiguous.
func TestNow(t *testing.T) {
	before := time.Now().UnixMilli()
	result := now()
	after := time.Now().UnixMilli()

	if result < before || result > after {
		t.Errorf("now() = %d, want between %d and %d", result, before, after)
	}
}

// TestNowIncreases verifies that successive calls to now() produce
// non-decreasing values. Version ordering in History depends on
// timestamps increasing monotonically. If now() could go backwards
// (e.g. due to a time zone bug), History would return versions in
// the wrong order.
func TestNowIncreases(t *testing.T) {
	t1 := now()
	time.Sleep(time.Millisecond)
	t2 := now()

	if t2 < t1 {
		t.Errorf("now() did not increase: %d then %d", t1, t2)
	}
}

// TestUnescape exercises the JSON string unescaper used by the label()
// fast path and Search content matching. JSON strings can contain
// escape sequences like \" \\ \n \uXXXX — if unescape() missed any of
// these, labels or content containing special characters would fail to
// match, making affected documents invisible to Search and causing
// label mismatches during compaction.
func TestUnescape(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no escapes", "hello world", "hello world"},
		{"quote", `hello \"world\"`, `hello "world"`},
		{"backslash", `a\\b`, `a\b`},
		{"slash", `a\/b`, `a/b`},
		{"newline", `line1\\nline2`, "line1\\nline2"},
		{"literal newline", `line1\nline2`, "line1\nline2"},
		{"tab", `col1\tcol2`, "col1\tcol2"},
		{"carriage return", `a\rb`, "a\rb"},
		{"backspace", `a\bb`, "a\bb"},
		{"formfeed", `a\fb`, "a\fb"},
		{"unicode basic", `\u0041`, "A"},
		{"unicode multi", `\u00e9`, "\u00e9"},
		{"mixed", `say \"hi\"\nbye\\end`, "say \"hi\"\nbye\\end"},
		{"empty", "", ""},
		{"trailing backslash", `abc\`, `abc\`},
		{"short unicode", `\u00`, `\u00`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(unescape([]byte(tt.in)))
			if got != tt.want {
				t.Errorf("unescape(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
