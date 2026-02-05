package folio

import (
	"testing"
	"time"
)

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

func TestMaxLabelSize(t *testing.T) {
	if MaxLabelSize != 256 {
		t.Errorf("MaxLabelSize = %d, want 256", MaxLabelSize)
	}
}

func TestMinRecordSize(t *testing.T) {
	if MinRecordSize != 53 {
		t.Errorf("MinRecordSize = %d, want 53", MinRecordSize)
	}
}

func TestDecodeIndexRecord(t *testing.T) {
	data := []byte(`{"idx":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":5000,"_l":"my app"}`)

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

func TestDecodeDataRecord(t *testing.T) {
	data := []byte(`{"idx":2,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my app","_d":"content","_h":"compressed"}`)

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

func TestDecodeHistoryRecord(t *testing.T) {
	data := []byte(`{"idx":3,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my app","_d":"","_h":"compressed"}`)

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

func TestDecodeIndexRecordWithDecodeIndex(t *testing.T) {
	data := []byte(`{"idx":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":5000,"_l":"my app"}`)

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

func TestDecodeInvalidJSON(t *testing.T) {
	_, err := decode([]byte(`{invalid`))
	if err != ErrCorruptRecord {
		t.Errorf("expected ErrCorruptRecord, got %v", err)
	}
}

func TestDecodeEmpty(t *testing.T) {
	_, err := decode([]byte{})
	if err != ErrCorruptRecord {
		t.Errorf("expected ErrCorruptRecord, got %v", err)
	}
}

func TestValid(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected bool
	}{
		{"valid record", []byte(`{"idx":1,...}`), true},
		{"blanked spaces", []byte("          "), false},
		{"empty line", []byte(""), false},
		{"starts with brace", []byte("{"), true},
		{"starts with space", []byte(` {"idx":1}`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := valid(tt.input); got != tt.expected {
				t.Errorf("valid(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLabel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"index record", `{"idx":1,"_id":"abc","_ts":123,"_o":100,"_l":"my label"}`, "my label"},
		{"data record", `{"idx":2,"_id":"abc","_ts":123,"_l":"my label","_d":"data","_h":"hist"}`, "my label"},
		{"history record", `{"idx":3,"_id":"abc","_ts":123,"_l":"my label","_d":"","_h":"hist"}`, "my label"},
		{"special chars", `{"idx":1,"_id":"abc","_ts":123,"_o":100,"_l":"foo/bar:baz"}`, "foo/bar:baz"},
		{"empty label", `{"idx":1,"_id":"abc","_ts":123,"_o":100,"_l":""}`, ""},
		{"path separators", `{"idx":1,"_id":"abc","_ts":123,"_o":100,"_l":"a/b/c"}`, `a/b/c`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := label([]byte(tt.input)); got != tt.expected {
				t.Errorf("label() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNow(t *testing.T) {
	before := time.Now().UnixMilli()
	result := now()
	after := time.Now().UnixMilli()

	if result < before || result > after {
		t.Errorf("now() = %d, want between %d and %d", result, before, after)
	}
}

func TestNowIncreases(t *testing.T) {
	t1 := now()
	time.Sleep(time.Millisecond)
	t2 := now()

	if t2 < t1 {
		t.Errorf("now() did not increase: %d then %d", t1, t2)
	}
}
