package folio

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDecodeMalformed(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"not json", []byte("not json")},
		{"incomplete", []byte(`{"idx":1`)},
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

func TestDecodeIndexMalformed(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"not json", []byte("not json")},
		{"incomplete", []byte(`{"idx":1`)},
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

func TestValidMalformed(t *testing.T) {
	tests := []struct {
		data []byte
		want bool
	}{
		{[]byte(`{"idx":1}`), true},
		{[]byte(`{`), true},           // starts with brace
		{[]byte(` {"idx":1}`), false}, // starts with space
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

func TestLabelMalformed(t *testing.T) {
	tests := []struct {
		data []byte
		want string
	}{
		{[]byte(`{"idx":1,"_l":"test"}`), "test"},
		{[]byte(`{"idx":1}`), ""},               // no _l field
		{[]byte(`{"idx":1,"_l":""}`), ""},       // empty label
		{[]byte(`not json`), ""},                // not json
		{[]byte(`{"idx":1,"_l":"unclosed`), ""}, // unclosed quote
	}

	for _, tt := range tests {
		got := label(tt.data)
		if got != tt.want {
			t.Errorf("label(%q) = %q, want %q", tt.data, got, tt.want)
		}
	}
}

func TestHeaderMalformed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	// Write 128 bytes of invalid JSON (enough to read, but can't parse)
	invalid := make([]byte, HeaderSize)
	copy(invalid, []byte("not a valid header"))
	os.WriteFile(path, invalid, 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err != ErrCorruptHeader {
		t.Errorf("header(malformed) = %v, want ErrCorruptHeader", err)
	}
}

func TestHeaderTooShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	// Write header that's too short - will fail on read
	os.WriteFile(path, []byte(`{"_e":0}`), 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	// Returns EOF because file is shorter than HeaderSize
	if err == nil {
		t.Error("header(short) should return error")
	}
}

func TestMalformedRecordSkippedInSparse(t *testing.T) {
	db := openTestDB(t)

	// Write a valid record
	db.Set("valid", "content")

	// Manually write malformed data
	db.raw([]byte("not valid json"))
	db.raw([]byte(`{"idx":2,"_id":"0000000000000000","_ts":1234567890123,"_l":"another","_d":"data","_h":"hist"}`))

	// Sparse scan should skip malformed
	sz, err := size(db.reader)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	results := sparse(db.reader, "", HeaderSize, sz, TypeRecord)

	// Should have found valid records, skipped malformed
	if len(results) < 1 {
		t.Error("sparse should find valid records despite malformed")
	}
}

func TestMalformedRecordSkippedInScanm(t *testing.T) {
	db := openTestDB(t)

	db.Set("valid", "content")

	// Manually write short data that passes valid() but fails scanm checks
	db.raw([]byte(`{short}`))

	sz, err := size(db.reader)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	entries := scanm(db.reader, HeaderSize, sz, 0)

	// Should have found valid entries
	if len(entries) < 1 {
		t.Error("scanm should find valid entries despite malformed")
	}
}

func TestBlankedRecordSkipped(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2") // This blanks the old index

	// Get should find the new version
	data, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}

func TestCompressDecompressErrorPath(t *testing.T) {
	// Decompress invalid data
	_, err := decompress("not valid base85")

	// Should return an error
	if err == nil {
		t.Error("decompress(invalid) should return error")
	}
	if !errors.Is(err, ErrDecompress) {
		t.Errorf("decompress(invalid) error = %v, want ErrDecompress", err)
	}
}
