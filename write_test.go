package folio

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRawAppendToEOF(t *testing.T) {
	db := openTestDB(t)

	initial := db.tail
	offset, err := db.raw([]byte(`{"test":"data"}`))
	if err != nil {
		t.Fatalf("raw: %v", err)
	}

	if offset != initial {
		t.Errorf("offset = %d, want %d", offset, initial)
	}
}

func TestRawUpdatesTail(t *testing.T) {
	db := openTestDB(t)

	initial := db.tail
	data := []byte(`{"test":"data"}`)
	db.raw(data)

	expected := initial + int64(len(data)) + 1 // +1 for newline
	if db.tail != expected {
		t.Errorf("tail = %d, want %d", db.tail, expected)
	}
}

func TestRawAddsNewline(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, "test.folio", Config{})
	defer db.Close()

	data := []byte(`{"test":"data"}`)
	offset, _ := db.raw(data)

	// Read back from file
	f, _ := os.Open(filepath.Join(dir, "test.folio"))
	defer f.Close()

	buf := make([]byte, len(data)+1)
	f.ReadAt(buf, offset)

	if buf[len(buf)-1] != '\n' {
		t.Error("raw did not append newline")
	}
}

func TestRawSetsDirtyFlag(t *testing.T) {
	db := openTestDB(t)

	if db.header.Error != 0 {
		t.Fatal("header should start clean")
	}

	db.raw([]byte(`{"test":"data"}`))

	if db.header.Error != 1 {
		t.Error("header.Error should be 1 after write")
	}
}

func TestAppend(t *testing.T) {
	db := openTestDB(t)

	record := &Record{
		Type:      TypeRecord,
		ID:        "0123456789abcdef",
		Timestamp: 1234567890123,
		Label:     "test",
		Data:      "content",
		History:   "compressed",
	}

	idx := &Index{
		Type:      TypeIndex,
		ID:        "0123456789abcdef",
		Timestamp: 1234567890123,
		Label:     "test",
	}

	offset, err := db.append(record, idx)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Read Record back
	data, _ := line(db.reader, offset)
	if !valid(data) {
		t.Error("appended record is not valid")
	}

	decoded, _ := decode(data)
	if decoded.Label != "test" {
		t.Errorf("Record Label = %q, want %q", decoded.Label, "test")
	}

	// Read Index back (offset + len(record) + 1)
	idxOffset := offset + int64(len(data)) + 1
	idxData, _ := line(db.reader, idxOffset)
	idxDecoded, _ := decodeIndex(idxData)

	if idxDecoded.Label != "test" {
		t.Errorf("Index Label = %q, want %q", idxDecoded.Label, "test")
	}
	if idxDecoded.Offset != offset {
		t.Errorf("Index Offset = %d, want %d", idxDecoded.Offset, offset)
	}
}

func TestWriteAtOverwrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, "test.folio", Config{})
	defer db.Close()

	// Write initial data
	db.raw([]byte(`{"idx":2,"_id":"0123456789abcdef"}`))

	// Overwrite part of it
	db.writeAt(HeaderSize+8, []byte("3"))

	// Read back
	data, _ := line(db.reader, HeaderSize)
	if data[8] != '3' {
		t.Errorf("overwrite failed: got %q", string(data))
	}
}

func TestWriteAtDoesNotAffectTail(t *testing.T) {
	db := openTestDB(t)

	db.raw([]byte(`{"test":"data"}`))
	tailBefore := db.tail

	db.writeAt(HeaderSize, []byte("X"))

	if db.tail != tailBefore {
		t.Errorf("tail changed: %d -> %d", tailBefore, db.tail)
	}
}

func TestWriteAtWithSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, "test.folio", Config{SyncWrites: true})
	defer db.Close()

	db.raw([]byte(`{"test":"data"}`))

	// writeAt should sync
	err := db.writeAt(HeaderSize, []byte("X"))
	if err != nil {
		t.Errorf("writeAt with SyncWrites: %v", err)
	}
}

func TestRawWithSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, "test.folio", Config{SyncWrites: true})
	defer db.Close()

	_, err := db.raw([]byte(`{"test":"data"}`))
	if err != nil {
		t.Errorf("raw with SyncWrites: %v", err)
	}
}
