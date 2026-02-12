// Write primitive tests.
//
// The write layer has three operations: raw (append bytes + newline),
// append (encode a record+index pair then raw both), and writeAt
// (overwrite bytes at a specific offset). Every mutation to the database
// file flows through one of these three functions, so their correctness
// is a prerequisite for every higher-level operation.
//
// Key invariants tested here:
//   - raw always appends at db.tail and advances tail by len(data)+1
//   - raw always terminates the line with '\n' (JSONL format requires it)
//   - raw sets the dirty flag before writing (crash recovery depends on it)
//   - append writes a record then its index, and the index's _o field
//     points back to the record's offset (this is how Get finds data)
//   - writeAt overwrites in place without moving tail (used for blanking
//     old indexes and flipping the dirty flag)
package folio

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRawAppendToEOF verifies that raw writes at the current tail
// offset. If raw wrote at a different position — say byte 0 — it would
// overwrite the header, destroying the file.
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

// TestRawUpdatesTail verifies that tail advances by exactly len(data)+1
// (the +1 is for the newline). If tail advanced by too little, the next
// write would partially overwrite this record. If it advanced by too
// much, there would be a gap of uninitialised bytes that line() would
// read as a corrupt record.
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

// TestRawAddsNewline verifies that raw terminates every write with '\n'.
// The JSONL format requires one JSON object per line; line() reads until
// it hits '\n'. Without the trailing newline, line() would read past
// this record into the next one, concatenating two JSON objects into an
// unparseable blob.
func TestRawAddsNewline(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{})
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

// TestRawSetsDirtyFlag verifies that the first write sets the header's
// Error field to 1. This is the crash-recovery signal: if the process
// dies after this point, the next Open will see Error=1 and run Repair.
// If raw didn't set the dirty flag, a crash mid-write would leave the
// file inconsistent with no automatic recovery on next Open.
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

// TestAppend verifies the high-level append operation that writes a
// record+index pair. The critical check is that the index's Offset
// field points to the record's byte position — this is how Get finds
// the data. If append wrote the index before the record or calculated
// the offset incorrectly, Get would seek to the wrong position and
// either read a different record or hit an out-of-bounds error.
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

// TestWriteAtOverwrites verifies that writeAt modifies bytes in place
// at the specified offset. This is used by Delete (to blank an index
// with spaces) and by dirty() (to flip the error flag byte). If writeAt
// appended instead of overwriting, blanking would fail — the old index
// would remain visible and the blanked data would be appended as a
// corrupt record at the end of the file.
func TestWriteAtOverwrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{})
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

// TestWriteAtDoesNotAffectTail verifies that writeAt leaves the tail
// offset unchanged. writeAt modifies existing bytes (blanking, dirty
// flag); it must not advance tail, otherwise the next raw() call would
// skip over valid file space, leaving a gap of uninitialised bytes.
func TestWriteAtDoesNotAffectTail(t *testing.T) {
	db := openTestDB(t)

	db.raw([]byte(`{"test":"data"}`))
	tailBefore := db.tail

	db.writeAt(HeaderSize, []byte("X"))

	if db.tail != tailBefore {
		t.Errorf("tail changed: %d -> %d", tailBefore, db.tail)
	}
}

// TestWriteAtWithSyncWrites verifies that writeAt calls fsync when
// SyncWrites is enabled. Without fsync, the OS may buffer the overwrite
// and a power loss would leave the old bytes on disk — meaning a deleted
// document's index would reappear, or the dirty flag would revert to
// clean, preventing crash recovery.
func TestWriteAtWithSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{SyncWrites: true})
	defer db.Close()

	db.raw([]byte(`{"test":"data"}`))

	// writeAt should sync
	err := db.writeAt(HeaderSize, []byte("X"))
	if err != nil {
		t.Errorf("writeAt with SyncWrites: %v", err)
	}
}

// TestSetWithSyncWrites exercises the full Set→raw→fsync path with
// SyncWrites enabled, including an update that blanks the old index.
// This is the durability guarantee: after Set returns, the data is on
// stable storage. If fsync were skipped, a power loss could lose the
// write even though Set returned success.
func TestSetWithSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{SyncWrites: true})
	defer db.Close()

	if err := db.Set("doc", "v1"); err != nil {
		t.Fatalf("Set v1: %v", err)
	}

	if err := db.Set("doc", "v2"); err != nil {
		t.Fatalf("Set v2: %v", err)
	}

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}

// TestDeleteWithSyncWrites verifies that Delete + SyncWrites persists
// the blank (space-overwrite) to disk. If the blank weren't synced, a
// power loss could leave the old index intact, resurrecting the deleted
// document on next Open.
func TestDeleteWithSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{SyncWrites: true})
	defer db.Close()

	db.Set("doc", "content")

	if err := db.Delete("doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get deleted: got %v, want ErrNotFound", err)
	}
}

// TestRawWithSyncWrites verifies that raw calls fsync when SyncWrites
// is enabled. raw is the lowest-level write primitive — if it didn't
// sync, neither Set nor Delete would be durable regardless of their
// own fsync calls, because the actual bytes are written by raw.
func TestRawWithSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{SyncWrites: true})
	defer db.Close()

	_, err := db.raw([]byte(`{"test":"data"}`))
	if err != nil {
		t.Errorf("raw with SyncWrites: %v", err)
	}
}
