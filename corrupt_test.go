// On-disk corruption tests.
//
// A storage engine's most important code is the code that runs when things
// go wrong. These tests verify that every read path surfaces a clear error
// when the file is damaged, rather than returning garbage or panicking.
//
// Every test writes valid data through the normal API, then surgically
// damages specific bytes before calling the operation under test. Two
// corruption techniques are used, chosen for different reasons:
//
// Byte patching (writeAt at offset +34): After compaction, records and
// indexes are found via binary search over fixed-position fields — the
// type byte lives at TypePos and the ID at bytes IDStart–IDEnd. By
// corrupting at byte 34 (past the ID but inside the JSON body), binary search still
// locates the line, but JSON parsing fails when decodeIndex or decode
// tries to unmarshal it. This simulates bitrot or a partial sector write
// that damages the middle of a record while leaving the header intact.
//
// Type-mismatch injection (raw with "_o":"bad"): The sparse scanner
// pre-validates each line by calling decode() into a Record struct.
// Record has no _o field, so a string value for _o is silently ignored
// and decode succeeds. But when the caller then calls decodeIndex() into
// an Index struct, _o maps to Offset (int64) and the string value causes
// an unmarshal error. This is the only way to reach the decodeIndex error
// path after a sparse scan, because sparse already filters out lines with
// invalid JSON. It simulates a record whose JSON structure is technically
// valid but whose field types have been corrupted.
package folio

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// --- Get ---
//
// Get has three code paths that read from disk: binary search over the
// sorted index, reading the data record the index points to, and linear
// scan over the sparse region. Each can fail independently.

// Covers get.go line 30: decodeIndex fails on a sorted index line.
//
// After compaction, Get binary-searches the sorted index section for the
// document's ID. If the line it finds has corrupt JSON, decodeIndex
// returns ErrCorruptIndex. Without this check, Get would either panic on
// nil fields or silently return an empty document, both catastrophic for
// a database.
func TestGetCorruptSortedIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	// Byte 34 is past the fixed-position ID prefix, inside the JSON body.
	// Binary search still finds this line by ID, but unmarshal fails.
	db.writeAt(db.indexStart()+34, []byte("!!!!"))

	_, err := db.Get("doc")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// Covers get.go line 39: decode fails on the data record that a valid
// sorted index points to.
//
// Get successfully reads and decodes the sorted index (which gives the
// byte offset of the data record), then seeks to that offset to read the
// actual content. If the data record at that offset has damaged JSON,
// decode returns ErrCorruptRecord. This catches the case where the index
// survived but the data it references did not — e.g. a crash during a
// write that completed the index but corrupted the record.
func TestGetCorruptSortedRecord(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	// The first record in the heap starts at HeaderSize (128). Corrupt its
	// JSON body while leaving the index section untouched.
	db.writeAt(HeaderSize+34, []byte("!!!!"))

	_, err := db.Get("doc")
	if !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("got %v, want ErrCorruptRecord", err)
	}
}

// Covers get.go line 35: line() fails because the sorted index's _o
// offset points past the end of the file.
//
// The _o field in an index record is the byte offset where the data
// record lives. If this value is corrupted to a position beyond EOF,
// line() returns io.EOF. This catches truncated files where the index
// section survived but the data section was lost. The replacement value
// must be the same byte length as the original to preserve the JSON
// structure — otherwise the test would hit the decodeIndex error instead.
func TestGetCorruptSortedRecordOffset(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	// Read the index line, find the _o value, and replace its digits
	// with 9s. Same length preserves JSON structure; the value is
	// guaranteed past EOF for a file this small (~300 bytes).
	data, _ := line(db.reader, db.indexStart())
	marker := []byte(`"_o":`)
	i := bytes.Index(data, marker)
	if i == -1 {
		t.Fatal("_o field not found in index line")
	}
	valStart := i + len(marker)
	valEnd := valStart
	for valEnd < len(data) && data[valEnd] != ',' && data[valEnd] != '}' {
		valEnd++
	}
	db.writeAt(db.indexStart()+int64(valStart), bytes.Repeat([]byte("9"), valEnd-valStart))

	_, err := db.Get("doc")
	if err == nil {
		t.Error("expected error when index points past EOF")
	}
}

// Covers get.go line 58: decodeIndex fails on a sparse index line.
//
// When a document only exists in the sparse region (written since the
// last compaction), Get falls back to a linear scan. The sparse scanner
// pre-validates JSON via decode() into a Record struct, which has no _o
// field. Giving _o a string value passes that check. But the subsequent
// decodeIndex() into an Index struct fails because Offset is int64.
// Without this error path, Get would proceed with a zero offset and
// read whatever happens to be at byte 0 of the file (the header).
func TestGetCorruptSparseIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	// "newdoc" doesn't exist in the sorted region. The injected line
	// lands in sparse and matches on ID, but decodeIndex chokes on
	// the string-typed _o field.
	id := hash("newdoc", db.header.Algorithm)
	bad := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":"bad","_l":"newdoc"}`, id)
	db.raw([]byte(bad))

	_, err := db.Get("newdoc")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// Covers get.go line 63: line() fails on a sparse index whose _o points
// past EOF. Same scenario as the sorted variant above, but exercising
// the sparse code path.
func TestGetCorruptSparseRecordOffset(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	id := hash("doc2", db.header.Algorithm)
	idx := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":9999999,"_l":"doc2"}`, id)
	db.raw([]byte(idx))

	_, err := db.Get("doc2")
	if err == nil {
		t.Error("expected error when sparse index points past EOF")
	}
}

// Covers get.go line 67: decode fails on the data record that a valid
// sparse index points to. The index is structurally valid and its _o
// offset resolves to a real file position, but the record at that
// position has truncated JSON.
func TestGetCorruptSparseRecordData(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	// Write a truncated record first, then an index that points to it.
	// The record has an unclosed JSON string, so decode fails.
	recOff, _ := db.raw([]byte(`{"_r":2,"_id":"0000000000000000","_ts":1234567890123,"_l":"doc2","_d":"!!!CORRUPT`))
	id := hash("doc2", db.header.Algorithm)
	idx := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":%d,"_l":"doc2"}`, id, recOff)
	db.raw([]byte(idx))

	_, err := db.Get("doc2")
	if !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("got %v, want ErrCorruptRecord", err)
	}
}

// --- Exists ---
//
// Exists follows the same two-region lookup as Get but returns a bool
// instead of reading the data record. It still decodes each index line,
// so the same corruption scenarios apply.

// Covers get.go line 93: decodeIndex fails on a sorted index during
// Exists. Same corruption as TestGetCorruptSortedIndex but exercising
// the Exists code path, which has its own decodeIndex call.
func TestExistsCorruptSortedIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	db.writeAt(db.indexStart()+34, []byte("!!!!"))

	_, err := db.Exists("doc")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// Covers get.go line 112: decodeIndex fails on a sparse index during
// Exists. Same type-mismatch injection as TestGetCorruptSparseIndex.
func TestExistsCorruptSparseIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	id := hash("newdoc", db.header.Algorithm)
	bad := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":"bad","_l":"newdoc"}`, id)
	db.raw([]byte(bad))

	_, err := db.Exists("newdoc")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// --- Delete ---
//
// Delete looks up the document's index (sorted then sparse) to find the
// data record offset, then blanks both. If the index can't be decoded,
// Delete must fail rather than silently leaving the document in place.

// Covers delete.go line 28: decodeIndex fails on a sorted index during
// Delete. If Delete can't decode the index, it cannot locate the data
// record to blank, so the delete must be refused rather than silently
// succeeding and leaving stale data discoverable.
func TestDeleteCorruptSortedIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	db.writeAt(db.indexStart()+34, []byte("!!!!"))

	err := db.Delete("doc")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// Covers delete.go line 48: decodeIndex fails on a sparse index during
// Delete.
func TestDeleteCorruptSparseIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	id := hash("newdoc", db.header.Algorithm)
	bad := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":"bad","_l":"newdoc"}`, id)
	db.raw([]byte(bad))

	err := db.Delete("newdoc")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// --- Set (update path) ---
//
// When Set is called for a document that already exists, it needs to
// decode the old index to find the previous data record for retirement
// (retype to history, blank _d, erase index). If the old index is
// corrupt, Set must fail rather than appending a duplicate without
// retiring the old version — that would leave two live indexes for the
// same document, causing undefined behaviour on subsequent reads.

// Covers set.go line 50: decodeIndex fails on the sorted index during
// an update.
func TestSetCorruptSortedIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	db.writeAt(db.indexStart()+34, []byte("!!!!"))

	err := db.Set("doc", "updated")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// Covers set.go line 70: decodeIndex fails on a sparse index during
// an update.
func TestSetCorruptSparseIndex(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	id := hash("newdoc", db.header.Algorithm)
	bad := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":"bad","_l":"newdoc"}`, id)
	db.raw([]byte(bad))

	err := db.Set("newdoc", "updated")
	if !errors.Is(err, ErrCorruptIndex) {
		t.Errorf("got %v, want ErrCorruptIndex", err)
	}
}

// --- History ---
//
// History collects every version of a document by scanning the heap
// (via group) and sparse region. Each matching line is decoded, its
// label and type are verified, and its _h field is decompressed. There
// are four distinct failure modes.

// Covers history.go line 61: decode fails on a heap record.
//
// group() finds records by fixed-position ID matching (no JSON parsing),
// so a record with a valid prefix but corrupt JSON body is returned to
// History. decode() then fails. Without this check, History would skip
// the version silently or panic on nil fields — both unacceptable for a
// version history API where missing entries mean lost data.
func TestHistoryCorruptRecord(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	db.writeAt(HeaderSize+34, []byte("!!!!"))

	_, err := collect(db.History("doc"))
	if !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("got %v, want ErrCorruptRecord", err)
	}
}

// Covers history.go line 71: decompress fails on the _h field.
//
// The record's JSON is valid and decodes fine, but the compressed
// snapshot in _h has been damaged. We overwrite the _h payload with
// "AAAAA" — this is valid ascii85 (decodes to 4 bytes) but those bytes
// are not a valid zstd frame. This exercises the zstd-specific error
// branch in decompress(), distinct from the ascii85 error branch tested
// in malformed_test.go.
func TestHistoryCorruptHistory(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	// Find the _h field's value start position within the record and
	// overwrite the first 5 bytes of the compressed payload.
	data, _ := line(db.reader, HeaderSize)
	i := bytes.Index(data, []byte(`"_h":"`))
	if i == -1 {
		t.Fatal("could not locate _h field in record at HeaderSize")
	}
	db.writeAt(HeaderSize+int64(i)+6, []byte("AAAAA"))

	_, err := collect(db.History("doc"))
	if !errors.Is(err, ErrDecompress) {
		t.Errorf("got %v, want ErrDecompress", err)
	}
}

// Covers history.go line 67: label mismatch causes a skip.
//
// Hash collisions mean two different labels can produce the same ID.
// group() collects all records sharing an ID, so History must verify
// each record's _l field matches the requested label. Here we simulate
// a collision (or corruption) by overwriting _l with a different value.
// History should return zero versions rather than returning someone
// else's data.
func TestHistoryCorruptLabel(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	data, _ := line(db.reader, HeaderSize)
	i := bytes.Index(data, []byte(`"_l":"doc"`))
	if i == -1 {
		t.Fatal("could not locate _l field in record at HeaderSize")
	}
	// Overwrite "doc" with "zzz" — same length, different label.
	db.writeAt(HeaderSize+int64(i)+6, []byte("zzz"))

	versions, err := collect(db.History("doc"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("got %d versions, want 0 (label should not match)", len(versions))
	}
}

// Covers history.go line 64: type mismatch causes a skip.
//
// History only collects TypeRecord (2) and TypeHistory (3) entries. If a
// record's type byte is corrupted to TypeIndex (1), it should be skipped.
// Without this guard, History could try to decompress an index line's _h
// field (which doesn't exist), producing garbage or a panic.
func TestHistoryCorruptType(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Compact()

	// TypePos of the record is the type digit. Change '2' to '1'.
	db.writeAt(HeaderSize+TypePos, []byte("1"))

	versions, err := collect(db.History("doc"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(versions) != 0 {
		t.Errorf("got %d versions, want 0 (type 1 should be skipped)", len(versions))
	}
}

// --- group (scan.go) ---

// Covers scan.go line 184: group's forward walk skips invalid records.
//
// After compaction, all versions of a document are contiguous in the
// heap. group() binary-searches for one, then walks forward collecting
// the rest. If a record in the middle of the group has been corrupted
// (here, the opening brace is replaced with a space so valid() returns
// false), group must skip it and continue rather than stopping the walk.
// The surviving version should still be returned.
func TestGroupSkipsInvalidRecord(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Compact()

	// Replace the first record's '{' with ' '. This makes valid() return
	// false, so group() skips it. The second record (v2) is untouched.
	db.writeAt(HeaderSize, []byte(" "))

	versions, err := collect(db.History("doc"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("got %d versions, want 1 (corrupted v1 should be skipped)", len(versions))
	}
}

// --- List ---

// TestListCorruptIndexStillReturnsLabel verifies that List extracts
// labels via byte scanning (label()) rather than JSON parsing. A record
// with a corrupt _o field still has a valid _l, so List returns the
// label successfully — the corruption only surfaces when Get tries to
// follow the offset.
func TestListCorruptIndexStillReturnsLabel(t *testing.T) {
	db := openTestDB(t)
	db.Set("a", "content")
	db.Compact()

	id := hash("doc2", db.header.Algorithm)
	bad := fmt.Sprintf(`{"_r":1,"_id":"%s","_ts":1234567890123,"_o":"bad","_l":"doc2"}`, id)
	db.raw([]byte(bad))

	labels, err := collect(db.List())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, l := range labels {
		if l == "doc2" {
			found = true
		}
	}
	if !found {
		t.Error("List should return label from record with corrupt _o")
	}
}

// --- decompress ---

// Covers compress.go line 63: zstd decode fails on valid ascii85 input.
//
// decompress has two error branches: ascii85 decoding (line 59) and zstd
// decompression (line 63). The ascii85 branch is tested in malformed_test.go
// with "not valid base85". This test covers the zstd branch by providing
// "AAAAA" — five characters in the valid ascii85 range (33–117) that decode
// to four bytes which are not a valid zstd frame header. Both branches
// must return ErrDecompress so callers can distinguish corruption from
// other failures.
func TestDecompressInvalidZstd(t *testing.T) {
	_, err := decompress("AAAAA")
	if !errors.Is(err, ErrDecompress) {
		t.Errorf("got %v, want ErrDecompress", err)
	}
}
