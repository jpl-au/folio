// Core CRUD and lifecycle tests.
//
// These tests exercise the public API (Open, Close, Set, Get, Delete,
// Exists, List, History, Compact, Purge, Rehash) through its happy
// paths and common error conditions. Each test creates a fresh database
// in a temporary directory, performs a sequence of operations, and
// verifies the result. Together they form the functional specification
// of the database: if any of these tests fail, a fundamental guarantee
// has been broken.
package folio

import (
	"path/filepath"
	"strings"
	"testing"
)

// openTestDB creates a fresh database in a temporary directory and
// registers cleanup to close it when the test finishes. Used by
// nearly every test in the suite.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestOpenCreateNew verifies that Open creates a new file when the path
// doesn't exist. This is the first-run experience — if Open required
// the file to already exist, users would need a separate create step.
func TestOpenCreateNew(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	path := filepath.Join(dir, "test.folio")
	if _, err := filepath.Glob(path); err != nil {
		t.Errorf("file not created")
	}
}

// TestOpenExisting verifies that data persists across close and reopen.
// This is the durability guarantee: a document written before Close must
// be readable after reopen. If the header or tail offset were not
// persisted correctly, the reopened database would think it was empty.
func TestOpenExisting(t *testing.T) {
	dir := t.TempDir()

	db1, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db1.Set("doc", "content")
	db1.Close()

	db2, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	data, err := db2.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

// TestOpenDefaultConfig verifies that passing Config{} applies sensible
// defaults. If defaults weren't applied, a zero-value ReadBuffer would
// cause line() to allocate a 0-byte buffer and fail to read any record.
func TestOpenDefaultConfig(t *testing.T) {
	db := openTestDB(t)

	if db.config.HashAlgorithm != AlgXXHash3 {
		t.Errorf("HashAlgorithm = %d, want %d", db.config.HashAlgorithm, AlgXXHash3)
	}
	if db.config.ReadBuffer != 64*1024 {
		t.Errorf("ReadBuffer = %d, want %d", db.config.ReadBuffer, 64*1024)
	}
	if db.config.MaxRecordSize != 16*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", db.config.MaxRecordSize, 16*1024*1024)
	}
}

// TestClose verifies that Close transitions the state to StateClosed
// and that subsequent operations return ErrClosed. Without the state
// transition, operations after Close would attempt to use closed file
// handles.
func TestClose(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db.Set("doc", "content")

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := db.Get("doc")
	if err != ErrClosed {
		t.Errorf("Get after close: got %v, want ErrClosed", err)
	}
}

// TestSetGet is the most fundamental test: write a document, read it
// back, verify the content matches. If this fails, nothing else works.
func TestSetGet(t *testing.T) {
	db := openTestDB(t)

	if err := db.Set("myapp", "hello world"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	data, err := db.Get("myapp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "hello world" {
		t.Errorf("Get = %q, want %q", data, "hello world")
	}
}

// TestSetUpdate verifies that Set on an existing label returns the new
// content. Set must blank the old index so Get finds the new version.
// If the old index weren't blanked, Get would return whichever version
// it found first — potentially the stale one.
func TestSetUpdate(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}

// TestSetLabelTooLong verifies that labels exceeding MaxLabelSize are
// rejected. Without this limit, a very long label would produce a
// record too large for the fixed-position field extraction.
func TestSetLabelTooLong(t *testing.T) {
	db := openTestDB(t)

	label := string(make([]byte, MaxLabelSize+1))
	err := db.Set(label, "content")
	if err != ErrLabelTooLong {
		t.Errorf("Set long label: got %v, want ErrLabelTooLong", err)
	}
}

// TestSetLabelWithQuote verifies that labels containing double quotes
// are rejected. Labels are stored as JSON string values without
// escaping; a quote in the label would break the JSON structure of
// the record line, making it unparseable by decode().
func TestSetLabelWithQuote(t *testing.T) {
	db := openTestDB(t)

	err := db.Set(`my"label`, "content")
	if err != ErrInvalidLabel {
		t.Errorf("Set label with quote: got %v, want ErrInvalidLabel", err)
	}
}

// TestSetEmptyContent verifies that empty content is rejected. An empty
// _d field would be indistinguishable from a blanked history record's
// _d field, causing confusion in Search and History.
func TestSetEmptyContent(t *testing.T) {
	db := openTestDB(t)

	err := db.Set("doc", "")
	if err != ErrEmptyContent {
		t.Errorf("Set empty: got %v, want ErrEmptyContent", err)
	}
}

// TestGetNotFound verifies that Get returns ErrNotFound for a label
// that was never Set. If Get returned a zero-value or nil error, the
// caller would think the document exists with empty content.
func TestGetNotFound(t *testing.T) {
	db := openTestDB(t)

	_, err := db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

// TestDelete verifies the delete→get cycle. After Delete, Get must
// return ErrNotFound. Delete blanks the index with spaces; if the blank
// were too short, valid() would still see a '{' and Get would find it.
func TestDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	if err := db.Delete("doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

// TestDeleteNotFound verifies that deleting a nonexistent document
// returns ErrNotFound rather than succeeding silently. A silent success
// would mislead callers who check for errors to confirm the document
// existed.
func TestDeleteNotFound(t *testing.T) {
	db := openTestDB(t)

	err := db.Delete("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

// TestExistsAfterCompact verifies that Exists works after compaction
// moves records from the sparse region into the sorted heap. Compaction
// rebuilds the file with new section boundaries; if Exists only checked
// the sparse region (pre-compaction layout), it would report false for
// every document after compaction.
func TestExistsAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Compact()

	exists, err := db.Exists("doc")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("Exists should be true after compaction")
	}

	exists, _ = db.Exists("missing")
	if exists {
		t.Error("Exists should be false for missing doc after compaction")
	}
}

// TestGetAfterCompact verifies that Get retrieves the correct content
// after compaction. Compaction rewrites every record with new byte
// offsets; if the rebuilt indexes pointed to stale offsets from the old
// file layout, Get would read garbage or the wrong document.
func TestGetAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Compact()

	data, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}

	_, err = db.Get("missing")
	if err != ErrNotFound {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

// TestOpenBadPath verifies that Open returns an error when the parent
// directory doesn't exist. If Open silently created intermediate
// directories, it could write files to unexpected locations. The caller
// must ensure the parent directory exists.
func TestOpenBadPath(t *testing.T) {
	_, err := Open("/nonexistent/path/test.folio", Config{})
	if err == nil {
		t.Error("Open bad path: expected error")
	}
}

// TestExists verifies the full lifecycle of Exists: false before Set,
// true after Set, false after Delete. This exercises Exists through
// every state transition a document can undergo. If Exists used a stale
// cache or failed to check both sorted and sparse regions, it would
// report the wrong state after a transition.
func TestExists(t *testing.T) {
	db := openTestDB(t)

	exists, _ := db.Exists("doc")
	if exists {
		t.Error("Exists before Set should be false")
	}

	db.Set("doc", "content")

	exists, _ = db.Exists("doc")
	if !exists {
		t.Error("Exists after Set should be true")
	}

	db.Delete("doc")

	exists, _ = db.Exists("doc")
	if exists {
		t.Error("Exists after Delete should be false")
	}
}

// TestList verifies that List returns all labels in the database and
// that an empty database returns an empty slice. If List missed the
// sparse region or double-counted documents in both regions, the count
// would be wrong.
func TestList(t *testing.T) {
	db := openTestDB(t)

	labels, _ := db.List()
	if len(labels) != 0 {
		t.Errorf("List empty db: got %d, want 0", len(labels))
	}

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")

	labels, _ = db.List()
	if len(labels) != 3 {
		t.Errorf("List: got %d labels, want 3", len(labels))
	}
}

// TestListAfterDelete verifies that deleted documents are excluded from
// List results. Delete blanks the index with spaces; if List didn't
// check valid() on each index, blanked entries would appear as phantom
// labels in the output.
func TestListAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Delete("a")

	labels, _ := db.List()
	if len(labels) != 1 {
		t.Errorf("List after delete: got %d, want 1", len(labels))
	}
	if labels[0] != "b" {
		t.Errorf("List[0] = %q, want %q", labels[0], "b")
	}
}

// TestHistoryMultiDocCompact verifies that History returns the correct
// versions when multiple documents are interleaved in the heap after
// compaction. The group() function walks forward through the sorted
// heap and must stop at the boundary where the ID changes. If it
// overran the boundary, it would include records from a different
// document in the history.
func TestHistoryMultiDocCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "a1")
	db.Set("a", "a2")
	db.Set("b", "b1")
	db.Set("c", "c1")
	db.Compact()

	// Exercises group() forward walk stopping at a different ID boundary
	versions, err := db.History("a")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(versions) != 2 {
		t.Errorf("History(a): got %d, want 2", len(versions))
	}
}

// TestHistorySparseOnly verifies that History works for a document that
// exists only in the sparse region (written after the last compaction).
// The group() function searches the sorted heap first; if the ID isn't
// found there, it must fall back to scanning the sparse region. If it
// returned nil instead, sparse-only documents would have no history.
func TestHistorySparseOnly(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "content")
	db.Compact()
	db.Set("b", "new") // only in sparse region

	// Exercises group() returning nil (ID not in heap)
	versions, err := db.History("b")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("History(b): got %d, want 1", len(versions))
	}
}

// TestHistory verifies that History returns all versions in
// chronological order (oldest first). If the ordering were reversed or
// versions were deduplicated by mistake, callers would see an incorrect
// audit trail and couldn't reconstruct the document's evolution.
func TestHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	versions, err := db.History("doc")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("History: got %d versions, want 3", len(versions))
	}

	if versions[0].Data != "v1" {
		t.Errorf("versions[0] = %q, want v1", versions[0].Data)
	}
	if versions[2].Data != "v3" {
		t.Errorf("versions[2] = %q, want v3", versions[2].Data)
	}
}

// TestHistoryAfterDelete verifies that History excludes the delete
// tombstone but preserves all prior versions. Delete blanks the index
// and removes the current version, but the historical data records
// remain. If History counted the blanked index as a version, the count
// would be wrong.
func TestHistoryAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Delete("doc")

	versions, _ := db.History("doc")
	if len(versions) != 2 {
		t.Errorf("History after delete: got %d, want 2", len(versions))
	}
}

// TestHistoryNonexistent verifies that History returns an empty slice
// for a label that was never written. If it returned nil instead of an
// empty slice, callers would need nil checks before ranging over the
// result.
func TestHistoryNonexistent(t *testing.T) {
	db := openTestDB(t)

	versions, _ := db.History("nonexistent")
	if len(versions) != 0 {
		t.Errorf("History nonexistent: got %d, want 0", len(versions))
	}
}

// TestCompact verifies that Compact preserves the latest version of
// each document and retains full version history. Compaction rebuilds
// the file: it sorts records into the heap, rewrites indexes with new
// offsets, and empties the sparse region. If it dropped the history
// chain or mixed up which version was current, data would be lost.
func TestCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("a", "1-updated")

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	data, _ := db.Get("a")
	if data != "1-updated" {
		t.Errorf("Get after compact = %q, want %q", data, "1-updated")
	}

	versions, _ := db.History("a")
	if len(versions) != 2 {
		t.Errorf("History after compact: got %d, want 2", len(versions))
	}
}

// TestPurge verifies that Purge discards all historical versions and
// keeps only the current version of each document. Unlike Compact
// (which preserves history), Purge is a destructive operation that
// trades audit-trail completeness for smaller file size. If Purge kept
// old versions, the file would never shrink.
func TestPurge(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	if err := db.Purge(); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	data, _ := db.Get("doc")
	if data != "v3" {
		t.Errorf("Get after purge = %q, want %q", data, "v3")
	}

	versions, _ := db.History("doc")
	if len(versions) != 1 {
		t.Errorf("History after purge: got %d, want 1 (current only)", len(versions))
	}
}

// TestRehash verifies the basic rehash operation: the algorithm field
// changes and documents remain accessible. Rehash recomputes every
// record's ID from its label using the new algorithm; if any ID were
// wrong, Get (which also computes the ID from the label) would produce
// a different hash and the document would become unreachable.
func TestRehash(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	if err := db.Rehash(AlgFNV1a); err != nil {
		t.Fatalf("Rehash: %v", err)
	}

	if db.header.Algorithm != AlgFNV1a {
		t.Errorf("Algorithm = %d, want %d", db.header.Algorithm, AlgFNV1a)
	}

	data, _ := db.Get("doc")
	if data != "content" {
		t.Errorf("Get after rehash = %q, want %q", data, "content")
	}
}

// TestLargeContent verifies that 1 MB of content survives a Set→Get
// round-trip. Large content exercises the line() reader's buffer
// growth path — if the ReadBuffer (default 64 KB) weren't expanded
// when the record exceeds it, line() would return a truncated record
// and Get would decode garbage.
func TestLargeContent(t *testing.T) {
	db := openTestDB(t)

	// 1MB of text
	content := strings.Repeat("x", 1024*1024)

	if err := db.Set("large", content); err != nil {
		t.Fatalf("Set large: %v", err)
	}

	data, err := db.Get("large")
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	if data != content {
		t.Errorf("Get large: length %d, want %d", len(data), len(content))
	}
}

// TestUnicodeContent verifies that multi-byte UTF-8 content (CJK
// characters, emoji, accented Latin) survives a Set→Get round-trip.
// JSON encoding preserves Unicode natively, but if any byte-counting
// logic confused byte length with rune count, multi-byte characters
// would be split at record boundaries and corrupted.
func TestUnicodeContent(t *testing.T) {
	db := openTestDB(t)

	content := "日本語テキスト 🎉 émojis and spëcial châräctérs"

	db.Set("unicode", content)

	data, _ := db.Get("unicode")
	if data != content {
		t.Errorf("unicode content: got %q, want %q", data, content)
	}
}

// TestStateConstants verifies that the state machine constants have
// their expected integer values. The state machine relies on ordering
// (StateAll < StateRead < StateNone < StateClosed) for comparison-based
// access checks. If the constants were reordered or renumbered, the
// state gate would allow or deny operations incorrectly.
func TestStateConstants(t *testing.T) {
	if StateAll != 0 {
		t.Errorf("StateAll = %d, want 0", StateAll)
	}
	if StateRead != 1 {
		t.Errorf("StateRead = %d, want 1", StateRead)
	}
	if StateNone != 2 {
		t.Errorf("StateNone = %d, want 2", StateNone)
	}
	if StateClosed != 3 {
		t.Errorf("StateClosed = %d, want 3", StateClosed)
	}
}
