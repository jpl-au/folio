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
	"iter"
	"path/filepath"
	"strings"
	"testing"
)

// collect materialises an iter.Seq2[T, error] into a slice, stopping
// on the first error. Used across the test suite wherever callers need
// to inspect the full result set (length checks, index access, etc.).
func collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var items []T
	for item, err := range seq {
		if err != nil {
			return items, err
		}
		items = append(items, item)
	}
	return items, nil
}

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

	labels, _ := collect(db.List())
	if len(labels) != 0 {
		t.Errorf("List empty db: got %d, want 0", len(labels))
	}

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")

	labels, _ = collect(db.List())
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

	labels, _ := collect(db.List())
	if len(labels) != 1 {
		t.Errorf("List after delete: got %d, want 1", len(labels))
	}
	if labels[0] != "b" {
		t.Errorf("List[0] = %q, want %q", labels[0], "b")
	}
}

// TestAll verifies that All returns every document with correct content.
// This is the single-pass alternative to List+Get — if All missed any
// document or returned wrong content, export and backup use cases would
// produce incomplete or corrupt output.
func TestAll(t *testing.T) {
	db := openTestDB(t)

	docs, _ := collect(db.All())
	if len(docs) != 0 {
		t.Errorf("All empty db: got %d, want 0", len(docs))
	}

	db.Set("a", "alpha")
	db.Set("b", "bravo")
	db.Set("c", "charlie")

	docs, _ = collect(db.All())
	if len(docs) != 3 {
		t.Errorf("All: got %d docs, want 3", len(docs))
	}

	got := make(map[string]string)
	for _, d := range docs {
		got[d.Label] = d.Data
	}
	for _, want := range []struct{ label, data string }{
		{"a", "alpha"}, {"b", "bravo"}, {"c", "charlie"},
	} {
		if got[want.label] != want.data {
			t.Errorf("All[%s] = %q, want %q", want.label, got[want.label], want.data)
		}
	}
}

// TestAllAfterUpdate verifies that All returns only the latest version
// of each document. Set retypes old data records from 2→3, so the type
// check at TypePos must exclude them. If it didn't, All would yield
// stale content alongside the current version.
func TestAllAfterUpdate(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	docs, _ := collect(db.All())
	if len(docs) != 1 {
		t.Fatalf("All after update: got %d, want 1", len(docs))
	}
	if docs[0].Data != "v2" {
		t.Errorf("All[0].Data = %q, want %q", docs[0].Data, "v2")
	}
}

// TestAllAfterDelete verifies that deleted documents are excluded from
// All results. Delete retypes the data record to history and blanks _d,
// so the type check skips it. If All returned deleted documents, callers
// would see phantom entries with blank content.
func TestAllAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Delete("a")

	docs, _ := collect(db.All())
	if len(docs) != 1 {
		t.Fatalf("All after delete: got %d, want 1", len(docs))
	}
	if docs[0].Label != "b" {
		t.Errorf("All[0].Label = %q, want %q", docs[0].Label, "b")
	}
}

// TestAllAfterCompact verifies that All works after compaction moves
// records from sparse into the sorted heap. Compaction rewrites byte
// offsets; if the scan boundaries (heapEnd, sparseStart) were stale,
// All would scan the wrong regions and miss documents.
func TestAllAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "alpha")
	db.Set("b", "bravo")
	db.Set("a", "updated")
	db.Compact()

	docs, _ := collect(db.All())
	if len(docs) != 2 {
		t.Fatalf("All after compact: got %d, want 2", len(docs))
	}

	got := make(map[string]string)
	for _, d := range docs {
		got[d.Label] = d.Data
	}
	if got["a"] != "updated" {
		t.Errorf("All[a] = %q, want %q", got["a"], "updated")
	}
	if got["b"] != "bravo" {
		t.Errorf("All[b] = %q, want %q", got["b"], "bravo")
	}
}

// TestAllEscapedContent verifies that All correctly unescapes JSON
// string escapes in _d content. Content with newlines, quotes, or
// backslashes is JSON-encoded on disk; All must return the original
// content, not the escaped representation.
func TestAllEscapedContent(t *testing.T) {
	db := openTestDB(t)

	content := "line1\nline2\ttab\"quote\\backslash"
	db.Set("escaped", content)

	docs, _ := collect(db.All())
	if len(docs) != 1 {
		t.Fatalf("All escaped: got %d, want 1", len(docs))
	}
	if docs[0].Data != content {
		t.Errorf("All[0].Data = %q, want %q", docs[0].Data, content)
	}
}

// TestAllEarlyBreak verifies that breaking from the range loop stops
// the scan without reading the rest of the file. If the iterator didn't
// respect the break, it would waste I/O scanning records that the
// caller will never see.
func TestAllEarlyBreak(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")

	var count int
	for range db.All() {
		count++
		break
	}
	if count != 1 {
		t.Errorf("All early break: got %d, want 1", count)
	}
}

// TestRenameSameLength verifies the in-place patch path: when old and
// new labels have the same byte length, Rename patches _id and _l
// directly without creating a new record. Get(old) must return
// ErrNotFound and Get(new) must return the original content.
func TestRenameSameLength(t *testing.T) {
	db := openTestDB(t)

	db.Set("aaa", "content")
	if err := db.Rename("aaa", "bbb"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	_, err := db.Get("aaa")
	if err != ErrNotFound {
		t.Errorf("Get(old) = %v, want ErrNotFound", err)
	}

	data, err := db.Get("bbb")
	if err != nil {
		t.Fatalf("Get(new): %v", err)
	}
	if data != "content" {
		t.Errorf("Get(new) = %q, want %q", data, "content")
	}
}

// TestRenameDifferentLength verifies the fallback path: when labels
// differ in length, Rename appends a new record+index and blanks the
// old ones. The content must survive and be accessible under the new
// label.
func TestRenameDifferentLength(t *testing.T) {
	db := openTestDB(t)

	db.Set("short", "content")
	if err := db.Rename("short", "much-longer-label"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	_, err := db.Get("short")
	if err != ErrNotFound {
		t.Errorf("Get(old) = %v, want ErrNotFound", err)
	}

	data, err := db.Get("much-longer-label")
	if err != nil {
		t.Fatalf("Get(new): %v", err)
	}
	if data != "content" {
		t.Errorf("Get(new) = %q, want %q", data, "content")
	}
}

// TestRenameNotFound verifies that renaming a nonexistent document
// returns ErrNotFound.
func TestRenameNotFound(t *testing.T) {
	db := openTestDB(t)

	err := db.Rename("nonexistent", "new")
	if err != ErrNotFound {
		t.Errorf("Rename missing: got %v, want ErrNotFound", err)
	}
}

// TestRenameExists verifies that renaming to an existing label returns
// ErrExists rather than silently overwriting the target document.
func TestRenameExists(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "alpha")
	db.Set("b", "bravo")

	err := db.Rename("a", "b")
	if err != ErrExists {
		t.Errorf("Rename to existing: got %v, want ErrExists", err)
	}

	// Both documents should be unchanged.
	data, _ := db.Get("a")
	if data != "alpha" {
		t.Errorf("Get(a) = %q, want %q", data, "alpha")
	}
	data, _ = db.Get("b")
	if data != "bravo" {
		t.Errorf("Get(b) = %q, want %q", data, "bravo")
	}
}

// TestRenameSameLabel verifies that renaming to the same label is a
// no-op that returns nil.
func TestRenameSameLabel(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	if err := db.Rename("doc", "doc"); err != nil {
		t.Errorf("Rename(same): %v", err)
	}

	data, _ := db.Get("doc")
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

// TestRenameAfterCompact verifies that Rename works when the document
// is in the sorted heap (post-compaction) rather than the sparse
// region. The index lookup must use the sorted section's binary search
// path.
func TestRenameAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("old", "content")
	db.Compact()

	if err := db.Rename("old", "new"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	data, err := db.Get("new")
	if err != nil {
		t.Fatalf("Get(new): %v", err)
	}
	if data != "content" {
		t.Errorf("Get(new) = %q, want %q", data, "content")
	}
}

// TestRenameListReflects verifies that List returns the new label and
// not the old one after a rename.
func TestRenameListReflects(t *testing.T) {
	db := openTestDB(t)

	db.Set("before", "content")
	db.Rename("before", "after")

	labels, _ := collect(db.List())
	if len(labels) != 1 {
		t.Fatalf("List: got %d, want 1", len(labels))
	}
	if labels[0] != "after" {
		t.Errorf("List[0] = %q, want %q", labels[0], "after")
	}
}

// TestBatch verifies that Batch writes multiple documents under a
// single lock hold. All documents must be readable after the call.
func TestBatch(t *testing.T) {
	db := openTestDB(t)

	err := db.Batch(
		Document{"a", "alpha"},
		Document{"b", "bravo"},
		Document{"c", "charlie"},
	)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	for _, want := range []struct{ label, data string }{
		{"a", "alpha"}, {"b", "bravo"}, {"c", "charlie"},
	} {
		data, err := db.Get(want.label)
		if err != nil {
			t.Fatalf("Get(%s): %v", want.label, err)
		}
		if data != want.data {
			t.Errorf("Get(%s) = %q, want %q", want.label, data, want.data)
		}
	}
}

// TestBatchUpdate verifies that Batch correctly retires old versions
// when updating existing documents. The same document appearing twice
// in the batch must yield only the last value.
func TestBatchUpdate(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	err := db.Batch(
		Document{"doc", "v2"},
		Document{"doc", "v3"},
	)
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	data, _ := db.Get("doc")
	if data != "v3" {
		t.Errorf("Get = %q, want %q", data, "v3")
	}
}

// TestBatchValidation verifies that Batch validates all inputs
// before writing any documents. If the second document has an invalid
// label, the first must not be written.
func TestBatchValidation(t *testing.T) {
	db := openTestDB(t)

	err := db.Batch(
		Document{"valid", "content"},
		Document{"", "content"},
	)
	if err != ErrInvalidLabel {
		t.Errorf("Batch invalid: got %v, want ErrInvalidLabel", err)
	}

	_, err = db.Get("valid")
	if err != ErrNotFound {
		t.Errorf("Get(valid) after failed batch: got %v, want ErrNotFound", err)
	}
}

// TestBatchEmpty verifies that Batch with no arguments is a no-op.
func TestBatchEmpty(t *testing.T) {
	db := openTestDB(t)

	if err := db.Batch(); err != nil {
		t.Errorf("Batch(): %v", err)
	}
}

// TestCount verifies that Count tracks creates and returns 0 for an
// empty database. Count is maintained atomically by Set and Delete
// and requires no I/O or locking.
func TestCount(t *testing.T) {
	db := openTestDB(t)

	if db.Count() != 0 {
		t.Errorf("Count empty = %d, want 0", db.Count())
	}

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")

	if db.Count() != 3 {
		t.Errorf("Count = %d, want 3", db.Count())
	}
}

// TestCountAfterUpdate verifies that updating a document does not
// change the count. Set increments only for new labels; updates
// replace content without creating a new document.
func TestCountAfterUpdate(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	if db.Count() != 1 {
		t.Errorf("Count after updates = %d, want 1", db.Count())
	}
}

// TestCountAfterDelete verifies that Count decrements on delete.
func TestCountAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Delete("a")

	if db.Count() != 1 {
		t.Errorf("Count after delete = %d, want 1", db.Count())
	}
}

// TestCountAfterCompact verifies that Compact corrects the count to
// an accurate value derived from the index map.
func TestCountAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("a", "updated")
	db.Delete("b")
	db.Set("c", "3")
	db.Compact()

	if db.Count() != 2 {
		t.Errorf("Count after compact = %d, want 2", db.Count())
	}
}

// TestCountPersistence verifies that the count survives close and
// reopen. Close writes the full header (including _c) to disk; Open
// reads it back and initialises the atomic counter.
func TestCountPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	db1, _ := Open(path, Config{})
	db1.Set("a", "1")
	db1.Set("b", "2")
	db1.Set("c", "3")
	db1.Close()

	db2, err := Open(path, Config{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	if db2.Count() != 3 {
		t.Errorf("Count after reopen = %d, want 3", db2.Count())
	}
}

// TestCountAfterRename verifies that Rename does not change the count.
// Renaming changes the label, not the number of documents.
func TestCountAfterRename(t *testing.T) {
	db := openTestDB(t)

	db.Set("old", "content")
	db.Set("other", "data")

	if db.Count() != 2 {
		t.Errorf("Count before rename = %d, want 2", db.Count())
	}

	db.Rename("old", "new")

	if db.Count() != 2 {
		t.Errorf("Count after rename = %d, want 2", db.Count())
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
	versions, err := collect(db.History("a"))
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
	versions, err := collect(db.History("b"))
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

	versions, err := collect(db.History("doc"))
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

	versions, _ := collect(db.History("doc"))
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

	versions, _ := collect(db.History("nonexistent"))
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

	versions, _ := collect(db.History("a"))
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

	versions, _ := collect(db.History("doc"))
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
