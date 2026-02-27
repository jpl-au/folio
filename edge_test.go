// Boundary condition and edge case tests.
//
// These tests exercise the boundary conditions that normal usage rarely
// hits: maximum-length labels, empty databases, crash recovery, double
// close, operations after close, and the interaction between sorted and
// sparse regions after compaction. Each test targets a specific "what if"
// scenario that, if mishandled, would cause either data loss, a panic,
// or a confusing error message.
package folio

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLabelExactly256Bytes verifies that a label at exactly MaxLabelSize
// is accepted. The validation uses > (not >=), so an off-by-one in the
// comparison would reject the maximum valid length, silently reducing
// the effective limit by one byte.
func TestLabelExactly256Bytes(t *testing.T) {
	db := openTestDB(t)

	label := strings.Repeat("x", MaxLabelSize)
	err := db.Set(label, "content")
	if err != nil {
		t.Errorf("Set with 256-byte label: %v", err)
	}

	data, err := db.Get(label)
	if err != nil {
		t.Errorf("Get: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

// TestLabelWithPathSeparators verifies that labels containing '/' and
// '\' are accepted and round-trip correctly. Labels are stored as JSON
// string values — path separators are valid JSON characters and must
// not be rejected or escaped differently. If Set sanitised path
// separators, users who use hierarchical naming (e.g. "config/db/host")
// would find their documents stored under a mangled label.
func TestLabelWithPathSeparators(t *testing.T) {
	db := openTestDB(t)

	label := "a/b/c\\d"
	err := db.Set(label, "content")
	if err != nil {
		t.Errorf("Set with path separators: %v", err)
	}

	data, _ := db.Get(label)
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

// TestEmptyLabel verifies that an empty string is rejected with
// ErrInvalidLabel. An empty label would produce a record with _l:""
// which the label() extractor returns as "" — the same value it returns
// for missing or corrupt labels. This would make the document
// indistinguishable from a damaged record during compaction.
func TestEmptyLabel(t *testing.T) {
	db := openTestDB(t)

	err := db.Set("", "content")
	if err != ErrInvalidLabel {
		t.Errorf("Set empty label: got %v, want ErrInvalidLabel", err)
	}
}

// TestFreshDatabaseEdgeCases exercises every read and maintenance
// operation on an empty database. A fresh database has only a header;
// heap and index offsets are both zero, and the sparse region starts at
// HeaderSize with tail == HeaderSize (no records). Every operation must
// handle this gracefully: Get and Delete return ErrNotFound, List and
// History return empty slices, Compact and Purge succeed as no-ops. If
// any of these panicked on empty input (e.g. a binary search on a
// zero-length range), the database would be unusable until the first
// Set, which is a poor developer experience.
func TestFreshDatabaseEdgeCases(t *testing.T) {
	db := openTestDB(t)

	// Get on empty
	_, err := db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get on empty: got %v, want ErrNotFound", err)
	}

	// Delete on empty
	err = db.Delete("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Delete on empty: got %v, want ErrNotFound", err)
	}

	// List on empty
	labels, _ := collect(db.List())
	if len(labels) != 0 {
		t.Errorf("List on empty: got %d, want 0", len(labels))
	}

	// Exists on empty
	exists, _ := db.Exists("nonexistent")
	if exists {
		t.Error("Exists on empty should be false")
	}

	// History on empty
	versions, _ := collect(db.History("nonexistent"))
	if len(versions) != 0 {
		t.Errorf("History on empty: got %d, want 0", len(versions))
	}

	// Compact on empty
	err = db.Compact()
	if err != nil {
		t.Errorf("Compact on empty: %v", err)
	}

	// Purge on empty
	err = db.Purge()
	if err != nil {
		t.Errorf("Purge on empty: %v", err)
	}
}

// TestCrashRecoveryDirtyFlag simulates a crash by setting the dirty
// flag and closing file handles without calling Close. On reopen, Open
// must detect Error=1 in the header and run Repair automatically. If
// crash recovery didn't fire, the file might have a partially-written
// record at the end, causing subsequent reads to hit JSON parse errors
// or return incomplete data.
func TestCrashRecoveryDirtyFlag(t *testing.T) {
	dir := t.TempDir()

	// Create DB and set dirty flag
	db1, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db1.Set("doc", "content")
	// Don't close cleanly - leave dirty flag set

	// Manually set dirty flag and close handles
	dirty(db1.writer, true)
	db1.writer.Sync()
	db1.reader.Close()
	db1.writer.Close()
	db1.root.Close()

	// Reopen - should trigger repair
	db2, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer db2.Close()

	// Data should still be accessible
	data, err := db2.Get("doc")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}

	// Dirty flag should be clear
	if db2.header.Error != 0 {
		t.Error("dirty flag should be cleared after recovery")
	}
}

// TestCrashRecoveryTmpFile simulates a crash during compaction by
// leaving an orphan .tmp file alongside the database. Repair writes to
// a .tmp file then renames it over the original. If the process dies
// between the write and the rename, the .tmp file is left behind. On
// next Open, the presence of a .tmp file triggers cleanup and repair
// to ensure the database is consistent.
func TestCrashRecoveryTmpFile(t *testing.T) {
	dir := t.TempDir()

	// Create DB
	db1, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db1.Set("doc", "content")
	db1.Close()

	// Create orphan .tmp file
	tmpPath := filepath.Join(dir, "test.folio.tmp")
	os.WriteFile(tmpPath, []byte("garbage"), 0644)

	// Reopen - should delete .tmp and repair
	db2, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("Open with tmp file: %v", err)
	}
	defer db2.Close()

	// tmp file should be deleted
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error(".tmp file should be deleted")
	}

	// Data should still be accessible
	data, _ := db2.Get("doc")
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

// TestDoubleClose verifies that calling Close twice returns an error
// on the second call rather than panicking. In production, a deferred
// Close can run after an explicit Close in an error path. If the second
// Close tried to close already-closed file handles, it would either
// panic or close a file descriptor that has been reused by another
// goroutine — a subtle and dangerous bug.
func TestDoubleClose(t *testing.T) {
	db := openTestDB(t)

	err := db.Close()
	if err != nil {
		t.Fatalf("First close: %v", err)
	}

	// Second close should return error (handles already closed)
	err = db.Close()
	if err == nil {
		t.Error("Second close should return error")
	}
}

// TestOperationsAfterClose verifies that every public method returns
// ErrClosed after the database is closed. Without this guard, operations
// would attempt to read from or write to closed file handles, producing
// OS-level errors that are difficult for callers to interpret. Returning
// a dedicated ErrClosed makes the cause immediately clear.
func TestOperationsAfterClose(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := db.Get("doc")
	if err != ErrClosed {
		t.Errorf("Get after close: got %v, want ErrClosed", err)
	}

	err = db.Set("doc", "new")
	if err != ErrClosed {
		t.Errorf("Set after close: got %v, want ErrClosed", err)
	}

	err = db.Delete("doc")
	if err != ErrClosed {
		t.Errorf("Delete after close: got %v, want ErrClosed", err)
	}

	_, err = db.Exists("doc")
	if err != ErrClosed {
		t.Errorf("Exists after close: got %v, want ErrClosed", err)
	}

	_, err = collect(db.List())
	if err != ErrClosed {
		t.Errorf("List after close: got %v, want ErrClosed", err)
	}

	_, err = collect(db.History("doc"))
	if err != ErrClosed {
		t.Errorf("History after close: got %v, want ErrClosed", err)
	}
}

// TestDeleteFromSparse verifies deletion when the document exists only
// in the sparse region (no compaction has occurred). Delete must find
// the index via linear scan and blank it. If Delete only searched the
// sorted section, it would return ErrNotFound for any document that
// hasn't been compacted yet.
func TestDeleteFromSparse(t *testing.T) {
	db := openTestDB(t)

	// Add document (goes to sparse section since no compaction)
	db.Set("doc", "content")

	// Delete from sparse
	err := db.Delete("doc")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

// TestDeleteFromSorted verifies deletion when the document is in the
// sorted section (after compaction). Delete must find the index via
// binary search and blank it. This is a different code path from
// sparse deletion — if the sorted-search path had a bug, documents
// would become undeletable after compaction.
func TestDeleteFromSorted(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Compact() // Moves to sorted section

	err := db.Delete("doc")
	if err != nil {
		t.Fatalf("Delete from sorted: %v", err)
	}

	_, err = db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

// TestExistsClosed verifies that Exists returns ErrClosed on a closed
// database. Exists shares the same read path as Get (blockRead + scan),
// so it must also check the closed state. Without this, Exists would
// attempt to read from a closed file handle.
func TestExistsClosed(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := db.Exists("doc")
	if err != ErrClosed {
		t.Errorf("Exists on closed: got %v, want ErrClosed", err)
	}
}

// TestHistoryAfterCompact verifies that History returns all versions
// after compaction. Before compaction, each version is a separate record
// in sparse. After compaction, older versions are compressed into the
// _h field of the current record in the heap. History must decompress
// _h and reconstruct the full version list. If decompression failed
// silently, History would return only the current version.
func TestHistoryAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")
	db.Compact()

	versions, err := collect(db.History("doc"))
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("History: got %d, want 3", len(versions))
	}
	if versions[0].Data != "v1" {
		t.Errorf("versions[0] = %q, want v1", versions[0].Data)
	}
	if versions[2].Data != "v3" {
		t.Errorf("versions[2] = %q, want v3", versions[2].Data)
	}
}

// TestHistoryMixedRegions exercises the case where some versions are
// in the sorted heap (from a previous compaction) and a newer version
// is in the sparse region (written after compaction). History must
// combine both sources: group() reads from the heap, and sparse()
// reads from the tail. If either source were skipped, History would
// return an incomplete version list.
func TestHistoryMixedRegions(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Compact()
	db.Set("doc", "v3")

	versions, err := collect(db.History("doc"))
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("History: got %d, want 3", len(versions))
	}
	if versions[2].Data != "v3" {
		t.Errorf("versions[2] = %q, want v3", versions[2].Data)
	}
}

// TestDeleteAfterCompactFromSorted verifies that deleting one document
// from the sorted section doesn't affect other documents. Delete blanks
// the target index with spaces; if the blank were too wide (e.g.
// extending into the next index line), it would corrupt the adjacent
// document's index and make it unfindable.
func TestDeleteAfterCompactFromSorted(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "content-a")
	db.Set("b", "content-b")
	db.Compact()

	if err := db.Delete("a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := db.Get("a")
	if err != ErrNotFound {
		t.Errorf("Get deleted: got %v, want ErrNotFound", err)
	}

	data, _ := db.Get("b")
	if data != "content-b" {
		t.Errorf("Get(b) = %q, want %q", data, "content-b")
	}
}

// TestSetUpdateSorted verifies the update path when the existing version
// is in the sorted section. Set must binary-search the sorted indexes to
// find the old index, blank it, then append the new version to sparse.
// If Set failed to blank the sorted index, both the old and new versions
// would have visible indexes, and Get would return whichever it found
// first — possibly the stale version.
func TestSetUpdateSorted(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Compact()
	db.Set("doc", "v2")

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}

	versions, _ := collect(db.History("doc"))
	if len(versions) != 2 {
		t.Errorf("History: got %d, want 2", len(versions))
	}
}

// TestSparseOverridesSorted verifies the lookup precedence rule: sparse
// results override sorted results. Get checks sparse first because it
// contains the most recent writes. If Get checked sorted first and
// returned immediately on a match, it would always return the pre-
// compaction version, ignoring any updates written after compaction.
func TestSparseOverridesSorted(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Compact() // v1 now in sorted section

	db.Set("doc", "v2") // v2 in sparse section

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q (sparse should override sorted)", data, "v2")
	}
}
