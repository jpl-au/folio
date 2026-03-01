// In-memory index tests.
//
// The in-memory index (Config.Index) maps hex IDs to index record
// offsets in a plain map[string]int64. It eliminates binary search
// and sparse scans, giving O(1) lookups with a single file read per
// Get. These tests verify that the index is correctly built at Open,
// maintained across writes, and rebuilt after Compact and Rehash.
package folio

import (
	"path/filepath"
	"testing"
)

func openIndexDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{Index: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestIndexGetSet exercises the basic Set→Get path with the index enabled.
func TestIndexGetSet(t *testing.T) {
	db := openIndexDB(t)

	db.Set("doc", "hello")

	data, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "hello" {
		t.Errorf("Get = %q, want %q", data, "hello")
	}
}

// TestIndexUpdate verifies that Set overwrites the index entry correctly.
func TestIndexUpdate(t *testing.T) {
	db := openIndexDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	data, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}

// TestIndexDelete verifies that Delete removes the index entry.
func TestIndexDelete(t *testing.T) {
	db := openIndexDB(t)

	db.Set("doc", "content")
	db.Delete("doc")

	_, err := db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

// TestIndexExists verifies that Exists uses the index.
func TestIndexExists(t *testing.T) {
	db := openIndexDB(t)

	db.Set("doc", "content")

	ok, err := db.Exists("doc")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Error("Exists = false, want true")
	}

	ok, err = db.Exists("missing")
	if err != nil {
		t.Fatalf("Exists(missing): %v", err)
	}
	if ok {
		t.Error("Exists(missing) = true, want false")
	}
}

// TestIndexRename verifies that Rename updates the index: old ID removed,
// new ID inserted.
func TestIndexRename(t *testing.T) {
	db := openIndexDB(t)

	db.Set("old", "content")
	if err := db.Rename("old", "new"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	_, err := db.Get("old")
	if err != ErrNotFound {
		t.Errorf("Get(old) after rename: got %v, want ErrNotFound", err)
	}

	data, err := db.Get("new")
	if err != nil {
		t.Fatalf("Get(new): %v", err)
	}
	if data != "content" {
		t.Errorf("Get(new) = %q, want %q", data, "content")
	}
}

// TestIndexAfterCompact verifies the index is rebuilt from the new file.
func TestIndexAfterCompact(t *testing.T) {
	db := openIndexDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("a", "1-updated")

	db.Compact()

	data, err := db.Get("a")
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if data != "1-updated" {
		t.Errorf("Get(a) = %q, want %q", data, "1-updated")
	}

	data, err = db.Get("b")
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if data != "2" {
		t.Errorf("Get(b) = %q, want %q", data, "2")
	}

	// Verify the index has the right number of entries.
	if len(db.index) != 2 {
		t.Errorf("len(index) = %d, want 2", len(db.index))
	}
}

// TestIndexDisabled verifies that db.index is nil when Config.Index is false.
func TestIndexDisabled(t *testing.T) {
	db := openTestDB(t)

	if db.index != nil {
		t.Error("index should be nil when Config.Index is false")
	}

	// All operations should still work without the index.
	db.Set("doc", "content")
	data, _ := db.Get("doc")
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

// TestIndexAfterRehash verifies the index is rebuilt with new IDs.
func TestIndexAfterRehash(t *testing.T) {
	db := openIndexDB(t)

	db.Set("doc", "content")

	if err := db.Rehash(AlgFNV1a); err != nil {
		t.Fatalf("Rehash: %v", err)
	}

	data, err := db.Get("doc")
	if err != nil {
		t.Fatalf("Get after rehash: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}
