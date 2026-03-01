// Bloom filter integration tests.
//
// These verify that the bloom filter is correctly wired into the
// database operations: Get, Exists, Compact, and the disabled path.
package folio

import (
	"path/filepath"
	"testing"
)

// TestGetBloomSkipsSparse exercises the bloom filter integration in Get.
// With the filter enabled, Get("nonexistent") should return ErrNotFound
// without scanning the sparse region. The test also verifies that a
// present document is still found — the filter must have no false
// negatives.
func TestGetBloomSkipsSparse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{BloomFilter: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Set("doc1", "content1")

	_, err = db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get miss: got %v, want ErrNotFound", err)
	}

	// Verify hit still works
	data, err := db.Get("doc1")
	if err != nil {
		t.Fatalf("Get hit: %v", err)
	}
	if data != "content1" {
		t.Errorf("Get = %q, want %q", data, "content1")
	}
}

// TestExistsBloomSkipsSparse exercises the bloom filter integration in
// Exists. Same principle as TestGetBloomSkipsSparse but for the Exists
// code path, which has its own bloom check.
func TestExistsBloomSkipsSparse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{BloomFilter: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Set("doc1", "content1")

	exists, err := db.Exists("nonexistent")
	if err != nil {
		t.Fatalf("Exists miss: %v", err)
	}
	if exists {
		t.Error("Exists should return false for absent key")
	}

	exists, err = db.Exists("doc1")
	if err != nil {
		t.Fatalf("Exists hit: %v", err)
	}
	if !exists {
		t.Error("Exists should return true for present key")
	}
}

// TestBloomAfterCompact verifies that the bloom filter is reset during
// compaction and correctly tracks new writes afterward. Compaction moves
// all sparse records into the sorted section, so the sparse region is
// empty. If the bloom weren't reset, it would contain stale entries for
// documents now in sorted, causing unnecessary sparse scans.
func TestBloomAfterCompact(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{BloomFilter: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Set("doc1", "v1")
	db.Set("doc2", "v2")

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// After compact, bloom is reset (sparse is empty)
	// New writes should be tracked
	db.Set("doc3", "v3")

	data, err := db.Get("doc3")
	if err != nil {
		t.Fatalf("Get doc3: %v", err)
	}
	if data != "v3" {
		t.Errorf("Get = %q, want %q", data, "v3")
	}

	// Sorted docs still found via binary search
	data, err = db.Get("doc1")
	if err != nil {
		t.Fatalf("Get doc1: %v", err)
	}
	if data != "v1" {
		t.Errorf("Get = %q, want %q", data, "v1")
	}
}

// TestBloomDisabled verifies that the database works correctly when the
// bloom filter is not enabled. The bloom field must be nil (not an empty
// filter), and all operations must work without nil-pointer panics in
// the bloom check paths.
func TestBloomDisabled(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{BloomFilter: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if db.bloom != nil {
		t.Error("bloom should be nil when disabled")
	}

	db.Set("doc1", "content1")

	data, err := db.Get("doc1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "content1" {
		t.Errorf("Get = %q, want %q", data, "content1")
	}

	_, err = db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get miss: got %v, want ErrNotFound", err)
	}
}
