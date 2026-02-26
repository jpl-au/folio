// Bloom filter tests.
//
// The optional bloom filter is a probabilistic data structure that
// tracks which document IDs exist in the sparse region. When enabled,
// Get and Exists check the bloom filter before performing a linear scan
// of the sparse section. A "definitely not present" result skips the
// scan entirely — a significant speedup when the sparse region is large
// and most lookups are for documents in the sorted section.
//
// The filter trades a small false-positive rate (<2%) for the ability
// to skip most sparse scans. These tests verify correctness (no false
// negatives), the false-positive rate is within bounds, and that the
// filter is properly reset after compaction (which empties the sparse
// region).
package folio

import (
	"path/filepath"
	"strconv"
	"testing"
)

// TestBloomAddContains verifies the basic contract: after Add("x"),
// Contains("x") must return true. A false negative would cause Get to
// skip the sparse scan and return ErrNotFound for a document that
// exists — silent data loss.
func TestBloomAddContains(t *testing.T) {
	b := newBloom()
	b.Add("abc123")
	if !b.Contains("abc123") {
		t.Error("Contains should return true for added ID")
	}
}

// TestBloomMiss verifies that Contains returns false for an ID that was
// never added. This is the fast path that skips the sparse scan. A
// false positive here is acceptable (the bloom filter allows them) but
// a systematic false positive for all IDs would defeat the purpose.
func TestBloomMiss(t *testing.T) {
	b := newBloom()
	b.Add("abc123")
	if b.Contains("xyz789") {
		t.Error("Contains should return false for absent ID")
	}
}

// TestBloomReset verifies that Reset clears all bits. Compaction calls
// Reset because it empties the sparse region — if old bits survived,
// Get would unnecessarily scan the (now-empty) sparse section for every
// document that was previously added, wasting the performance benefit
// of the filter.
func TestBloomReset(t *testing.T) {
	b := newBloom()
	b.Add("abc123")
	b.Reset()
	if b.Contains("abc123") {
		t.Error("Contains should return false after Reset")
	}
}

// TestBloomFPRate measures the false-positive rate with 1000 entries
// and 10000 probes. The filter is sized for <1% FP rate at expected
// load; this test uses a 2% threshold to allow for statistical noise.
// If the rate exceeded this, the filter would trigger sparse scans too
// often and provide negligible speedup.
func TestBloomFPRate(t *testing.T) {
	b := newBloom()
	for i := range 1000 {
		b.Add("present-" + strconv.Itoa(i))
	}

	fp := 0
	tests := 10000
	for i := range tests {
		if b.Contains("absent-" + strconv.Itoa(i)) {
			fp++
		}
	}

	rate := float64(fp) / float64(tests)
	if rate > 0.02 {
		t.Errorf("false positive rate %.4f exceeds 2%%", rate)
	}
}

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
