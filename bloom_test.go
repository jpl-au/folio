package folio

import (
	"path/filepath"
	"strconv"
	"testing"
)

func TestBloomAddContains(t *testing.T) {
	b := newBloom()
	b.Add("abc123")
	if !b.Contains("abc123") {
		t.Error("Contains should return true for added ID")
	}
}

func TestBloomMiss(t *testing.T) {
	b := newBloom()
	b.Add("abc123")
	if b.Contains("xyz789") {
		t.Error("Contains should return false for absent ID")
	}
}

func TestBloomReset(t *testing.T) {
	b := newBloom()
	b.Add("abc123")
	b.Reset()
	if b.Contains("abc123") {
		t.Error("Contains should return false after Reset")
	}
}

func TestBloomFPRate(t *testing.T) {
	b := newBloom()
	for i := 0; i < 1000; i++ {
		b.Add("present-" + strconv.Itoa(i))
	}

	fp := 0
	tests := 10000
	for i := 0; i < tests; i++ {
		if b.Contains("absent-" + strconv.Itoa(i)) {
			fp++
		}
	}

	rate := float64(fp) / float64(tests)
	if rate > 0.02 {
		t.Errorf("false positive rate %.4f exceeds 2%%", rate)
	}
}

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
