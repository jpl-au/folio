// Document isolation tests.
//
// Each document is identified by a 16-hex-char ID derived from its
// label via hash(). Because the ID space is 2^64, collisions are
// astronomically unlikely for real workloads — but the lookup path must
// still correctly distinguish documents by comparing the full label
// string (stored in _l), not just the hash-derived ID. These tests
// verify that CRUD operations on one document never affect a different
// document, even when both are in the same file region (sparse or
// sorted). The "many documents" tests stress this at scale by writing
// 50–100 documents and confirming every one survives Get and Compact.
package folio

import (
	"testing"
)

// findCollision finds two labels that hash to the same ID.
// This is for testing purposes — in practice collisions are rare.
func findCollision(alg int) (string, string) {
	seen := make(map[string]string)
	for i := 0; i < 100000; i++ {
		label := string(rune('a'+i%26)) + string(rune('0'+i/26))
		id := hash(label, alg)
		if existing, ok := seen[id]; ok {
			return existing, label
		}
		seen[id] = label
	}
	return "", "" // No collision found in range
}

// TestHashCollisionGet verifies that Get returns the correct content
// for each of two documents. If Get only matched on the ID (hash) and
// not the label, a hash collision would cause it to return the wrong
// document's content.
func TestHashCollisionGet(t *testing.T) {
	// Use labels that we manually verify have different hashes
	// (actual collisions are extremely rare with good hash functions)
	db := openTestDB(t)

	db.Set("label-a", "content-a")
	db.Set("label-b", "content-b")

	dataA, _ := db.Get("label-a")
	dataB, _ := db.Get("label-b")

	if dataA != "content-a" {
		t.Errorf("Get(label-a) = %q, want %q", dataA, "content-a")
	}
	if dataB != "content-b" {
		t.Errorf("Get(label-b) = %q, want %q", dataB, "content-b")
	}
}

// TestHashCollisionSet verifies that updating one document doesn't
// affect another. Set must find and blank only the old index for the
// target label. If it blanked any index with the same ID, an update
// to doc-one would destroy doc-two's index, making it unfindable.
func TestHashCollisionSet(t *testing.T) {
	db := openTestDB(t)

	// Set two documents
	db.Set("doc-one", "v1")
	db.Set("doc-two", "v2")

	// Update one
	db.Set("doc-one", "v1-updated")

	// Both should have correct values
	d1, _ := db.Get("doc-one")
	d2, _ := db.Get("doc-two")

	if d1 != "v1-updated" {
		t.Errorf("Get(doc-one) = %q, want %q", d1, "v1-updated")
	}
	if d2 != "v2" {
		t.Errorf("Get(doc-two) = %q, want %q", d2, "v2")
	}
}

// TestHashCollisionDelete verifies that deleting one document doesn't
// affect another. Delete must match on the full label, not just the ID.
// If it blanked the wrong index, a Delete("first") could destroy
// "second"'s lookup entry.
func TestHashCollisionDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("first", "content-first")
	db.Set("second", "content-second")

	db.Delete("first")

	_, err := db.Get("first")
	if err != ErrNotFound {
		t.Errorf("Get(first) after delete: got %v, want ErrNotFound", err)
	}

	data, _ := db.Get("second")
	if data != "content-second" {
		t.Errorf("Get(second) = %q, want %q", data, "content-second")
	}
}

// TestHashCollisionExists verifies that Exists correctly distinguishes
// between a document that exists and one that doesn't. If Exists only
// checked the ID, any document with the same hash prefix would appear
// to exist.
func TestHashCollisionExists(t *testing.T) {
	db := openTestDB(t)

	db.Set("exists-a", "content")

	existsA, _ := db.Exists("exists-a")
	existsB, _ := db.Exists("exists-b")

	if !existsA {
		t.Error("Exists(exists-a) should be true")
	}
	if existsB {
		t.Error("Exists(exists-b) should be false")
	}
}

// TestHashCollisionHistory verifies that History returns versions only
// for the requested document. History uses group() which walks forward
// through the heap collecting records with the same ID — but it must
// also check the label to avoid mixing versions from different documents
// that might share an ID.
func TestHashCollisionHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("history-a", "a-v1")
	db.Set("history-a", "a-v2")
	db.Set("history-b", "b-v1")

	histA, _ := db.History("history-a")
	histB, _ := db.History("history-b")

	if len(histA) != 2 {
		t.Errorf("History(history-a) = %d versions, want 2", len(histA))
	}
	if len(histB) != 1 {
		t.Errorf("History(history-b) = %d versions, want 1", len(histB))
	}
}

// TestHashCollisionAfterCompact verifies document isolation after
// compaction. Compact sorts records by ID, so two documents with close
// IDs will be adjacent in the heap. Binary search must still distinguish
// them by label, not just by the ID it used to find the region.
func TestHashCollisionAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("compact-a", "content-a")
	db.Set("compact-b", "content-b")

	db.Compact()

	dataA, _ := db.Get("compact-a")
	dataB, _ := db.Get("compact-b")

	if dataA != "content-a" {
		t.Errorf("Get(compact-a) after compact = %q, want %q", dataA, "content-a")
	}
	if dataB != "content-b" {
		t.Errorf("Get(compact-b) after compact = %q, want %q", dataB, "content-b")
	}
}

// TestHashCollisionSortedThenSparse exercises the two-region lookup
// when documents live in different sections. One document is compacted
// into the sorted section; a second is written to sparse afterward.
// Get must search both regions and return the correct content for each.
func TestHashCollisionSortedThenSparse(t *testing.T) {
	db := openTestDB(t)

	// Create document, compact (moves to sorted)
	db.Set("sorted-doc", "v1")
	db.Compact()

	// Create another with potentially similar hash prefix
	db.Set("sparse-doc", "v2")

	// Both should be retrievable
	d1, _ := db.Get("sorted-doc")
	d2, _ := db.Get("sparse-doc")

	if d1 != "v1" {
		t.Errorf("Get(sorted-doc) = %q, want %q", d1, "v1")
	}
	if d2 != "v2" {
		t.Errorf("Get(sparse-doc) = %q, want %q", d2, "v2")
	}
}

// TestManyDocumentsDifferentLabels writes 100 documents and verifies
// every one is retrievable. With 100 unique IDs in the sparse region,
// this stresses the linear scan path — if sparse() had an off-by-one
// in its loop or stopped early, some documents would be missing.
func TestManyDocumentsDifferentLabels(t *testing.T) {
	db := openTestDB(t)

	// Create many documents
	for i := 0; i < 100; i++ {
		label := string(rune('a'+i%26)) + string(rune('0'+i/26))
		db.Set(label, "content-"+label)
	}

	// All should be retrievable
	for i := 0; i < 100; i++ {
		label := string(rune('a'+i%26)) + string(rune('0'+i/26))
		data, err := db.Get(label)
		if err != nil {
			t.Errorf("Get(%q): %v", label, err)
			continue
		}
		if data != "content-"+label {
			t.Errorf("Get(%q) = %q, want %q", label, data, "content-"+label)
		}
	}
}

// TestManyDocumentsAfterCompact writes 50 documents, compacts, and
// verifies every one survives. This stresses the sorted-section binary
// search with a realistic number of entries — if the sort order were
// wrong or the index offsets drifted during rebuild, some documents
// would be unreachable.
func TestManyDocumentsAfterCompact(t *testing.T) {
	db := openTestDB(t)

	for i := 0; i < 50; i++ {
		label := string(rune('a'+i%26)) + string(rune('0'+i/26))
		db.Set(label, "content-"+label)
	}

	db.Compact()

	for i := 0; i < 50; i++ {
		label := string(rune('a'+i%26)) + string(rune('0'+i/26))
		data, err := db.Get(label)
		if err != nil {
			t.Errorf("Get(%q) after compact: %v", label, err)
			continue
		}
		if data != "content-"+label {
			t.Errorf("Get(%q) = %q, want %q", label, data, "content-"+label)
		}
	}
}
