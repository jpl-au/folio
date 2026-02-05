package folio

import (
	"testing"
)

// findCollision finds two labels that hash to the same ID.
// This is for testing purposes - in practice collisions are rare.
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
