// Hash algorithm migration (Rehash) tests.
//
// Rehash rebuilds the entire database with a different hash algorithm.
// Every record's ID is recomputed from its label using the new
// algorithm, then the file is rebuilt with the new IDs. This is a
// destructive operation — if Rehash failed to recompute even one ID,
// that document would become unreachable because Get computes the ID
// from the label using the header's current algorithm.
//
// These tests verify: the algorithm field changes in the header, all
// documents remain accessible after migration, version history
// survives, the timestamp is updated, every pairwise algorithm
// migration works, and Rehash composes correctly with Compact.
package folio

import (
	"path/filepath"
	"testing"
)

// TestRehashChangesAlgorithm verifies that Rehash updates the header's
// Algorithm field. If it didn't, the next Get call would still use the
// old algorithm to compute IDs, producing different hashes from the
// records' stored IDs and making every document unfindable.
func TestRehashChangesAlgorithm(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	if db.header.Algorithm != AlgXXHash3 {
		t.Fatalf("initial algorithm = %d, want %d", db.header.Algorithm, AlgXXHash3)
	}

	db.Rehash(AlgFNV1a)

	if db.header.Algorithm != AlgFNV1a {
		t.Errorf("algorithm after rehash = %d, want %d", db.header.Algorithm, AlgFNV1a)
	}
}

// TestRehashDataStillAccessible verifies that every document is
// reachable after migration. Rehash recomputes each document's ID from
// its label — if the label were read incorrectly (e.g. not unescaped),
// the new ID would be wrong and the document would be lost.
func TestRehashDataStillAccessible(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "content-a")
	db.Set("b", "content-b")
	db.Set("c", "content-c")

	db.Rehash(AlgFNV1a)

	for _, lbl := range []string{"a", "b", "c"} {
		data, err := db.Get(lbl)
		if err != nil {
			t.Errorf("Get(%q) after rehash: %v", lbl, err)
		}
		if data != "content-"+lbl {
			t.Errorf("Get(%q) = %q, want %q", lbl, data, "content-"+lbl)
		}
	}
}

// TestRehashHistoryAccessible verifies that version history survives
// the migration. Rehash must preserve the _h field and its compressed
// version chain. If it discarded history during the rebuild, users
// would lose their audit trail after switching algorithms.
func TestRehashHistoryAccessible(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Rehash(AlgBlake2b)

	versions, _ := db.History("doc")
	if len(versions) != 3 {
		t.Errorf("History after rehash: got %d, want 3", len(versions))
	}
}

// TestRehashUpdatesTimestamp verifies that the header timestamp
// advances after Rehash. The timestamp is used as a marker for when
// the file was last modified. If it weren't updated, monitoring tools
// or backup systems that check the header timestamp would think the
// file hadn't changed.
func TestRehashUpdatesTimestamp(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	tsBefore := db.header.Timestamp

	db.Rehash(AlgFNV1a)

	if db.header.Timestamp <= tsBefore {
		t.Error("header.Timestamp not updated after rehash")
	}
}

// TestRehashAllAlgorithms exercises every pairwise migration path:
// xxHash3→FNV-1a, FNV-1a→Blake2b, Blake2b→xxHash3. Each produces
// different IDs for the same labels, so Rehash must rewrite every
// record. If any migration path were broken, users switching from
// that specific algorithm would lose access to their data.
func TestRehashAllAlgorithms(t *testing.T) {
	tests := []struct {
		from, to int
	}{
		{AlgXXHash3, AlgFNV1a},
		{AlgFNV1a, AlgBlake2b},
		{AlgBlake2b, AlgXXHash3},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			dir := t.TempDir()
			db, _ := Open(filepath.Join(dir, "test.folio"), Config{HashAlgorithm: tt.from})
			defer db.Close()

			db.Set("doc", "content")
			db.Rehash(tt.to)

			if db.header.Algorithm != tt.to {
				t.Errorf("algorithm = %d, want %d", db.header.Algorithm, tt.to)
			}

			data, err := db.Get("doc")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if data != "content" {
				t.Errorf("Get = %q, want %q", data, "content")
			}
		})
	}
}

// TestRehashEmptyDatabase verifies that Rehash on a database with no
// records succeeds and updates the algorithm. This catches nil-pointer
// or empty-slice bugs in the rebuild pipeline.
func TestRehashEmptyDatabase(t *testing.T) {
	db := openTestDB(t)

	err := db.Rehash(AlgFNV1a)
	if err != nil {
		t.Fatalf("Rehash empty DB: %v", err)
	}

	if db.header.Algorithm != AlgFNV1a {
		t.Errorf("algorithm = %d, want %d", db.header.Algorithm, AlgFNV1a)
	}
}

// TestRehashThenCompact verifies that Rehash followed by Compact
// produces a valid file. Both operations rebuild the file from scratch;
// if Rehash left the file in a state that Compact couldn't read (e.g.
// wrong section boundaries), the second rebuild would fail.
func TestRehashThenCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Rehash(AlgFNV1a)
	db.Compact()

	data, _ := db.Get("doc")
	if data != "content" {
		t.Errorf("Get after rehash+compact = %q, want %q", data, "content")
	}
}

// TestRehashAfterCompact verifies the reverse composition: Compact
// followed by Rehash. After compaction, records are in the sorted
// section with established section boundaries. Rehash must be able to
// read the compacted layout, recompute IDs, and rebuild correctly.
func TestRehashAfterCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Compact()
	db.Rehash(AlgFNV1a)

	data, _ := db.Get("doc")
	if data != "content" {
		t.Errorf("Get after compact+rehash = %q, want %q", data, "content")
	}
}
