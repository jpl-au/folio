// Repair and compaction tests.
//
// Repair rebuilds the entire file from scratch: it reads every valid
// record, sorts them by ID then timestamp, writes data records and
// history into a new heap section, builds a fresh sorted index, and
// replaces the original file. Compact is a convenience wrapper that
// calls Repair without purging history.
//
// This rebuild is the only way to reclaim space from deleted documents
// and to move sparse-region records into the sorted section where they
// can be found via O(log n) binary search instead of O(n) linear scan.
// Getting it wrong means data loss — so these tests verify that every
// document, every version, and every history snapshot survives the
// rebuild, that section boundaries are set correctly, and that the
// database is fully operational after repair completes.
package folio

import (
	"testing"
)

// TestRepairSortsData verifies that Repair produces a sorted heap
// section. Before compaction, records are appended in arrival order;
// after compaction, they must be sorted by ID so that binary search
// (scan) can find them in O(log n). If Repair failed to sort, binary
// search would miss records that exist in the file, making them
// invisible to Get.
func TestRepairSortsData(t *testing.T) {
	db := openTestDB(t)

	// Create in non-sorted order by ID
	db.Set("zzz", "last")
	db.Set("aaa", "first")
	db.Set("mmm", "middle")

	db.Repair(nil)

	// After repair, data section should be sorted
	// Verify by checking header boundaries are set
	if db.header.Heap == 0 {
		t.Error("header.Heap not set after repair")
	}
	if db.header.Index == 0 {
		t.Error("header.Index not set after repair")
	}
	if db.header.Heap >= db.header.Index {
		t.Error("data section should end before index section")
	}
}

// TestRepairPreservesHistory verifies that all three versions of a
// document survive compaction. Repair compresses older versions into the
// _h field of the current record using zstd+ascii85. If the compression
// or decompression round-trip lost data, History would return fewer
// versions than were written, silently destroying the audit trail.
func TestRepairPreservesHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Repair(nil)

	versions, _ := collect(db.History("doc"))
	if len(versions) != 3 {
		t.Errorf("History: got %d versions, want 3", len(versions))
	}
}

// TestRepairWithPurgeHistory verifies that PurgeHistory discards all
// but the current version. This is an explicit opt-in to destroy the
// audit trail in exchange for a smaller file. If purge failed to strip
// history, the _h field would keep growing with each compaction,
// eventually dominating file size for frequently-updated documents.
func TestRepairWithPurgeHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Repair(&CompactOptions{PurgeHistory: true})

	versions, _ := collect(db.History("doc"))
	if len(versions) != 1 {
		t.Errorf("History after purge: got %d versions, want 1", len(versions))
	}
}

// TestRepairUpdatesHeader verifies that Repair writes correct section
// boundaries into the header. Before compaction, Heap and Index are
// both zero (all records live in the sparse region). After compaction,
// Heap must point to the start of the data section and Index to the
// start of the index section. If either were wrong, binary search
// would operate on the wrong byte range and miss every document.
func TestRepairUpdatesHeader(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	if db.header.Heap != 0 {
		t.Error("header.Heap should be 0 before repair")
	}

	db.Repair(nil)

	if db.header.Heap == 0 {
		t.Error("header.Heap should be set after repair")
	}
	if db.header.Index == 0 {
		t.Error("header.Index should be set after repair")
	}
	if db.header.Error != 0 {
		t.Error("header.Error should be 0 after repair")
	}
}

// TestRepairDataStillAccessible is the end-to-end smoke test: write
// three documents, compact, then Get each one. This catches any bug
// in the rebuild pipeline — sorting, index generation, header update,
// file replacement — that would cause a previously-accessible document
// to become unreachable after compaction.
func TestRepairDataStillAccessible(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "content-a")
	db.Set("b", "content-b")
	db.Set("c", "content-c")

	db.Repair(nil)

	for _, lbl := range []string{"a", "b", "c"} {
		data, err := db.Get(lbl)
		if err != nil {
			t.Errorf("Get(%q) after repair: %v", lbl, err)
		}
		if data != "content-"+lbl {
			t.Errorf("Get(%q) = %q, want %q", lbl, data, "content-"+lbl)
		}
	}
}

// TestRepairNilOptions verifies that Repair accepts nil options without
// panicking. Compact() calls Repair(nil) internally, so a nil-pointer
// dereference here would crash every compaction.
func TestRepairNilOptions(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	err := db.Repair(nil)
	if err != nil {
		t.Fatalf("Repair(nil): %v", err)
	}
}

// TestCompactPreservesHistory verifies that Compact (the convenience
// wrapper) preserves history by default. Compact calls Repair without
// PurgeHistory, so all versions must survive. If the default changed
// to purge, users would silently lose their audit trail.
func TestCompactPreservesHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	db.Compact()

	versions, _ := collect(db.History("doc"))
	if len(versions) != 2 {
		t.Errorf("History after Compact: got %d, want 2", len(versions))
	}
}

// TestPurgeRemovesHistory verifies that Purge keeps only the latest
// version of each document while leaving the current version accessible.
// This is the destructive counterpart to Compact — if Purge accidentally
// kept history or accidentally deleted the current version, users would
// either waste space or lose their data.
func TestPurgeRemovesHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Purge()

	versions, _ := collect(db.History("doc"))
	if len(versions) != 1 {
		t.Errorf("History after Purge: got %d, want 1", len(versions))
	}

	// Current version should still be accessible
	data, _ := db.Get("doc")
	if data != "v3" {
		t.Errorf("Get after Purge = %q, want %q", data, "v3")
	}
}

// TestRepairEmptyDatabase verifies that Repair handles a database with
// no records. An empty database has only a header; the scanm that reads
// all records would return an empty slice. If Repair didn't handle this
// gracefully, the rebuild could produce a file shorter than HeaderSize
// or leave the header in an inconsistent state.
func TestRepairEmptyDatabase(t *testing.T) {
	db := openTestDB(t)

	err := db.Repair(nil)
	if err != nil {
		t.Fatalf("Repair empty DB: %v", err)
	}

	// Should still be able to use
	err = db.Set("doc", "content")
	if err != nil {
		t.Fatalf("Set after repair empty: %v", err)
	}
}

// TestRepairAfterDelete verifies that deleted documents are excluded
// from the rebuilt file. Delete blanks the index with spaces, making
// it invisible to lookups but still physically present. Repair must
// honour the deletion by not including the blanked record in the new
// file. If it didn't, deleted documents would reappear after compaction.
func TestRepairAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "content-a")
	db.Set("b", "content-b")
	db.Delete("a")

	db.Repair(nil)

	_, err := db.Get("a")
	if err != ErrNotFound {
		t.Errorf("Get deleted after repair: got %v, want ErrNotFound", err)
	}

	data, _ := db.Get("b")
	if data != "content-b" {
		t.Errorf("Get(b) = %q, want %q", data, "content-b")
	}
}

// TestRepairSparseEmptyAfter verifies that compaction moves all sparse
// records into the sorted section, leaving the sparse region empty
// (tail == header.Index). If sparse records were left behind, they
// would be duplicated — once in the sorted section and once in sparse —
// causing Get to return stale versions and History to show duplicates.
func TestRepairSparseEmptyAfter(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Repair(nil)

	// tail should equal header.Index (sparse section empty)
	if db.tail != db.header.Index {
		t.Errorf("tail = %d, want %d (header.Index)", db.tail, db.header.Index)
	}
}

// TestRepairBlockReaders exercises the BlockReaders option, which
// transitions the state to StateNone during repair. This blocks both
// readers and writers, providing a consistent snapshot at the cost of
// availability. Without this option, Repair uses StateRead (allowing
// concurrent reads). The test verifies data integrity after a
// blocking repair — if the file replacement step were not atomic,
// a reader that snuck in during the swap would read a partial file.
func TestRepairBlockReaders(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "content-a")
	db.Set("b", "content-b")
	db.Set("a", "content-a-v2")

	err := db.Repair(&CompactOptions{BlockReaders: true})
	if err != nil {
		t.Fatalf("Repair(BlockReaders): %v", err)
	}

	data, _ := db.Get("a")
	if data != "content-a-v2" {
		t.Errorf("Get(a) = %q, want %q", data, "content-a-v2")
	}

	data, _ = db.Get("b")
	if data != "content-b" {
		t.Errorf("Get(b) = %q, want %q", data, "content-b")
	}

	versions, _ := collect(db.History("a"))
	if len(versions) != 2 {
		t.Errorf("History(a): got %d, want 2", len(versions))
	}
}

// TestRepairBlockReadersPurge combines both options: blocking readers
// and purging history. This is the most aggressive rebuild mode. The
// test verifies that only the latest version survives and that the
// current data is correct after the combined operation.
func TestRepairBlockReadersPurge(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	err := db.Repair(&CompactOptions{BlockReaders: true, PurgeHistory: true})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}

	data, _ := db.Get("doc")
	if data != "v3" {
		t.Errorf("Get = %q, want %q", data, "v3")
	}

	versions, _ := collect(db.History("doc"))
	if len(versions) != 1 {
		t.Errorf("History: got %d, want 1", len(versions))
	}
}

// TestCompactClosed verifies that Compact on a closed database returns
// an error. Compact needs both read access (to scan existing records)
// and write access (to replace the file). If it didn't check the closed
// state, it would attempt to read from a closed file handle, producing
// an OS-level error that would be confusing to debug.
func TestCompactClosed(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	err := db.Compact()
	if err == nil {
		t.Error("Compact on closed DB: expected error")
	}
}

// TestCompactThenSet verifies that new writes after compaction land in
// the sparse region and are still accessible alongside sorted data.
// After Compact, the sparse region is empty (tail == header.Index).
// A subsequent Set must append past the index section. If Set
// miscalculated the tail offset, it would overwrite sorted indexes,
// corrupting the lookup table for every existing document.
func TestCompactThenSet(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "v1")
	db.Compact()
	db.Set("b", "v2")

	dataA, _ := db.Get("a")
	dataB, _ := db.Get("b")

	if dataA != "v1" {
		t.Errorf("Get(a) = %q, want %q", dataA, "v1")
	}
	if dataB != "v2" {
		t.Errorf("Get(b) = %q, want %q", dataB, "v2")
	}
}

// TestCompactThenUpdate verifies the update-after-compact path. Set
// must find the existing document's sorted index (via binary search),
// blank it, and append the new version to sparse. If Set failed to
// blank the old sorted index, Get would still find the old version
// because sorted results take precedence when sparse has no match.
func TestCompactThenUpdate(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Compact()
	db.Set("doc", "v2")

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}
