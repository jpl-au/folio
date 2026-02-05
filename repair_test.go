package folio

import (
	"testing"
)

func TestRepairSortsData(t *testing.T) {
	db := openTestDB(t)

	// Create in non-sorted order by ID
	db.Set("zzz", "last")
	db.Set("aaa", "first")
	db.Set("mmm", "middle")

	db.Repair(nil)

	// After repair, data section should be sorted
	// Verify by checking header boundaries are set
	if db.header.Data == 0 {
		t.Error("header.Data not set after repair")
	}
	if db.header.Index == 0 {
		t.Error("header.Index not set after repair")
	}
	if db.header.Data >= db.header.Index {
		t.Error("data section should end before index section")
	}
}

func TestRepairPreservesHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Repair(nil)

	versions, _ := db.History("doc")
	if len(versions) != 3 {
		t.Errorf("History: got %d versions, want 3", len(versions))
	}
}

func TestRepairWithPurgeHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Repair(&CompactOptions{PurgeHistory: true})

	versions, _ := db.History("doc")
	if len(versions) != 1 {
		t.Errorf("History after purge: got %d versions, want 1", len(versions))
	}
}

func TestRepairUpdatesHeader(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	if db.header.Data != 0 {
		t.Error("header.Data should be 0 before repair")
	}

	db.Repair(nil)

	if db.header.Data == 0 {
		t.Error("header.Data should be set after repair")
	}
	if db.header.Index == 0 {
		t.Error("header.Index should be set after repair")
	}
	if db.header.Error != 0 {
		t.Error("header.Error should be 0 after repair")
	}
}

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

func TestRepairNilOptions(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	err := db.Repair(nil)
	if err != nil {
		t.Fatalf("Repair(nil): %v", err)
	}
}

func TestCompactPreservesHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	db.Compact()

	versions, _ := db.History("doc")
	if len(versions) != 2 {
		t.Errorf("History after Compact: got %d, want 2", len(versions))
	}
}

func TestPurgeRemovesHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	db.Purge()

	versions, _ := db.History("doc")
	if len(versions) != 1 {
		t.Errorf("History after Purge: got %d, want 1", len(versions))
	}

	// Current version should still be accessible
	data, _ := db.Get("doc")
	if data != "v3" {
		t.Errorf("Get after Purge = %q, want %q", data, "v3")
	}
}

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

func TestRepairSparseEmptyAfter(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Repair(nil)

	// tail should equal header.Index (sparse section empty)
	if db.tail != db.header.Index {
		t.Errorf("tail = %d, want %d (header.Index)", db.tail, db.header.Index)
	}
}

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
