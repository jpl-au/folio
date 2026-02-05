package folio

import (
	"testing"
)

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

func TestRehashUpdatesTimestamp(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	tsBefore := db.header.Timestamp

	db.Rehash(AlgFNV1a)

	if db.header.Timestamp <= tsBefore {
		t.Error("header.Timestamp not updated after rehash")
	}
}

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
			db, _ := Open(dir, "test.folio", Config{HashAlgorithm: tt.from})
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
