package folio

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLabelExactly256Bytes(t *testing.T) {
	db := openTestDB(t)

	label := strings.Repeat("x", MaxLabelSize)
	err := db.Set(label, "content")
	if err != nil {
		t.Errorf("Set with 256-byte label: %v", err)
	}

	data, err := db.Get(label)
	if err != nil {
		t.Errorf("Get: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

func TestLabelWithPathSeparators(t *testing.T) {
	db := openTestDB(t)

	label := "a/b/c\\d"
	err := db.Set(label, "content")
	if err != nil {
		t.Errorf("Set with path separators: %v", err)
	}

	data, _ := db.Get(label)
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

func TestEmptyLabel(t *testing.T) {
	db := openTestDB(t)

	err := db.Set("", "content")
	if err != nil {
		t.Errorf("Set with empty label: %v", err)
	}

	data, _ := db.Get("")
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

func TestFreshDatabaseEdgeCases(t *testing.T) {
	db := openTestDB(t)

	// Get on empty
	_, err := db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get on empty: got %v, want ErrNotFound", err)
	}

	// Delete on empty
	err = db.Delete("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Delete on empty: got %v, want ErrNotFound", err)
	}

	// List on empty
	labels, _ := db.List()
	if len(labels) != 0 {
		t.Errorf("List on empty: got %d, want 0", len(labels))
	}

	// Exists on empty
	exists, _ := db.Exists("nonexistent")
	if exists {
		t.Error("Exists on empty should be false")
	}

	// History on empty
	versions, _ := db.History("nonexistent")
	if len(versions) != 0 {
		t.Errorf("History on empty: got %d, want 0", len(versions))
	}

	// Compact on empty
	err = db.Compact()
	if err != nil {
		t.Errorf("Compact on empty: %v", err)
	}

	// Purge on empty
	err = db.Purge()
	if err != nil {
		t.Errorf("Purge on empty: %v", err)
	}
}

func TestCrashRecoveryDirtyFlag(t *testing.T) {
	dir := t.TempDir()

	// Create DB and set dirty flag
	db1, _ := Open(dir, "test.folio", Config{})
	db1.Set("doc", "content")
	// Don't close cleanly - leave dirty flag set

	// Manually set dirty flag and close handles
	dirty(db1.writer, true)
	db1.writer.Sync()
	db1.reader.Close()
	db1.writer.Close()
	db1.root.Close()

	// Reopen - should trigger repair
	db2, err := Open(dir, "test.folio", Config{})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer db2.Close()

	// Data should still be accessible
	data, err := db2.Get("doc")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}

	// Dirty flag should be clear
	if db2.header.Error != 0 {
		t.Error("dirty flag should be cleared after recovery")
	}
}

func TestCrashRecoveryTmpFile(t *testing.T) {
	dir := t.TempDir()

	// Create DB
	db1, _ := Open(dir, "test.folio", Config{})
	db1.Set("doc", "content")
	db1.Close()

	// Create orphan .tmp file
	tmpPath := filepath.Join(dir, "test.folio.tmp")
	os.WriteFile(tmpPath, []byte("garbage"), 0644)

	// Reopen - should delete .tmp and repair
	db2, err := Open(dir, "test.folio", Config{})
	if err != nil {
		t.Fatalf("Open with tmp file: %v", err)
	}
	defer db2.Close()

	// tmp file should be deleted
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error(".tmp file should be deleted")
	}

	// Data should still be accessible
	data, _ := db2.Get("doc")
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

func TestDoubleClose(t *testing.T) {
	db := openTestDB(t)

	err := db.Close()
	if err != nil {
		t.Fatalf("First close: %v", err)
	}

	// Second close should return error (handles already closed)
	err = db.Close()
	if err == nil {
		t.Error("Second close should return error")
	}
}

func TestOperationsAfterClose(t *testing.T) {
	db := openTestDB(t)
	db.Set("doc", "content")
	db.Close()

	_, err := db.Get("doc")
	if err != ErrClosed {
		t.Errorf("Get after close: got %v, want ErrClosed", err)
	}

	err = db.Set("doc", "new")
	if err != ErrClosed {
		t.Errorf("Set after close: got %v, want ErrClosed", err)
	}

	err = db.Delete("doc")
	if err != ErrClosed {
		t.Errorf("Delete after close: got %v, want ErrClosed", err)
	}

	_, err = db.Exists("doc")
	if err != ErrClosed {
		t.Errorf("Exists after close: got %v, want ErrClosed", err)
	}

	_, err = db.List()
	if err != ErrClosed {
		t.Errorf("List after close: got %v, want ErrClosed", err)
	}

	_, err = db.History("doc")
	if err != ErrClosed {
		t.Errorf("History after close: got %v, want ErrClosed", err)
	}
}

func TestDeleteFromSparse(t *testing.T) {
	db := openTestDB(t)

	// Add document (goes to sparse section since no compaction)
	db.Set("doc", "content")

	// Delete from sparse
	err := db.Delete("doc")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestDeleteFromSorted(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	db.Compact() // Moves to sorted section

	err := db.Delete("doc")
	if err != nil {
		t.Fatalf("Delete from sorted: %v", err)
	}

	_, err = db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestSparseOverridesSorted(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Compact() // v1 now in sorted section

	db.Set("doc", "v2") // v2 in sparse section

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q (sparse should override sorted)", data, "v2")
	}
}
