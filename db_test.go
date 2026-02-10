package folio

import (
	"path/filepath"
	"strings"
	"testing"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOpenCreateNew(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	path := filepath.Join(dir, "test.folio")
	if _, err := filepath.Glob(path); err != nil {
		t.Errorf("file not created")
	}
}

func TestOpenExisting(t *testing.T) {
	dir := t.TempDir()

	db1, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db1.Set("doc", "content")
	db1.Close()

	db2, err := Open(filepath.Join(dir, "test.folio"), Config{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	data, err := db2.Get("doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "content" {
		t.Errorf("Get = %q, want %q", data, "content")
	}
}

func TestOpenDefaultConfig(t *testing.T) {
	db := openTestDB(t)

	if db.config.HashAlgorithm != AlgXXHash3 {
		t.Errorf("HashAlgorithm = %d, want %d", db.config.HashAlgorithm, AlgXXHash3)
	}
	if db.config.ReadBuffer != 64*1024 {
		t.Errorf("ReadBuffer = %d, want %d", db.config.ReadBuffer, 64*1024)
	}
	if db.config.MaxRecordSize != 16*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", db.config.MaxRecordSize, 16*1024*1024)
	}
}

func TestClose(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{})
	db.Set("doc", "content")

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := db.Get("doc")
	if err != ErrClosed {
		t.Errorf("Get after close: got %v, want ErrClosed", err)
	}
}

func TestSetGet(t *testing.T) {
	db := openTestDB(t)

	if err := db.Set("myapp", "hello world"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	data, err := db.Get("myapp")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if data != "hello world" {
		t.Errorf("Get = %q, want %q", data, "hello world")
	}
}

func TestSetUpdate(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")

	data, _ := db.Get("doc")
	if data != "v2" {
		t.Errorf("Get = %q, want %q", data, "v2")
	}
}

func TestSetLabelTooLong(t *testing.T) {
	db := openTestDB(t)

	label := string(make([]byte, MaxLabelSize+1))
	err := db.Set(label, "content")
	if err != ErrLabelTooLong {
		t.Errorf("Set long label: got %v, want ErrLabelTooLong", err)
	}
}

func TestSetLabelWithQuote(t *testing.T) {
	db := openTestDB(t)

	err := db.Set(`my"label`, "content")
	if err != ErrInvalidLabel {
		t.Errorf("Set label with quote: got %v, want ErrInvalidLabel", err)
	}
}

func TestSetEmptyContent(t *testing.T) {
	db := openTestDB(t)

	err := db.Set("doc", "")
	if err != ErrEmptyContent {
		t.Errorf("Set empty: got %v, want ErrEmptyContent", err)
	}
}

func TestGetNotFound(t *testing.T) {
	db := openTestDB(t)

	_, err := db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get missing: got %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")
	if err := db.Delete("doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := db.Get("doc")
	if err != ErrNotFound {
		t.Errorf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	db := openTestDB(t)

	err := db.Delete("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func TestExists(t *testing.T) {
	db := openTestDB(t)

	exists, _ := db.Exists("doc")
	if exists {
		t.Error("Exists before Set should be false")
	}

	db.Set("doc", "content")

	exists, _ = db.Exists("doc")
	if !exists {
		t.Error("Exists after Set should be true")
	}

	db.Delete("doc")

	exists, _ = db.Exists("doc")
	if exists {
		t.Error("Exists after Delete should be false")
	}
}

func TestList(t *testing.T) {
	db := openTestDB(t)

	labels, _ := db.List()
	if len(labels) != 0 {
		t.Errorf("List empty db: got %d, want 0", len(labels))
	}

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("c", "3")

	labels, _ = db.List()
	if len(labels) != 3 {
		t.Errorf("List: got %d labels, want 3", len(labels))
	}
}

func TestListAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Delete("a")

	labels, _ := db.List()
	if len(labels) != 1 {
		t.Errorf("List after delete: got %d, want 1", len(labels))
	}
	if labels[0] != "b" {
		t.Errorf("List[0] = %q, want %q", labels[0], "b")
	}
}

func TestHistory(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	versions, err := db.History("doc")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("History: got %d versions, want 3", len(versions))
	}

	if versions[0].Data != "v1" {
		t.Errorf("versions[0] = %q, want v1", versions[0].Data)
	}
	if versions[2].Data != "v3" {
		t.Errorf("versions[2] = %q, want v3", versions[2].Data)
	}
}

func TestHistoryAfterDelete(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Delete("doc")

	versions, _ := db.History("doc")
	if len(versions) != 2 {
		t.Errorf("History after delete: got %d, want 2", len(versions))
	}
}

func TestHistoryNonexistent(t *testing.T) {
	db := openTestDB(t)

	versions, _ := db.History("nonexistent")
	if len(versions) != 0 {
		t.Errorf("History nonexistent: got %d, want 0", len(versions))
	}
}

func TestCompact(t *testing.T) {
	db := openTestDB(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Set("a", "1-updated")

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	data, _ := db.Get("a")
	if data != "1-updated" {
		t.Errorf("Get after compact = %q, want %q", data, "1-updated")
	}

	versions, _ := db.History("a")
	if len(versions) != 2 {
		t.Errorf("History after compact: got %d, want 2", len(versions))
	}
}

func TestPurge(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "v1")
	db.Set("doc", "v2")
	db.Set("doc", "v3")

	if err := db.Purge(); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	data, _ := db.Get("doc")
	if data != "v3" {
		t.Errorf("Get after purge = %q, want %q", data, "v3")
	}

	versions, _ := db.History("doc")
	if len(versions) != 1 {
		t.Errorf("History after purge: got %d, want 1 (current only)", len(versions))
	}
}

func TestRehash(t *testing.T) {
	db := openTestDB(t)

	db.Set("doc", "content")

	if err := db.Rehash(AlgFNV1a); err != nil {
		t.Fatalf("Rehash: %v", err)
	}

	if db.header.Algorithm != AlgFNV1a {
		t.Errorf("Algorithm = %d, want %d", db.header.Algorithm, AlgFNV1a)
	}

	data, _ := db.Get("doc")
	if data != "content" {
		t.Errorf("Get after rehash = %q, want %q", data, "content")
	}
}

func TestLargeContent(t *testing.T) {
	db := openTestDB(t)

	// 1MB of text
	content := strings.Repeat("x", 1024*1024)

	if err := db.Set("large", content); err != nil {
		t.Fatalf("Set large: %v", err)
	}

	data, err := db.Get("large")
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	if data != content {
		t.Errorf("Get large: length %d, want %d", len(data), len(content))
	}
}

func TestUnicodeContent(t *testing.T) {
	db := openTestDB(t)

	content := "日本語テキスト 🎉 émojis and spëcial châräctérs"

	db.Set("unicode", content)

	data, _ := db.Get("unicode")
	if data != content {
		t.Errorf("unicode content: got %q, want %q", data, content)
	}
}

func TestStateConstants(t *testing.T) {
	if StateAll != 0 {
		t.Errorf("StateAll = %d, want 0", StateAll)
	}
	if StateRead != 1 {
		t.Errorf("StateRead = %d, want 1", StateRead)
	}
	if StateNone != 2 {
		t.Errorf("StateNone = %d, want 2", StateNone)
	}
	if StateClosed != 3 {
		t.Errorf("StateClosed = %d, want 3", StateClosed)
	}
}
