package folio

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigSyncWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{SyncWrites: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if !db.config.SyncWrites {
		t.Error("SyncWrites not set")
	}

	// Operations should still work
	err = db.Set("doc", "content")
	if err != nil {
		t.Errorf("Set with SyncWrites: %v", err)
	}
}

func TestConfigHashAlgorithm(t *testing.T) {
	tests := []struct {
		alg  int
		want int
	}{
		{0, AlgXXHash3},         // Default
		{AlgXXHash3, AlgXXHash3},
		{AlgFNV1a, AlgFNV1a},
		{AlgBlake2b, AlgBlake2b},
	}

	for _, tt := range tests {
		dir := t.TempDir()
		db, _ := Open(filepath.Join(dir, "test.folio"), Config{HashAlgorithm: tt.alg})

		if db.config.HashAlgorithm != tt.want {
			t.Errorf("HashAlgorithm(%d) = %d, want %d", tt.alg, db.config.HashAlgorithm, tt.want)
		}
		db.Close()
	}
}

func TestConfigReadBufferDefault(t *testing.T) {
	db := openTestDB(t)

	if db.config.ReadBuffer != 64*1024 {
		t.Errorf("ReadBuffer = %d, want %d", db.config.ReadBuffer, 64*1024)
	}
}

func TestConfigReadBufferCustom(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{ReadBuffer: 128 * 1024})
	defer db.Close()

	if db.config.ReadBuffer != 128*1024 {
		t.Errorf("ReadBuffer = %d, want %d", db.config.ReadBuffer, 128*1024)
	}
}

func TestConfigMaxRecordSizeDefault(t *testing.T) {
	db := openTestDB(t)

	if db.config.MaxRecordSize != 16*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", db.config.MaxRecordSize, 16*1024*1024)
	}
}

func TestConfigMaxRecordSizeCustom(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{MaxRecordSize: 8 * 1024 * 1024})
	defer db.Close()

	if db.config.MaxRecordSize != 8*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", db.config.MaxRecordSize, 8*1024*1024)
	}
}

func TestMaxRecordSizeConstant(t *testing.T) {
	if MaxRecordSize != 16*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", MaxRecordSize, 16*1024*1024)
	}
}

func TestVeryLargeContent(t *testing.T) {
	db := openTestDB(t)

	// 5MB content - should work
	content := strings.Repeat("x", 5*1024*1024)
	err := db.Set("large", content)
	if err != nil {
		t.Fatalf("Set 5MB: %v", err)
	}

	data, err := db.Get("large")
	if err != nil {
		t.Fatalf("Get 5MB: %v", err)
	}
	if len(data) != len(content) {
		t.Errorf("len = %d, want %d", len(data), len(content))
	}
}
