// Configuration option tests.
//
// Config controls runtime behaviour: hash algorithm, sync writes,
// read buffer size, max record size, and bloom filter. The defaults
// are chosen for the common case (fast, no fsync, 64KB buffer, 16MB
// max). These tests verify that: defaults are applied when Config{}
// is passed, custom values override defaults, and the database remains
// fully functional with each configuration variant.
package folio

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestConfigSyncWrites verifies that SyncWrites=true is propagated to
// the database and that operations still succeed. SyncWrites adds
// fsync after every write for durability. If the flag weren't propagated,
// a user who requested durability would silently get buffered writes.
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

// TestConfigHashAlgorithm verifies that each hash algorithm option is
// accepted, and that the zero value defaults to xxHash3. The algorithm
// is stored in the header — if the default were wrong, new databases
// would be created with an invalid algorithm and all Set calls would
// produce empty IDs.
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

// TestConfigReadBufferDefault verifies that the default read buffer is
// 64KB. line() allocates this buffer for reading records — too small
// and large records would require multiple reads, too large and memory
// usage would spike for many concurrent readers.
func TestConfigReadBufferDefault(t *testing.T) {
	db := openTestDB(t)

	if db.config.ReadBuffer != 64*1024 {
		t.Errorf("ReadBuffer = %d, want %d", db.config.ReadBuffer, 64*1024)
	}
}

// TestConfigReadBufferCustom verifies that a custom buffer size
// overrides the default. Users with large records may need a bigger
// buffer to avoid multiple read syscalls per line.
func TestConfigReadBufferCustom(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{ReadBuffer: 128 * 1024})
	defer db.Close()

	if db.config.ReadBuffer != 128*1024 {
		t.Errorf("ReadBuffer = %d, want %d", db.config.ReadBuffer, 128*1024)
	}
}

// TestConfigMaxRecordSizeDefault verifies the 16MB default maximum
// record size. This limit prevents a single Set call from consuming
// excessive memory and producing a line too long for line() to read
// in a single buffer allocation.
func TestConfigMaxRecordSizeDefault(t *testing.T) {
	db := openTestDB(t)

	if db.config.MaxRecordSize != 16*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", db.config.MaxRecordSize, 16*1024*1024)
	}
}

// TestConfigMaxRecordSizeCustom verifies that a custom max record size
// overrides the default. Smaller limits protect against memory
// exhaustion in constrained environments.
func TestConfigMaxRecordSizeCustom(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "test.folio"), Config{MaxRecordSize: 8 * 1024 * 1024})
	defer db.Close()

	if db.config.MaxRecordSize != 8*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", db.config.MaxRecordSize, 8*1024*1024)
	}
}

// TestMaxRecordSizeConstant guards the exported constant that defines
// the default maximum. If it changed, existing documentation and user
// expectations would be wrong, and users who relied on storing 16MB
// records would get unexpected errors.
func TestMaxRecordSizeConstant(t *testing.T) {
	if MaxRecordSize != 16*1024*1024 {
		t.Errorf("MaxRecordSize = %d, want %d", MaxRecordSize, 16*1024*1024)
	}
}

// TestVeryLargeContent verifies that a 5MB document round-trips
// correctly. This exercises the read buffer growth path in line() — if
// the initial 64KB buffer is too small, line() must grow it until the
// entire record fits. A bug in the growth logic would truncate large
// records, returning partial content.
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
