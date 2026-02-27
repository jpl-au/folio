// Header serialisation and validation tests.
//
// The header is a fixed 128-byte JSON line at byte 0 of every folio file.
// It stores the hash algorithm, section boundaries (State[stHeap],
// State[stIndex]), and a dirty flag (Error) used for crash recovery. Every
// read operation depends on correct header values to find the sorted and
// sparse regions — a wrong heap offset would cause binary search to read
// data records as indexes, returning garbage offsets to Get.
//
// These tests verify: encoding produces exactly 128 bytes, round-trip
// encode/decode preserves all fields, the dirty flag occupies the expected
// byte position (so writeAt can flip it without re-encoding the full
// header), and invalid headers are rejected before any data operations
// begin.
package folio

import (
	"os"
	"path/filepath"
	"testing"
)

// TestHeaderSize guards the constant that every other offset calculation
// depends on. If HeaderSize drifted from 128, the first data record would
// be written at the wrong position and every subsequent seek would be off.
func TestHeaderSize(t *testing.T) {
	if HeaderSize != 128 {
		t.Errorf("HeaderSize = %d, want 128", HeaderSize)
	}
}

// TestHeaderEncode verifies that encode() produces exactly HeaderSize
// bytes terminated by a newline. The newline is critical: line() reads
// until '\n', so a header without a trailing newline would cause the
// first data read to consume the header plus the first record as one
// oversized line, failing JSON parsing.
func TestHeaderEncode(t *testing.T) {
	h := &Header{
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
		State:     [6]uint64{5000, 6000},
	}

	buf, err := h.encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if len(buf) != HeaderSize {
		t.Errorf("encoded length = %d, want %d", len(buf), HeaderSize)
	}

	if buf[HeaderSize-1] != '\n' {
		t.Errorf("last byte = %q, want newline", buf[HeaderSize-1])
	}
}

// TestHeaderEncodeFreshDB verifies encoding when State offsets are zero
// (no compaction has occurred). The JSON must still pad to exactly 128
// bytes — if the padding logic assumed non-zero section offsets, a fresh
// database would have a short header and every subsequent write would
// land at the wrong file position.
func TestHeaderEncodeFreshDB(t *testing.T) {
	h := &Header{
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
	}

	buf, err := h.encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if len(buf) != HeaderSize {
		t.Errorf("fresh header length = %d, want %d", len(buf), HeaderSize)
	}
}

// TestHeaderReadWrite is the round-trip test: encode a header with known
// field values, write it to a file, read it back with header(), and
// verify every field matches. This catches encoding bugs (e.g. a field
// mapped to the wrong JSON key) that would silently produce a valid
// header with swapped section boundaries.
func TestHeaderReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	// Write header
	original := &Header{
		Algorithm: AlgFNV1a,
		Timestamp: 1706000000000,
		State:     [6]uint64{1000, 2000, 0, 42, 10, 100},
	}

	buf, err := original.encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if err := os.WriteFile(path, buf, 0644); err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Read header
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open error: %v", err)
	}
	defer f.Close()

	h, err := header(f)
	if err != nil {
		t.Fatalf("header error: %v", err)
	}

	if h.Error != original.Error {
		t.Errorf("Error = %d, want %d", h.Error, original.Error)
	}
	if h.Algorithm != original.Algorithm {
		t.Errorf("Algorithm = %d, want %d", h.Algorithm, original.Algorithm)
	}
	if h.Timestamp != original.Timestamp {
		t.Errorf("Timestamp = %d, want %d", h.Timestamp, original.Timestamp)
	}
	if h.State != original.State {
		t.Errorf("State = %v, want %v", h.State, original.State)
	}
}

// TestHeaderDirtyFlag exercises the crash-recovery mechanism. Before any
// write, the dirty flag (Error field) is set to 1; after a clean Close
// or Compact, it is cleared to 0. On Open, a dirty flag of 1 means the
// previous session crashed mid-write, triggering automatic Repair. If
// dirty() failed to persist the flag, a crash after a write would leave
// the file in an inconsistent state with no recovery on next Open.
func TestHeaderDirtyFlag(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	// Create file with clean header
	h := &Header{
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
	}

	buf, _ := h.encode()
	os.WriteFile(path, buf, 0644)

	// Open for writing and set dirty
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	defer f.Close()

	if err := dirty(f, true); err != nil {
		t.Fatalf("dirty(true) error: %v", err)
	}

	// Verify dirty flag is set
	hdr, _ := header(f)
	if hdr.Error != 1 {
		t.Errorf("after dirty(true): Error = %d, want 1", hdr.Error)
	}

	// Clear dirty flag
	if err := dirty(f, false); err != nil {
		t.Fatalf("dirty(false) error: %v", err)
	}

	// Verify dirty flag is cleared
	hdr, _ = header(f)
	if hdr.Error != 0 {
		t.Errorf("after dirty(false): Error = %d, want 0", hdr.Error)
	}
}

// TestHeaderDirtyPosition verifies that the dirty flag lives at byte
// offset 13 in the file. The dirty() function uses writeAt to flip a
// single byte ('0'→'1') rather than re-encoding the full 128-byte
// header — this is an intentional optimisation because dirty() is called
// on every write. If the byte position drifted (e.g. due to a new JSON
// field being added before _e), dirty() would overwrite an unrelated
// field and the flag would never be detected on recovery.
func TestHeaderDirtyPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	h := &Header{
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
	}

	buf, _ := h.encode()
	os.WriteFile(path, buf, 0644)

	// Verify byte at offset 6 is '0' (clean)
	data, _ := os.ReadFile(path)
	if data[13] != '0' {
		t.Errorf("byte at offset 13 = %q, want '0'", data[13])
	}

	// Set dirty and verify byte changed
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	dirty(f, true)
	f.Close()

	data, _ = os.ReadFile(path)
	if data[13] != '1' {
		t.Errorf("after dirty(true): byte at offset 13 = %q, want '1'", data[13])
	}
}

// TestHeaderCorruptHeapTooSmall verifies that header() rejects a heap
// offset smaller than HeaderSize. A value of 50 would place data records
// inside the header region, so binary search would read header JSON as a
// record and return nonsense. The validation in header() catches this on
// Open before any data operations begin.
func TestHeaderCorruptHeapTooSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	h := &Header{Version: 1, Algorithm: AlgXXHash3, State: [6]uint64{50}}
	buf, _ := h.encode()
	os.WriteFile(path, buf, 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err != ErrCorruptHeader {
		t.Errorf("expected ErrCorruptHeader, got %v", err)
	}
}

// TestHeaderCorruptIndexTooSmall verifies that header() rejects an index
// offset smaller than HeaderSize. The index section must start after
// the header; a value of 50 would overlap the header, causing the
// sorted-index binary search to read header bytes as index JSON.
func TestHeaderCorruptIndexTooSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	h := &Header{Version: 1, Algorithm: AlgXXHash3, State: [6]uint64{0, 50}}
	buf, _ := h.encode()
	os.WriteFile(path, buf, 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err != ErrCorruptHeader {
		t.Errorf("expected ErrCorruptHeader, got %v", err)
	}
}

// TestHeaderCorruptHeapAfterIndex verifies that header() rejects a file
// where heap > index. The layout invariant is [Header][Heap][Index][Sparse]:
// heap must end before index begins. If this were inverted, the binary
// search boundaries would be wrong and Get would scan data records as
// indexes, returning byte offsets from document content fields.
func TestHeaderCorruptHeapAfterIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	h := &Header{Version: 1, Algorithm: AlgXXHash3, State: [6]uint64{5000, 4000}}
	buf, _ := h.encode()
	os.WriteFile(path, buf, 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err != ErrCorruptHeader {
		t.Errorf("expected ErrCorruptHeader, got %v", err)
	}
}

// TestHeaderCorruptJSON verifies that header() returns ErrCorruptHeader
// when the first 128 bytes are not valid JSON. This is the very first
// check Open performs — if it accepted garbage, every subsequent read
// would use uninitialised section boundaries (all zero), causing binary
// search to operate on an empty range and sparse scan to start from
// byte 0, reading the header as a data record.
func TestHeaderCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	buf := make([]byte, HeaderSize)
	copy(buf, []byte("not json at all"))
	buf[HeaderSize-1] = '\n'
	os.WriteFile(path, buf, 0644)

	f, _ := os.Open(path)
	defer f.Close()

	_, err := header(f)
	if err != ErrCorruptHeader {
		t.Errorf("expected ErrCorruptHeader, got %v", err)
	}
}

// TestHeaderAllAlgorithms verifies that every supported hash algorithm
// survives a header round-trip. The algorithm field controls which hash
// function is used for ID generation and index lookups — if encoding
// lost the algorithm value, a reopened database would use the default
// (xxHash3) regardless of what was configured, producing different IDs
// for the same labels and making all existing documents unfindable.
func TestHeaderAllAlgorithms(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		h := &Header{
			Algorithm: alg,
			Timestamp: 1706000000000,
		}

		buf, err := h.encode()
		if err != nil {
			t.Errorf("alg %d: encode error: %v", alg, err)
			continue
		}

		// Write and read back
		dir := t.TempDir()
		path := filepath.Join(dir, "test.folio")
		os.WriteFile(path, buf, 0644)

		f, _ := os.Open(path)
		hdr, err := header(f)
		f.Close()

		if err != nil {
			t.Errorf("alg %d: header error: %v", alg, err)
			continue
		}

		if hdr.Algorithm != alg {
			t.Errorf("alg %d: Algorithm = %d", alg, hdr.Algorithm)
		}
	}
}
