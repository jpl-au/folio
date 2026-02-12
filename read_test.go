// Read primitive tests.
//
// The read layer has four operations: line (read one JSONL record at a
// byte offset), align (find the next newline from a mid-line position),
// size (return the file length), and position (return the current seek
// offset). Every read operation in the database — Get, List, History,
// Search, scan, sparse — ultimately calls line() to extract a single
// record. align() is used by binary search to recover line boundaries
// after seeking to the middle of a record.
//
// These tests use raw files (not a full DB) to isolate the read
// primitives from the write path. Each test verifies one specific
// behaviour that, if broken, would cause cascading failures across
// all read operations.
package folio

import (
	"os"
	"path/filepath"
	"testing"
)

// createTestFile writes a raw string to a temporary file and returns
// an open read handle. Used by all read primitive tests to avoid the
// overhead and side effects of creating a full database.
func createTestFile(t *testing.T, content string) *os.File {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open test file: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestLineReadRecord verifies the basic case: reading the first line
// from offset 0. If line() didn't start reading at the given offset,
// it would skip bytes or include bytes from before the requested
// position, returning a different record.
func TestLineReadRecord(t *testing.T) {
	f := createTestFile(t, "first line\nsecond line\nthird line\n")

	data, err := line(f, 0)
	if err != nil {
		t.Fatalf("line error: %v", err)
	}
	if string(data) != "first line" {
		t.Errorf("line(0) = %q, want %q", data, "first line")
	}
}

// TestLineReadFromOffset verifies reading from a non-zero offset. Get
// uses the index's _o field to seek directly to a data record. If
// line() miscalculated the read position, Get would return the wrong
// record's content.
func TestLineReadFromOffset(t *testing.T) {
	f := createTestFile(t, "first line\nsecond line\nthird line\n")

	// Offset 11 is after "first line\n"
	data, err := line(f, 11)
	if err != nil {
		t.Fatalf("line error: %v", err)
	}
	if string(data) != "second line" {
		t.Errorf("line(11) = %q, want %q", data, "second line")
	}
}

// TestLineStripsNewline verifies that the returned byte slice does not
// include the trailing '\n'. If the newline were included, JSON
// unmarshal would fail because trailing whitespace after a JSON object
// is technically valid but trailing '\n' inside a field value is not.
func TestLineStripsNewline(t *testing.T) {
	f := createTestFile(t, "content\n")

	data, err := line(f, 0)
	if err != nil {
		t.Fatalf("line error: %v", err)
	}
	// Should not include the newline
	if len(data) > 0 && data[len(data)-1] == '\n' {
		t.Error("line should strip trailing newline")
	}
	if string(data) != "content" {
		t.Errorf("line = %q, want %q", data, "content")
	}
}

// TestLineAtEOF verifies that reading at EOF returns an error. If
// line() returned empty bytes with no error, callers would attempt to
// JSON-parse an empty slice, producing a confusing ErrCorruptRecord
// instead of a clear "no more data" signal.
func TestLineAtEOF(t *testing.T) {
	f := createTestFile(t, "content\n")

	_, err := line(f, 8) // offset at EOF
	if err == nil {
		t.Error("expected error at EOF")
	}
}

// TestLineNoTrailingNewline verifies that line() handles a file that
// doesn't end with '\n'. This happens after a crash mid-write where
// the newline was never flushed. line() must return the partial content
// rather than hanging, so that Repair can salvage whatever was written.
func TestLineNoTrailingNewline(t *testing.T) {
	f := createTestFile(t, "no newline")

	data, err := line(f, 0)
	if err != nil {
		t.Fatalf("line error: %v", err)
	}
	if string(data) != "no newline" {
		t.Errorf("line = %q, want %q", data, "no newline")
	}
}

// TestAlignFindNewline verifies that align() finds the next '\n' from
// a given offset. Binary search seeks to the midpoint of a byte range,
// which typically lands in the middle of a record. align() scans
// forward to the next newline so binary search can read the complete
// next line. If align() returned the wrong position, binary search
// would compare a partial record and make an incorrect left/right
// decision.
func TestAlignFindNewline(t *testing.T) {
	f := createTestFile(t, "first\nsecond\n")

	pos, err := align(f, 0)
	if err != nil {
		t.Fatalf("align error: %v", err)
	}
	if pos != 5 { // "first" is 5 bytes, newline at offset 5
		t.Errorf("align(0) = %d, want 5", pos)
	}
}

// TestAlignAtNewline verifies that align() returns the offset itself
// when already positioned on a newline. This prevents an off-by-one
// where binary search would skip one record every time it happened to
// land exactly on a line boundary.
func TestAlignAtNewline(t *testing.T) {
	f := createTestFile(t, "first\nsecond\n")

	// Newline is at offset 5
	pos, err := align(f, 5)
	if err != nil {
		t.Fatalf("align error: %v", err)
	}
	if pos != 5 {
		t.Errorf("align(5) = %d, want 5", pos)
	}
}

// TestAlignNoNewline verifies that align() returns -1 when no newline
// exists. This signals the end of the searchable range to binary search,
// preventing it from reading past the end of the file.
func TestAlignNoNewline(t *testing.T) {
	f := createTestFile(t, "no newline")

	pos, err := align(f, 0)
	if err != nil {
		t.Fatalf("align error: %v", err)
	}
	if pos != -1 {
		t.Errorf("align(0) = %d, want -1", pos)
	}
}

// TestAlignAtEOF verifies that align() returns -1 when called at the
// end of the file. This is the base case for binary search termination —
// without it, the search could loop forever trying to find a newline
// beyond the last byte.
func TestAlignAtEOF(t *testing.T) {
	f := createTestFile(t, "content\n")

	pos, err := align(f, 8) // at EOF
	if err != nil {
		t.Fatalf("align error: %v", err)
	}
	if pos != -1 {
		t.Errorf("align at EOF = %d, want -1", pos)
	}
}

// TestAlignMultipleNewlines verifies that align() finds the first
// newline from the given offset, not a later one. If it skipped to a
// subsequent newline, binary search would miss records, creating gaps
// in search results.
func TestAlignMultipleNewlines(t *testing.T) {
	f := createTestFile(t, "a\nb\nc\n")

	// Should find first newline at offset 1
	pos, err := align(f, 0)
	if err != nil {
		t.Fatalf("align error: %v", err)
	}
	if pos != 1 {
		t.Errorf("align(0) = %d, want 1", pos)
	}

	// From offset 2 should find newline at offset 3
	pos, err = align(f, 2)
	if err != nil {
		t.Fatalf("align error: %v", err)
	}
	if pos != 3 {
		t.Errorf("align(2) = %d, want 3", pos)
	}
}

// TestSize verifies that size() returns the file length. The file size
// determines the boundary of the sparse region — if size() were wrong,
// sparse() would either stop scanning early (missing documents) or read
// past the end of the file.
func TestSize(t *testing.T) {
	f := createTestFile(t, "hello world")

	s, err := size(f)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if s != 11 {
		t.Errorf("size = %d, want 11", s)
	}
}

// TestSizeEmpty verifies that size() returns 0 for an empty file. A
// fresh database file (before any writes) is exactly HeaderSize bytes,
// but this test uses a truly empty file to verify size() doesn't crash.
func TestSizeEmpty(t *testing.T) {
	f := createTestFile(t, "")

	s, err := size(f)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if s != 0 {
		t.Errorf("size(empty) = %d, want 0", s)
	}
}

// TestPosition verifies that position() returns the current file offset.
// This is used internally to track where the next read will occur. If
// position() returned a cached or stale value, sequential reads would
// re-read the same offset repeatedly.
func TestPosition(t *testing.T) {
	f := createTestFile(t, "content")

	// Initial position is 0
	pos, err := position(f)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if pos != 0 {
		t.Errorf("initial position = %d, want 0", pos)
	}

	// After seek
	f.Seek(5, 0)
	pos, err = position(f)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if pos != 5 {
		t.Errorf("after seek: position = %d, want 5", pos)
	}
}

// TestPositionAfterRead verifies that position() advances after a
// Read call. This confirms that position() queries the OS file
// descriptor rather than maintaining its own counter, which would
// drift if any other code seeked the file handle.
func TestPositionAfterRead(t *testing.T) {
	f := createTestFile(t, "content")

	buf := make([]byte, 3)
	f.Read(buf)

	pos, err := position(f)
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if pos != 3 {
		t.Errorf("after read: position = %d, want 3", pos)
	}
}
