package folio

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestLineAtEOF(t *testing.T) {
	f := createTestFile(t, "content\n")

	_, err := line(f, 8) // offset at EOF
	if err == nil {
		t.Error("expected error at EOF")
	}
}

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

func TestSize(t *testing.T) {
	f := createTestFile(t, "hello world")

	s := size(f)
	if s != 11 {
		t.Errorf("size = %d, want 11", s)
	}
}

func TestSizeEmpty(t *testing.T) {
	f := createTestFile(t, "")

	s := size(f)
	if s != 0 {
		t.Errorf("size(empty) = %d, want 0", s)
	}
}

func TestPosition(t *testing.T) {
	f := createTestFile(t, "content")

	// Initial position is 0
	if pos := position(f); pos != 0 {
		t.Errorf("initial position = %d, want 0", pos)
	}

	// After seek
	f.Seek(5, 0)
	if pos := position(f); pos != 5 {
		t.Errorf("after seek: position = %d, want 5", pos)
	}
}

func TestPositionAfterRead(t *testing.T) {
	f := createTestFile(t, "content")

	buf := make([]byte, 3)
	f.Read(buf)

	if pos := position(f); pos != 3 {
		t.Errorf("after read: position = %d, want 3", pos)
	}
}
