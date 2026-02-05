package folio

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHeaderSize(t *testing.T) {
	if HeaderSize != 128 {
		t.Errorf("HeaderSize = %d, want 128", HeaderSize)
	}
}

func TestHeaderEncode(t *testing.T) {
	h := &Header{
		Error:     0,
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
		Data:      5000,
		Index:     6000,
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

func TestHeaderEncodeFreshDB(t *testing.T) {
	h := &Header{
		Error:     0,
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
		Data:      0,
		Index:     0,
	}

	buf, err := h.encode()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if len(buf) != HeaderSize {
		t.Errorf("fresh header length = %d, want %d", len(buf), HeaderSize)
	}
}

func TestHeaderReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	// Write header
	original := &Header{
		Error:     0,
		Algorithm: AlgFNV1a,
		Timestamp: 1706000000000,
		Data:      1000,
		Index:     2000,
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
	if h.Data != original.Data {
		t.Errorf("Data = %d, want %d", h.Data, original.Data)
	}
	if h.Index != original.Index {
		t.Errorf("Index = %d, want %d", h.Index, original.Index)
	}
}

func TestHeaderDirtyFlag(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	// Create file with clean header
	h := &Header{
		Error:     0,
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
		Data:      0,
		Index:     0,
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

func TestHeaderDirtyPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.folio")

	h := &Header{
		Error:     0,
		Algorithm: AlgXXHash3,
		Timestamp: 1706000000000,
		Data:      0,
		Index:     0,
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

func TestHeaderAllAlgorithms(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		h := &Header{
			Error:     0,
			Algorithm: alg,
			Timestamp: 1706000000000,
			Data:      0,
			Index:     0,
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
