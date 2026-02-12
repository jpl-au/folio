// Compression round-trip tests.
//
// History versions are stored in the _h field as a zstd-compressed,
// ascii85-encoded string. Compression reduces file size (especially for
// documents with many versions), and ascii85 ensures the compressed
// bytes are safe to embed in a JSON string value without escaping.
//
// A compression bug has two failure modes: silent data corruption (the
// decompressed output differs from the original) or a crash during
// decompression (invalid zstd frame). Either would cause History to
// return wrong versions or fail entirely. These tests verify that every
// byte survives the round-trip for a variety of inputs: text, binary,
// empty, unicode, and large payloads.
package folio

import (
	"bytes"
	"testing"
)

// TestCompressDecompressRoundTrip verifies that compress→decompress is
// the identity function for a range of input types. Each input exercises
// a different edge case in zstd or ascii85: empty input (zstd produces
// a minimal frame), binary data (ascii85 must handle all 256 byte
// values), unicode (multi-byte sequences must not be split), and JSON
// (the most common real-world payload for document content).
func TestCompressDecompressRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"simple text", []byte("hello world")},
		{"empty", []byte{}},
		{"single byte", []byte{0x42}},
		{"binary data", []byte{0x00, 0x01, 0xff, 0xfe, 0x80, 0x7f}},
		{"unicode", []byte("日本語テキスト")},
		{"json", []byte(`{"key": "value", "num": 123}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := compress(tt.data)
			decoded, err := decompress(encoded)
			if err != nil {
				t.Fatalf("decompress: %v", err)
			}

			if !bytes.Equal(decoded, tt.data) {
				t.Errorf("round trip failed: got %v, want %v", decoded, tt.data)
			}
		})
	}
}

// TestCompressEmpty verifies that compressing empty input returns an
// empty string rather than a minimal zstd frame. This is an
// optimisation: a document with no history has _h:"", and decompress("")
// must return nil without attempting to decode a zstd frame.
func TestCompressEmpty(t *testing.T) {
	result := compress([]byte{})
	if result != "" {
		t.Errorf("compress(empty) = %q, want empty string", result)
	}
}

// TestDecompressEmpty verifies the empty-string fast path in
// decompress. History calls decompress on every _h value; a new
// document that has never been compacted has _h:"". If decompress
// tried to ascii85-decode an empty string, it would error, preventing
// History from working on any document that hasn't been compacted yet.
func TestDecompressEmpty(t *testing.T) {
	result, err := decompress("")
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if result != nil {
		t.Errorf("decompress(empty) = %v, want nil", result)
	}
}

// TestCompressLargeData verifies a 1MB round-trip. A document that has
// been updated thousands of times accumulates a large _h field. If the
// zstd encoder had a buffer size limit or the ascii85 encoder lost
// trailing bytes, the decompressed history would be silently truncated.
func TestCompressLargeData(t *testing.T) {
	// 1MB of data
	data := bytes.Repeat([]byte("test data for compression "), 40000)

	encoded := compress(data)
	decoded, err := decompress(encoded)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("large data round trip failed: lengths got %d, want %d", len(decoded), len(data))
	}
}

// TestCompressReducesSize verifies that highly repetitive content
// compresses to fewer bytes than the original. Without compression,
// the _h field would be larger than the document content itself for
// frequently-updated documents. If zstd were misconfigured (e.g.
// level 0 = no compression), the file would grow linearly with each
// compaction instead of sublinearly.
func TestCompressReducesSize(t *testing.T) {
	// Highly repetitive content should compress well
	data := bytes.Repeat([]byte("aaaaaaaaaa"), 1000)

	encoded := compress(data)

	if len(encoded) >= len(data) {
		t.Errorf("compression did not reduce size: encoded %d >= original %d", len(encoded), len(data))
	}
}

// TestCompressOutputPrintable verifies that every byte in the compressed
// output falls within the ascii85 printable range (33–117). The _h field
// is stored as a JSON string value — if the output contained control
// characters or double quotes, it would break the JSON structure of the
// entire record line, making it unparseable by decode().
func TestCompressOutputPrintable(t *testing.T) {
	data := []byte("test content for ascii85 encoding")
	encoded := compress(data)

	for i, b := range encoded {
		if b < 33 || b > 117 {
			// Ascii85 uses printable chars from ! (33) to u (117)
			t.Errorf("non-printable byte at position %d: %d (%c)", i, b, b)
		}
	}
}

// TestCompressBinaryData verifies that all 256 possible byte values
// survive the round-trip. zstd handles arbitrary binary data, but
// ascii85 encodes in 5-byte groups and pads the last group — if the
// padding logic were wrong, the last 1–3 bytes of the decompressed
// output would be corrupt, which for zstd means an invalid frame
// trailer and a decompression error.
func TestCompressBinaryData(t *testing.T) {
	// All possible byte values
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	encoded := compress(data)
	decoded, err := decompress(encoded)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}

	if !bytes.Equal(decoded, data) {
		t.Error("binary data round trip failed")
	}
}
