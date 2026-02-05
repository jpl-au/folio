package folio

import (
	"bytes"
	"testing"
)

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
			decoded := decompress(encoded)

			if !bytes.Equal(decoded, tt.data) {
				t.Errorf("round trip failed: got %v, want %v", decoded, tt.data)
			}
		})
	}
}

func TestCompressEmpty(t *testing.T) {
	result := compress([]byte{})
	if result != "" {
		t.Errorf("compress(empty) = %q, want empty string", result)
	}
}

func TestDecompressEmpty(t *testing.T) {
	result := decompress("")
	if result != nil {
		t.Errorf("decompress(empty) = %v, want nil", result)
	}
}

func TestCompressLargeData(t *testing.T) {
	// 1MB of data
	data := bytes.Repeat([]byte("test data for compression "), 40000)

	encoded := compress(data)
	decoded := decompress(encoded)

	if !bytes.Equal(decoded, data) {
		t.Errorf("large data round trip failed: lengths got %d, want %d", len(decoded), len(data))
	}
}

func TestCompressReducesSize(t *testing.T) {
	// Highly repetitive content should compress well
	data := bytes.Repeat([]byte("aaaaaaaaaa"), 1000)

	encoded := compress(data)

	if len(encoded) >= len(data) {
		t.Errorf("compression did not reduce size: encoded %d >= original %d", len(encoded), len(data))
	}
}

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

func TestCompressBinaryData(t *testing.T) {
	// All possible byte values
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	encoded := compress(data)
	decoded := decompress(encoded)

	if !bytes.Equal(decoded, data) {
		t.Error("binary data round trip failed")
	}
}
