// Compression utilities for history storage.
//
// Records store a compressed snapshot in the _h field for version retrieval.
// Content is Zstd-compressed then Ascii85-encoded to produce a JSON-safe string.
package folio

import (
	"bytes"
	"encoding/ascii85"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Package-level encoder/decoder for reuse (thread-safe).
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// compress applies Zstd compression then Ascii85 encoding.
// Returns a printable ASCII string suitable for JSON storage.
func compress(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	compressed := zstdEncoder.EncodeAll(data, nil)

	var encoded bytes.Buffer
	enc := ascii85.NewEncoder(&encoded)
	enc.Write(compressed)
	enc.Close()

	return encoded.String()
}

// decompress decodes Ascii85 then decompresses Zstd.
// Returns the original content.
func decompress(encoded string) []byte {
	if encoded == "" {
		return nil
	}

	dec := ascii85.NewDecoder(bytes.NewReader([]byte(encoded)))
	compressed, err := io.ReadAll(dec)
	if err != nil {
		return nil
	}

	out, err := zstdDecoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil
	}
	return out
}
