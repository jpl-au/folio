// Compression for inline history snapshots.
//
// Each record's _h field stores the document content at the time of write.
// The content is Zstd-compressed for size, then Ascii85-encoded to produce
// a printable string that can be embedded directly in a JSON value without
// escaping. This avoids the 33% overhead of base64 while remaining
// newline-free (critical for the line-delimited format).
package folio

import (
	"bytes"
	"encoding/ascii85"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Shared encoder/decoder — both are documented as safe for concurrent use.
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	zstdDecoder, _ = zstd.NewReader(nil)
)

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

func decompress(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, nil
	}

	dec := ascii85.NewDecoder(bytes.NewReader([]byte(encoded)))
	compressed, err := io.ReadAll(dec)
	if err != nil {
		return nil, fmt.Errorf("%w: ascii85: %w", ErrDecompress, err)
	}

	out, err := zstdDecoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: zstd: %w", ErrDecompress, err)
	}
	return out, nil
}
