// Hash algorithms for deriving the _id field from a document label.
//
// _id is always 16 hex characters (64 bits). This fixed width is what
// allows scanm to extract IDs at a known byte offset without parsing.
// The algorithm is stored in the header so all records in a file use the
// same one; Rehash can migrate between algorithms in place because the
// output width is identical across all three.
package folio

import (
	"fmt"
	"hash/fnv"

	"github.com/zeebo/xxh3"
	"golang.org/x/crypto/blake2b"
)

const (
	AlgXXHash3 = 1 // default — fastest, good distribution
	AlgFNV1a   = 2 // stdlib only, no external dependencies
	AlgBlake2b = 3 // cryptographic quality distribution
)

func hash(label string, alg int) string {
	switch alg {
	case AlgXXHash3:
		h := xxh3.HashString(label)
		return fmt.Sprintf("%016x", h)
	case AlgFNV1a:
		h := fnv.New64a()
		h.Write([]byte(label))
		return fmt.Sprintf("%016x", h.Sum64())
	case AlgBlake2b:
		h, _ := blake2b.New(8, nil) // 8 bytes = 64 bits
		h.Write([]byte(label))
		return fmt.Sprintf("%016x", h.Sum(nil))
	default:
		return ""
	}
}
