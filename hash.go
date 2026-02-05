// Hash algorithm implementations for document identifiers.
//
// The _id field is a 16 hex character hash of the label. Three algorithms
// are supported, selectable via Config.HashAlgorithm.
package folio

import (
	"fmt"
	"hash/fnv"

	"github.com/zeebo/xxh3"
	"golang.org/x/crypto/blake2b"
)

// Hash algorithm constants.
const (
	AlgXXHash3 = 1 // Default, fastest
	AlgFNV1a   = 2 // No external dependencies
	AlgBlake2b = 3 // Best distribution
)

// hash generates a 16 hex character ID from a label using the specified algorithm.
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
