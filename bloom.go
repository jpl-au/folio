// In-memory bloom filter for sparse region lookups.
//
// Sized for ~10k entries at 1% false positive rate. Built on Open from
// sparse IDs, maintained during the session, discarded on Close.
package folio

import (
	"hash/fnv"
)

// Bloom filter sizing constants.
const (
	BloomSize = 11982 // bytes, ~96k bits for 10k entries at 1% FP
	BloomK    = 7     // number of hash functions
)

type bloom struct {
	bits []byte
}

// newBloom returns a zeroed bloom filter.
func newBloom() *bloom {
	return &bloom{bits: make([]byte, BloomSize)}
}

// Add inserts an ID into the filter.
func (b *bloom) Add(id string) {
	for _, pos := range positions(id) {
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

// Contains returns true if the ID might be present, false if definitely absent.
func (b *bloom) Contains(id string) bool {
	for _, pos := range positions(id) {
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Reset clears all bits.
func (b *bloom) Reset() {
	clear(b.bits)
}

// positions returns BloomK bit positions using double hashing (FNV-64a + FNV-32a).
func positions(id string) [BloomK]uint {
	h64 := fnv.New64a()
	h64.Write([]byte(id))
	a := h64.Sum64()

	h32 := fnv.New32a()
	h32.Write([]byte(id))
	b := uint(h32.Sum32())

	nbits := uint(BloomSize * 8)
	var pos [BloomK]uint
	for i := range BloomK {
		pos[i] = (uint(a) + uint(i)*b) % nbits
	}
	return pos
}
