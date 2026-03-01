// Package bloom provides a probabilistic set membership filter.
//
// The filter is sized for ~10k entries at a 1% false positive rate
// (~12KB). False positives add a linear scan that would have happened
// without the filter, so the cost of over-counting is low. False
// negatives are impossible by construction.
package bloom

import (
	"hash/fnv"
)

const (
	Size = 11982 // ~96k bits: -(10000*ln(0.01))/(ln(2)^2)
	K    = 7     // optimal k: (Size*8/10000)*ln(2)
)

// Filter is a bloom filter backed by a fixed-size bit array.
type Filter struct {
	bits []byte
}

// New returns a filter with all bits cleared.
func New() *Filter {
	return &Filter{bits: make([]byte, Size)}
}

// Add marks id as present.
func (f *Filter) Add(id string) {
	for _, pos := range positions(id) {
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

// Contains reports whether id might be present. A false return is
// definitive; a true return may be a false positive.
func (f *Filter) Contains(id string) bool {
	for _, pos := range positions(id) {
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Reset clears all bits. Called after compaction because the sparse
// region is empty in the new file and the filter must be rebuilt from
// new appends.
func (f *Filter) Reset() {
	clear(f.bits)
}

// positions derives K bit indices using double hashing: h(i) = h1 + i*h2.
// Two independent hashes (FNV-64a, FNV-32a) simulate k independent functions.
//
// FNV is used here (not xxHash3, which is already a folio dependency) because
// the bloom filter needs hash functions independent from the ID hash. If the
// same algorithm generated both IDs and bloom positions, correlated
// collisions would produce correlated false positives, degrading the
// filter's effectiveness. FNV provides that independence and is stdlib-only.
func positions(id string) [K]uint {
	h64 := fnv.New64a()
	h64.Write([]byte(id))
	a := h64.Sum64()

	h32 := fnv.New32a()
	h32.Write([]byte(id))
	b := uint(h32.Sum32())

	nbits := uint(Size * 8)
	var pos [K]uint
	for i := range K {
		pos[i] = (uint(a) + uint(i)*b) % nbits
	}
	return pos
}
