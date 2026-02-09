// Optional bloom filter to accelerate negative lookups in the sparse region.
//
// The sparse region is scanned linearly, so lookups for IDs that do not
// exist there pay the full scan cost. When enabled (Config.BloomFilter),
// the filter is populated from sparse index IDs at Open and updated on
// each Set. A negative Contains result skips the sparse scan entirely.
// The filter is deliberately small (~12KB) — sized for ~10k entries at
// a 1% false positive rate — because false positives only add a linear
// scan that would have happened anyway without the filter.
package folio

import (
	"hash/fnv"
)

const (
	BloomSize = 11982 // ~96k bits: -(10000*ln(0.01))/(ln(2)^2)
	BloomK    = 7     // optimal k: (BloomSize*8/10000)*ln(2)
)

type bloom struct {
	bits []byte
}

func newBloom() *bloom {
	return &bloom{bits: make([]byte, BloomSize)}
}

func (b *bloom) Add(id string) {
	for _, pos := range positions(id) {
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *bloom) Contains(id string) bool {
	for _, pos := range positions(id) {
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Reset clears all bits. Called after compaction because the sparse region
// is empty in the new file and the filter must be rebuilt from new appends.
func (b *bloom) Reset() {
	clear(b.bits)
}

// positions derives BloomK bit indices using double hashing: h(i) = h1 + i*h2.
// Two independent hashes (FNV-64a, FNV-32a) simulate k independent functions.
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
