// Hash function correctness tests.
//
// The hash function maps a human-readable label (e.g. "config/db") to
// a 16-character hex ID that serves as the primary key for all lookups.
// Every record and index stores this ID at a fixed byte position (bytes
// 16–31) so that binary search can compare IDs without parsing JSON.
//
// Three properties are essential:
//  1. Determinism — the same label must always produce the same ID,
//     otherwise a document written with one ID could never be found.
//  2. Output format — exactly 16 lowercase hex characters, because
//     the fixed-position field extraction in scanm and binary search
//     assumes a 16-byte ID at a known offset.
//  3. Algorithm independence — different algorithms must produce
//     different IDs for the same label, so Rehash can detect stale
//     indexes and rebuild them.
package folio

import (
	"regexp"
	"testing"
)

var hexPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// TestHashXXHash3 verifies that the default algorithm produces a valid
// 16-hex-char ID. xxHash3 is the fastest option and the default for new
// databases. If it produced the wrong length, every fixed-position field
// after the ID (timestamp, label, data) would be at the wrong offset.
func TestHashXXHash3(t *testing.T) {
	result := hash("test", AlgXXHash3)
	if !hexPattern.MatchString(result) {
		t.Errorf("xxHash3 did not produce 16 hex chars: %q", result)
	}
}

// TestHashFNV1a verifies the FNV-1a alternative. This is a simpler hash
// with no external dependencies, offered for environments where xxHash3
// or Blake2b are unavailable.
func TestHashFNV1a(t *testing.T) {
	result := hash("test", AlgFNV1a)
	if !hexPattern.MatchString(result) {
		t.Errorf("FNV-1a did not produce 16 hex chars: %q", result)
	}
}

// TestHashBlake2b verifies the cryptographic alternative. Blake2b is
// slower but provides stronger collision resistance for security-
// sensitive use cases.
func TestHashBlake2b(t *testing.T) {
	result := hash("test", AlgBlake2b)
	if !hexPattern.MatchString(result) {
		t.Errorf("Blake2b did not produce 16 hex chars: %q", result)
	}
}

// TestHashDeterministic verifies that hashing the same label twice
// produces the same ID. Without determinism, a Set followed by a Get
// would compute different IDs and the document would be unfindable.
func TestHashDeterministic(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		h1 := hash("foo", alg)
		h2 := hash("foo", alg)
		if h1 != h2 {
			t.Errorf("alg %d: same label produced different hashes: %q vs %q", alg, h1, h2)
		}
	}
}

// TestHashDifferentLabels verifies that "foo" and "bar" produce
// different IDs. If they collided, Set("foo") then Set("bar") would
// overwrite the same document — silent data loss.
func TestHashDifferentLabels(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		h1 := hash("foo", alg)
		h2 := hash("bar", alg)
		if h1 == h2 {
			t.Errorf("alg %d: different labels produced same hash: %q", alg, h1)
		}
	}
}

// TestHashDifferentAlgorithms verifies that each algorithm produces a
// different ID for the same input. Rehash migrates from one algorithm
// to another by recomputing all IDs; if two algorithms produced the
// same IDs, Rehash would be a no-op and the migration would silently
// do nothing.
func TestHashDifferentAlgorithms(t *testing.T) {
	h1 := hash("foo", AlgXXHash3)
	h2 := hash("foo", AlgFNV1a)
	h3 := hash("foo", AlgBlake2b)

	if h1 == h2 || h1 == h3 || h2 == h3 {
		t.Errorf("same label with different algs produced same hash: xxh3=%q fnv=%q blake2b=%q", h1, h2, h3)
	}
}

// TestHashEmptyLabel verifies that an empty string produces a valid
// hash rather than panicking. Although Set rejects empty labels at a
// higher level, hash() must be safe for all inputs because it's also
// called during compaction where labels are read from existing records.
func TestHashEmptyLabel(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		result := hash("", alg)
		if !hexPattern.MatchString(result) {
			t.Errorf("alg %d: empty label did not produce valid hash: %q", alg, result)
		}
	}
}

// TestHashInvalidAlgorithm verifies that an unrecognised algorithm ID
// returns an empty string. This prevents silent misuse — if hash()
// returned a deterministic value for unknown algorithms, documents
// would be written with a made-up ID that no valid algorithm could
// reproduce, making them permanently unreachable.
func TestHashInvalidAlgorithm(t *testing.T) {
	result := hash("test", 99)
	if result != "" {
		t.Errorf("invalid alg should return empty string, got: %q", result)
	}
}

// TestHashAlgorithmConstants guards the numeric values stored in the
// header's Algorithm field. These values are persisted on disk — if a
// constant changed (e.g. AlgFNV1a became 3), existing databases would
// use the wrong hash function on reopen, producing different IDs for
// every label and making all documents unfindable.
func TestHashAlgorithmConstants(t *testing.T) {
	if AlgXXHash3 != 1 {
		t.Errorf("AlgXXHash3 = %d, want 1", AlgXXHash3)
	}
	if AlgFNV1a != 2 {
		t.Errorf("AlgFNV1a = %d, want 2", AlgFNV1a)
	}
	if AlgBlake2b != 3 {
		t.Errorf("AlgBlake2b = %d, want 3", AlgBlake2b)
	}
}
