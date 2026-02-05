package folio

import (
	"regexp"
	"testing"
)

var hexPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

func TestHashXXHash3(t *testing.T) {
	result := hash("test", AlgXXHash3)
	if !hexPattern.MatchString(result) {
		t.Errorf("xxHash3 did not produce 16 hex chars: %q", result)
	}
}

func TestHashFNV1a(t *testing.T) {
	result := hash("test", AlgFNV1a)
	if !hexPattern.MatchString(result) {
		t.Errorf("FNV-1a did not produce 16 hex chars: %q", result)
	}
}

func TestHashBlake2b(t *testing.T) {
	result := hash("test", AlgBlake2b)
	if !hexPattern.MatchString(result) {
		t.Errorf("Blake2b did not produce 16 hex chars: %q", result)
	}
}

func TestHashDeterministic(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		h1 := hash("foo", alg)
		h2 := hash("foo", alg)
		if h1 != h2 {
			t.Errorf("alg %d: same label produced different hashes: %q vs %q", alg, h1, h2)
		}
	}
}

func TestHashDifferentLabels(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		h1 := hash("foo", alg)
		h2 := hash("bar", alg)
		if h1 == h2 {
			t.Errorf("alg %d: different labels produced same hash: %q", alg, h1)
		}
	}
}

func TestHashDifferentAlgorithms(t *testing.T) {
	h1 := hash("foo", AlgXXHash3)
	h2 := hash("foo", AlgFNV1a)
	h3 := hash("foo", AlgBlake2b)

	if h1 == h2 || h1 == h3 || h2 == h3 {
		t.Errorf("same label with different algs produced same hash: xxh3=%q fnv=%q blake2b=%q", h1, h2, h3)
	}
}

func TestHashEmptyLabel(t *testing.T) {
	for _, alg := range []int{AlgXXHash3, AlgFNV1a, AlgBlake2b} {
		result := hash("", alg)
		if !hexPattern.MatchString(result) {
			t.Errorf("alg %d: empty label did not produce valid hash: %q", alg, result)
		}
	}
}

func TestHashInvalidAlgorithm(t *testing.T) {
	result := hash("test", 99)
	if result != "" {
		t.Errorf("invalid alg should return empty string, got: %q", result)
	}
}

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
