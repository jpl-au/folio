// Bloom filter unit tests.
//
// These verify the core probabilistic properties: no false negatives,
// bounded false-positive rate, and correct reset behaviour.
package bloom

import (
	"strconv"
	"testing"
)

// TestAddContains verifies the basic contract: after Add("x"),
// Contains("x") must return true. A false negative would cause the
// caller to skip a scan and miss a document that exists.
func TestAddContains(t *testing.T) {
	f := New()
	f.Add("abc123")
	if !f.Contains("abc123") {
		t.Error("Contains should return true for added ID")
	}
}

// TestMiss verifies that Contains returns false for an ID that was
// never added.
func TestMiss(t *testing.T) {
	f := New()
	f.Add("abc123")
	if f.Contains("xyz789") {
		t.Error("Contains should return false for absent ID")
	}
}

// TestReset verifies that Reset clears all bits.
func TestReset(t *testing.T) {
	f := New()
	f.Add("abc123")
	f.Reset()
	if f.Contains("abc123") {
		t.Error("Contains should return false after Reset")
	}
}

// TestFPRate measures the false-positive rate with 1000 entries and
// 10000 probes. The filter is sized for <1% FP rate; this test uses a
// 2% threshold to allow for statistical noise.
func TestFPRate(t *testing.T) {
	f := New()
	for i := range 1000 {
		f.Add("present-" + strconv.Itoa(i))
	}

	fp := 0
	tests := 10000
	for i := range tests {
		if f.Contains("absent-" + strconv.Itoa(i)) {
			fp++
		}
	}

	rate := float64(fp) / float64(tests)
	if rate > 0.02 {
		t.Errorf("false positive rate %.4f exceeds 2%%", rate)
	}
}
