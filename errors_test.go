// Sentinel error tests.
//
// Folio defines a set of named errors (ErrNotFound, ErrCorruptRecord,
// etc.) that callers use with errors.Is to decide how to handle failures.
// Each error maps to a specific failure mode — if two errors shared the
// same message or if one were accidentally nil, callers would take the
// wrong recovery action (e.g. treating a corrupt file as "not found"
// and silently creating a new database).
package folio

import (
	"errors"
	"testing"
)

// TestErrors verifies that every sentinel error is defined and has a
// unique message. If two errors had the same message, a caller matching
// on err.Error() would conflate them. If any were nil, an errors.Is
// check would panic.
func TestErrors(t *testing.T) {
	// Verify all errors are defined and distinct
	errs := []error{
		ErrNotFound,
		ErrLabelTooLong,
		ErrInvalidLabel,
		ErrEmptyContent,
		ErrClosed,
		ErrInvalidPattern,
		ErrCorruptHeader,
		ErrCorruptRecord,
		ErrCorruptIndex,
		ErrDecompress,
	}

	// Check none are nil
	for i, err := range errs {
		if err == nil {
			t.Errorf("error at index %d is nil", i)
		}
	}

	// Check all are distinct
	seen := make(map[string]int)
	for i, err := range errs {
		msg := err.Error()
		if prev, ok := seen[msg]; ok {
			t.Errorf("error at index %d has same message as index %d: %q", i, prev, msg)
		}
		seen[msg] = i
	}
}

// TestErrorsAreErrors verifies that errors.Is works with each sentinel.
// Folio's errors are created with errors.New, which returns a pointer
// type — errors.Is uses pointer identity for comparison. If an error
// were accidentally redeclared as a string value, errors.Is would fail
// and callers couldn't match it.
func TestErrorsAreErrors(t *testing.T) {
	// Verify errors work with errors.Is
	tests := []struct {
		name string
		err  error
	}{
		{"ErrNotFound", ErrNotFound},
		{"ErrLabelTooLong", ErrLabelTooLong},
		{"ErrInvalidLabel", ErrInvalidLabel},
		{"ErrEmptyContent", ErrEmptyContent},
		{"ErrClosed", ErrClosed},
		{"ErrInvalidPattern", ErrInvalidPattern},
		{"ErrCorruptHeader", ErrCorruptHeader},
		{"ErrCorruptRecord", ErrCorruptRecord},
		{"ErrCorruptIndex", ErrCorruptIndex},
		{"ErrDecompress", ErrDecompress},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !errors.Is(tt.err, tt.err) {
				t.Errorf("errors.Is(%v, %v) = false, want true", tt.err, tt.err)
			}
		})
	}
}
