package folio

import (
	"errors"
	"testing"
)

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !errors.Is(tt.err, tt.err) {
				t.Errorf("errors.Is(%v, %v) = false, want true", tt.err, tt.err)
			}
		})
	}
}
