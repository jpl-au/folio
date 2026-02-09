// Package folio provides an append-only document database backed by a single
// JSONL file. Documents are stored as newline-delimited JSON records with
// automatic versioning — every update preserves the previous content as a
// compressed history snapshot.
//
// The file is divided into sorted and sparse regions. Sorted regions support
// binary search after compaction; the sparse region collects new writes and
// is scanned linearly. Compaction merges the sparse region back into sorted
// order. This design keeps all state on disk without requiring in-memory
// indexes, though an optional bloom filter can accelerate negative lookups
// in the sparse region.
package folio

import "errors"

// Sentinel errors for programmatic handling. Callers can use errors.Is to
// distinguish recoverable conditions (ErrNotFound) from corruption
// (ErrCorruptHeader, ErrCorruptRecord, ErrCorruptIndex, ErrDecompress).
var (
	ErrNotFound       = errors.New("document not found")
	ErrLabelTooLong   = errors.New("label exceeds maximum size")
	ErrInvalidLabel   = errors.New("label contains invalid characters")
	ErrEmptyContent   = errors.New("content cannot be empty")
	ErrClosed         = errors.New("database is closed")
	ErrInvalidPattern = errors.New("invalid regex pattern")
	ErrCorruptHeader  = errors.New("corrupt header")
	ErrCorruptRecord  = errors.New("corrupt record")
	ErrCorruptIndex   = errors.New("corrupt index")
	ErrDecompress     = errors.New("decompression failed")
)
