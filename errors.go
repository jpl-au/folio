// Package folio provides an append-only document database backed by a single
// JSONL file. Documents are stored as newline-delimited JSON records with
// automatic versioning — every update preserves the previous content as a
// compressed history snapshot.
//
// The file is divided into a heap, index, and sparse region. The heap
// co-locates data and history records sorted by ID then timestamp, so all
// versions of a document are contiguous. Indexes follow the heap in sorted
// order. The sparse region collects new writes and is scanned linearly.
// Compaction merges the sparse region back into the heap. This design keeps
// all state on disk without requiring in-memory indexes, though an optional
// bloom filter can accelerate negative lookups in the sparse region.
package folio

import "errors"

// Sentinel errors for programmatic handling. Callers can use errors.Is to
// distinguish recoverable conditions (ErrNotFound) from corruption
// (ErrCorruptHeader, ErrCorruptRecord, ErrCorruptIndex, ErrDecompress).
var (
	ErrNotFound       = errors.New("document not found")
	ErrExists         = errors.New("document already exists")
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
