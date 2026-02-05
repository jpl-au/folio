// Package folio provides a disk-first document database with versioning.
//
// Folio stores JSON documents in an append-only file format with automatic
// history tracking. All operations work efficiently from disk without
// requiring in-memory indexes.
package folio

import "errors"

// Sentinel errors returned by database operations.
var (
	// ErrNotFound is returned when a document does not exist.
	ErrNotFound = errors.New("document not found")

	// ErrLabelTooLong is returned when a label exceeds MaxLabelSize bytes.
	ErrLabelTooLong = errors.New("label exceeds maximum size")

	// ErrInvalidLabel is returned when a label contains prohibited characters.
	ErrInvalidLabel = errors.New("label contains invalid characters")

	// ErrEmptyContent is returned when attempting to store empty content.
	ErrEmptyContent = errors.New("content cannot be empty")

	// ErrClosed is returned when operating on a closed database.
	ErrClosed = errors.New("database is closed")

	// ErrInvalidPattern is returned when a regex pattern fails to compile.
	ErrInvalidPattern = errors.New("invalid regex pattern")

	// ErrCorruptHeader is returned when the header cannot be parsed.
	ErrCorruptHeader = errors.New("corrupt header")

	// ErrCorruptRecord is returned when a data record cannot be parsed.
	ErrCorruptRecord = errors.New("corrupt record")

	// ErrCorruptIndex is returned when an index record cannot be parsed.
	ErrCorruptIndex = errors.New("corrupt index")
)
