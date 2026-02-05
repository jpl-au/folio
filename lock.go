package folio

import "os"

// LockMode represents the type of lock to acquire.
type LockMode int

const (
	LockShared    LockMode = iota // Read lock
	LockExclusive                 // Write lock
)

// fileLock handles OS-specific file locking.
type fileLock struct {
	f *os.File
}

// Lock acquires a lock on the file.
// Returns an error if the lock cannot be acquired immediately (non-blocking) convention
// or if the syscall fails.
// Note: The actual blocking/non-blocking behavior depends on the implementation.
// For this database, we generally want blocking behavior for operations.
func (l *fileLock) Lock(mode LockMode) error {
	return l.lock(mode)
}

// Unlock releases the lock.
func (l *fileLock) Unlock() error {
	return l.unlock()
}
