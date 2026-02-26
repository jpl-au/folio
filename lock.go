// OS-level file locking for cross-process coordination.
//
// fileLock wraps flock(2) / LockFileEx with a mutex that guards the file
// handle's lifetime. The mutex is held for the entire duration of the flock
// syscall so that Fd() cannot race with Close() on the same *os.File.
//
// Callers use setFile(nil) before closing the underlying file. This blocks
// until any in-flight flock completes, then makes subsequent Lock/Unlock
// calls no-ops. After reopening, setFile(f) restores normal operation.
package folio

import (
	"os"
	"sync"
)

// LockMode selects shared (read) or exclusive (write) locking.
type LockMode int

const (
	LockShared LockMode = iota
	LockExclusive
)

// fileLock coordinates OS-level file locks with safe handle teardown.
// The mu field serialises flock syscalls against setFile so that a
// concurrent Close cannot invalidate the fd mid-syscall.
type fileLock struct {
	mu sync.Mutex
	f  *os.File
}

// Lock acquires a shared or exclusive flock. Returns nil immediately
// if the handle has been cleared via setFile(nil).
func (l *fileLock) Lock(mode LockMode) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	return l.lock(mode)
}

// Unlock releases the flock. Returns nil immediately if the handle
// has been cleared via setFile(nil).
func (l *fileLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	return l.unlock()
}

// setFile swaps the underlying file handle. Passing nil drains any
// in-flight flock (blocks until the mutex is available) and disables
// further locking. Used by Close and Repair before closing the fd.
func (l *fileLock) setFile(f *os.File) {
	l.mu.Lock()
	l.f = f
	l.mu.Unlock()
}
