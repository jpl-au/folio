// Package flock provides OS-level file locking for cross-process coordination.
//
// Lock wraps flock(2) / LockFileEx with a mutex that guards the file
// handle's lifetime. The mutex is held for the entire duration of the flock
// syscall so that Fd() cannot race with Close() on the same *os.File.
//
// Callers use SetFile(nil) before closing the underlying file. This blocks
// until any in-flight flock completes, then makes subsequent Lock/Unlock
// calls no-ops. After reopening, SetFile(f) restores normal operation.
package flock

import (
	"os"
	"sync"
)

// Mode selects shared (read) or exclusive (write) locking.
type Mode int

const (
	Shared Mode = iota
	Exclusive
)

// Lock coordinates OS-level file locks with safe handle teardown.
// The mu field serialises flock syscalls against SetFile so that a
// concurrent Close cannot invalidate the fd mid-syscall.
type Lock struct {
	mu sync.Mutex
	f  *os.File
}

// New returns a Lock backed by the given file descriptor.
func New(f *os.File) *Lock {
	return &Lock{f: f}
}

// Acquire acquires a shared or exclusive flock. Returns nil immediately
// if the handle has been cleared via SetFile(nil).
func (l *Lock) Acquire(mode Mode) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	return l.lock(mode)
}

// Release releases the flock. Returns nil immediately if the handle
// has been cleared via SetFile(nil).
func (l *Lock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	return l.unlock()
}

// SetFile swaps the underlying file handle. Passing nil drains any
// in-flight flock (blocks until the mutex is available) and disables
// further locking. Used by Close and Repair before closing the fd.
func (l *Lock) SetFile(f *os.File) {
	l.mu.Lock()
	l.f = f
	l.mu.Unlock()
}
