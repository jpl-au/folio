//go:build unix || linux || darwin

// flock(2) implementation for Unix platforms.
// Both methods are called with l.mu held by the exported Lock/Unlock.
package folio

import "syscall"

func (l *fileLock) lock(mode LockMode) error {
	op := syscall.LOCK_SH
	if mode == LockExclusive {
		op = syscall.LOCK_EX
	}
	// Blocking flock — no LOCK_NB so the call waits for the lock.
	return syscall.Flock(int(l.f.Fd()), op)
}

func (l *fileLock) unlock() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
