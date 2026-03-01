//go:build unix || linux || darwin

// flock(2) implementation for Unix platforms.
// Both methods are called with l.mu held by the exported Acquire/Release.
package flock

import "syscall"

func (l *Lock) lock(mode Mode) error {
	op := syscall.LOCK_SH
	if mode == Exclusive {
		op = syscall.LOCK_EX
	}
	// Blocking flock — no LOCK_NB so the call waits for the lock.
	return syscall.Flock(int(l.f.Fd()), op)
}

func (l *Lock) unlock() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
