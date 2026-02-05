//go:build unix || linux || darwin

package folio

import (
	"syscall"
)

func (l *fileLock) lock(mode LockMode) error {
	op := syscall.LOCK_SH
	if mode == LockExclusive {
		op = syscall.LOCK_EX
	}
	// We want blocking behavior, so we don't add LOCK_NB
	return syscall.Flock(int(l.f.Fd()), op)
}

func (l *fileLock) unlock() error {
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
