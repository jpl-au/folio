//go:build windows

// LockFileEx/UnlockFileEx implementation for Windows.
// Both methods are called with l.mu held by the exported Lock/Unlock.
package folio

import (
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002
	LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
)

func (l *fileLock) lock(mode LockMode) error {
	var flags uint32 = 0
	if mode == LockExclusive {
		flags |= LOCKFILE_EXCLUSIVE_LOCK
	}

	// Blocking lock over the entire file region (0 to max).
	h := syscall.Handle(l.f.Fd())
	var overlapped syscall.Overlapped

	r1, _, err := procLockFileEx.Call(
		uintptr(h),
		uintptr(flags),
		0,
		0xFFFFFFFF,
		0xFFFFFFFF,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if r1 == 0 {
		return err
	}
	return nil
}

func (l *fileLock) unlock() error {
	h := syscall.Handle(l.f.Fd())
	var overlapped syscall.Overlapped

	r1, _, err := procUnlockFileEx.Call(
		uintptr(h),
		0,
		0xFFFFFFFF,
		0xFFFFFFFF,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if r1 == 0 {
		return err
	}
	return nil
}
