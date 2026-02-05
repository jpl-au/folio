//go:build windows

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
	// Windows lock flags
	LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002
	LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
)

func (l *fileLock) lock(mode LockMode) error {
	var flags uint32 = 0
	if mode == LockExclusive {
		flags |= LOCKFILE_EXCLUSIVE_LOCK
	}

	// Lock bytes 0 to max_uint32 (effectively the whole file region for our purposes)
	// We overlay strict locking on the file handle.

	h := syscall.Handle(l.f.Fd())
	var overlapped syscall.Overlapped

	// 0, 0, 0xFFFFFFFF, 0xFFFFFFFF = Lock region 0 to max
	r1, _, err := procLockFileEx.Call(
		uintptr(h),
		uintptr(flags),
		0,          // Reserved
		0xFFFFFFFF, // Low bytes of length
		0xFFFFFFFF, // High bytes of length
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
		0, // Reserved
		0xFFFFFFFF,
		0xFFFFFFFF,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if r1 == 0 {
		return err
	}
	return nil
}
