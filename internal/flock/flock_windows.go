//go:build windows

// LockFileEx/UnlockFileEx implementation for Windows.
// Both methods are called with l.mu held by the exported Acquire/Release.
package flock

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
	lockfileExclusiveLock = 0x00000002
)

func (l *Lock) lock(mode Mode) error {
	var flags uint32 = 0
	if mode == Exclusive {
		flags |= lockfileExclusiveLock
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

func (l *Lock) unlock() error {
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
