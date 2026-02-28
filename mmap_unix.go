//go:build unix

// Memory-mapped file I/O for Unix platforms.
//
// mmapFile maps a file read-only with MAP_SHARED so that in-place
// patches made through the writer fd are visible immediately (both fds
// share the kernel page cache). Only appends that extend the file
// beyond the mapped region require a remap.
package folio

import (
	"os"
	"syscall"
)

// mmapFile maps the entire file read-only. The caller must hold the
// write lock to prevent concurrent remaps.
func mmapFile(f *os.File, sz int64) ([]byte, error) {
	if sz == 0 {
		return nil, nil
	}
	return syscall.Mmap(int(f.Fd()), 0, int(sz), syscall.PROT_READ, syscall.MAP_SHARED)
}

// munmapFile releases a mapping created by mmapFile.
func munmapFile(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.Munmap(data)
}
