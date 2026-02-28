//go:build !unix

// Stub for platforms without mmap support.
package folio

import (
	"errors"
	"os"
)

// mmapFile is a no-op stub on non-unix platforms.
func mmapFile(_ *os.File, _ int64) ([]byte, error) {
	return nil, errors.New("mmap: not supported on this platform")
}

// munmapFile is a no-op stub on non-unix platforms.
func munmapFile(_ []byte) error { return nil }
