// Low-level read primitives for the newline-delimited record format.
//
// Every record is a single JSON line terminated by '\n'. These functions
// read individual lines and find record boundaries via io.ReaderAt so
// that concurrent readers do not interfere with each other's offsets.
//
// All read functions accept a source (an io.ReaderAt with a known size)
// rather than *os.File directly. When memory-mapping is enabled the
// source wraps the mmap'd region; otherwise it wraps the read-only fd.
package folio

import (
	"bufio"
	"io"
	"os"
)

// source provides position-independent reads over either a file
// descriptor or a memory-mapped region. All scan and read functions
// accept a source instead of *os.File.
type source struct {
	io.ReaderAt
	sz int64
}

// mapped adapts a byte slice (typically from mmap) to io.ReaderAt.
type mapped []byte

func (m mapped) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(m)) {
		return 0, io.EOF
	}
	n := copy(p, m[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// line reads the record starting at offset up to the next newline.
// SectionReader is used so the read is bounded and does not affect
// any shared file position.
func line(s source, offset int64) ([]byte, error) {
	remaining := s.sz - offset
	if remaining <= 0 {
		return nil, io.EOF
	}

	section := io.NewSectionReader(s, offset, remaining)
	reader := bufio.NewReader(section)
	data, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}

	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	return data, nil
}

// position returns the current seek offset of f.
func position(f *os.File) (int64, error) {
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return pos, nil
}

// align finds the next newline at or after offset, returning its byte
// position. Binary search lands at an arbitrary byte, so align is called
// to advance to the nearest record boundary before reading a pivot.
func align(s source, offset int64) (int64, error) {
	remaining := s.sz - offset
	if remaining <= 0 {
		return -1, nil
	}

	section := io.NewSectionReader(s, offset, remaining)
	reader := bufio.NewReader(section)

	pos := offset
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			return -1, nil
		}
		if err != nil {
			return -1, err
		}
		if b == '\n' {
			return pos, nil
		}
		pos++
	}
}
