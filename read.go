// Low-level read operations for file access.
//
// These primitives handle newline-delimited record reading and position tracking.
// All operations use io.SectionReader for bounded, concurrent-safe access.
package folio

import (
	"bufio"
	"io"
	"os"
)

// line reads a complete record from offset until newline.
// Returns bytes excluding the trailing newline character.
func line(f *os.File, offset int64) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	remaining := info.Size() - offset
	if remaining <= 0 {
		return nil, io.EOF
	}

	section := io.NewSectionReader(f, offset, remaining)
	reader := bufio.NewReader(section)
	data, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}

	// Strip trailing newline if present
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	return data, nil
}

// align finds the position of the next newline at or after offset.
// Returns the byte position of the newline, or -1 if none found.
func align(f *os.File, offset int64) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return -1, err
	}

	remaining := info.Size() - offset
	if remaining <= 0 {
		return -1, nil
	}

	section := io.NewSectionReader(f, offset, remaining)
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

// size returns the file size in bytes.
func size(f *os.File) int64 {
	info, _ := f.Stat()
	return info.Size()
}

// position returns the current file position.
func position(f *os.File) int64 {
	pos, _ := f.Seek(0, io.SeekCurrent)
	return pos
}
