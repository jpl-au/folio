// Low-level read primitives for the newline-delimited record format.
//
// Every record is a single JSON line terminated by '\n'. These functions
// read individual lines, find record boundaries, and query file size —
// all via SectionReader or ReadAt so that concurrent readers sharing a
// single *os.File do not interfere with each other's offsets.
package folio

import (
	"bufio"
	"io"
	"os"
)

// line reads the record starting at offset up to the next newline.
// SectionReader is used so the read is bounded by file size and does not
// affect the shared file position.
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

	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	return data, nil
}

// align finds the next newline at or after offset, returning its byte
// position. Binary search lands at an arbitrary byte, so align is called
// to advance to the nearest record boundary before reading a pivot.
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

func size(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func position(f *os.File) (int64, error) {
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return pos, nil
}
