// The header occupies the first 128 bytes of the database file. It stores
// byte offsets that divide the file into sections (heap, indexes, sparse)
// and a dirty flag for crash recovery. The fixed size allows the dirty flag
// to be toggled with a single-byte write at a known offset, avoiding a full
// header rewrite on every mutation.
package folio

import (
	"bytes"
	json "github.com/goccy/go-json"
	"os"
)

// HeaderSize is fixed so the dirty flag can be patched at a known byte
// offset without rewriting the whole header.
const HeaderSize = 128

// Header describes the file layout. The offset fields divide the file into
// contiguous sections that are read independently:
//
//	[0..128)        Header (this struct, space-padded, newline-terminated)
//	[128..Heap)     Heap: data + history sorted by ID then timestamp
//	[Heap..Index)   Sorted index records (point to current data in the heap)
//	[Index..EOF)    Sparse region (unsorted appends since last compaction)
//
// Within each ID group in the heap, records are sorted oldest-first.
// History records (idx=3) precede the current data record (idx=2).
// A zero offset means that section is empty or not yet established.
type Header struct {
	Version   int   `json:"_v"`   // Format version: 2 = current 128-byte header
	Error     int   `json:"_e"`   // Dirty flag: 1 = unclean shutdown detected
	Algorithm int   `json:"_alg"` // Hash algorithm used to derive _id from label
	Timestamp int64 `json:"_ts"`  // Unix ms when this header was last written
	Heap      int64 `json:"_h"`   // Byte offset where index section begins (end of heap)
	Index     int64 `json:"_i"`   // Byte offset where sparse region begins
}

// header parses the fixed-size header from byte 0 of the file.
func header(f *os.File) (*Header, error) {
	buf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return nil, err
	}

	var hdr Header
	if err := json.Unmarshal(bytes.TrimSpace(buf), &hdr); err != nil {
		return nil, ErrCorruptHeader
	}
	if hdr.Heap != 0 && hdr.Heap < HeaderSize {
		return nil, ErrCorruptHeader
	}
	if hdr.Index != 0 && hdr.Index < HeaderSize {
		return nil, ErrCorruptHeader
	}
	if hdr.Heap != 0 && hdr.Index != 0 && hdr.Heap > hdr.Index {
		return nil, ErrCorruptHeader
	}
	return &hdr, nil
}

// dirty patches the _e field in place without rewriting the full header.
// The value sits at byte 13: {"_v":2,"_e":X — this position is stable
// because _v and _e are always serialised first and _v is single-digit.
func dirty(w *os.File, v bool) error {
	b := byte('0')
	if v {
		b = '1'
	}
	_, err := w.WriteAt([]byte{b}, 13)
	return err
}

// encode serialises the header to exactly HeaderSize bytes, space-padded
// with a trailing newline. The fixed size ensures the first data record
// always starts at the same offset regardless of header content.
func (h *Header) encode() ([]byte, error) {
	data, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}

	// Pad with spaces to HeaderSize-1, then add newline
	padLen := HeaderSize - len(data) - 1
	if padLen < 0 {
		return nil, ErrCorruptHeader // header too large
	}

	buf := make([]byte, HeaderSize)
	copy(buf, data)
	for i := len(data); i < HeaderSize-1; i++ {
		buf[i] = ' '
	}
	buf[HeaderSize-1] = '\n'

	return buf, nil
}
