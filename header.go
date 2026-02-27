// The header occupies the first 128 bytes of the database file. It stores
// section boundaries, document count, and a dirty flag for crash recovery.
// The fixed size allows the dirty flag to be toggled with a single-byte
// write at a known offset, avoiding a full header rewrite on every mutation.
package folio

import (
	"bytes"
	"os"

	json "github.com/goccy/go-json"
)

// HeaderSize is fixed so the dirty flag can be patched at a known byte
// offset without rewriting the whole header.
const HeaderSize = 128

// State array indices. All mutable integer state lives in a single JSON
// array (_s) so related values are loaded and persisted together.
const (
	stHeap      = 0 // end of heap section (byte offset)
	stIndex     = 1 // end of index section (byte offset)
	stReserved  = 2 // reserved (0)
	stCount     = 3 // best-guess document count; corrected by Compact/Repair
	stWrites    = 4 // writes since last compaction
	stThreshold = 5 // auto-compaction modulus (0 = disabled)
)

// Header describes the file layout. The State array holds section boundaries
// and counters that divide the file into contiguous regions:
//
//	[0..128)                Header (this struct, space-padded, newline-terminated)
//	[128..State[stHeap])    Heap: data + history sorted by ID then timestamp
//	[State[stHeap]..State[stIndex])  Sorted index records
//	[State[stIndex]..EOF)   Sparse region (unsorted appends since last compaction)
//
// Within each ID group in the heap, records are sorted oldest-first.
// History records (_r=3) precede the current data record (_r=2).
// A zero offset means that section is empty or not yet established.
type Header struct {
	Version   int       `json:"_v"`   // Format version: 1 = current
	Error     int       `json:"_e"`   // Dirty flag: 1 = unclean shutdown detected
	Algorithm int       `json:"_alg"` // Hash algorithm used to derive _id from label
	Timestamp int64     `json:"_ts"`  // Unix ms when this header was last written
	State     [6]uint64 `json:"_s"`   // Section boundaries, counts, compaction state
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
	heap, idx := hdr.State[stHeap], hdr.State[stIndex]
	if heap != 0 && heap < HeaderSize {
		return nil, ErrCorruptHeader
	}
	if idx != 0 && idx < HeaderSize {
		return nil, ErrCorruptHeader
	}
	if heap != 0 && idx != 0 && heap > idx {
		return nil, ErrCorruptHeader
	}
	return &hdr, nil
}

// dirty patches the _e field in place without rewriting the full header.
// The value sits at byte 13: {"_v":1,"_e":X — this position is stable
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
