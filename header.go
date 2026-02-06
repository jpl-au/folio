// Header management for the database file.
//
// The header is exactly 88 bytes, padded with spaces and terminated with a newline.
// It contains section offsets and state information for crash recovery.
package folio

import (
	"bytes"
	json "github.com/goccy/go-json"
	"os"
)

// HeaderSize is the fixed size of the header in bytes.
const HeaderSize = 128

// Header contains database metadata stored at the start of the file.
type Header struct {
	Version   int   `json:"_v"`   // 1=Legacy, 2=Current (128 bytes)
	Error     int   `json:"_e"`   // 0=clean, 1=dirty (crash indicator)
	Algorithm int   `json:"_alg"` // Hash algorithm (1=xxHash3, 2=FNV1a, 3=Blake2b)
	Timestamp int64 `json:"_ts"`  // Unix milliseconds when written
	History   int64 `json:"_h"`   // Byte offset: end of records / start of history
	Data      int64 `json:"_d"`   // Byte offset: end of data/history section
	Index     int64 `json:"_i"`   // Byte offset: end of index section
}

// header reads and parses the header from a file.
func header(f *os.File) (*Header, error) {
	buf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return nil, err
	}

	var hdr Header
	if err := json.Unmarshal(bytes.TrimSpace(buf), &hdr); err != nil {
		return nil, ErrCorruptHeader
	}
	return &hdr, nil
}

// dirty sets or clears the dirty flag at the fixed offset in the header.
// The _e field is at byte offset 6: {"_e":X
func dirty(w *os.File, v bool) error {
	b := byte('0')
	if v {
		b = '1'
	}
	_, err := w.WriteAt([]byte{b}, 13)
	return err
}

// encode serialises the header to exactly HeaderSize bytes with padding.
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
