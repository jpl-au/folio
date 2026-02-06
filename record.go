// Record types for data storage.
//
// Three record types exist: Index (lookup), Record (current data), and History
// (previous versions). All are single-line JSON with the idx field first for
// efficient type detection.
package folio

import (
	"bytes"
	json "github.com/goccy/go-json"
	"encoding/hex"
	"strings"
	"time"
	"unicode/utf8"
)

// Record type markers.
const (
	TypeIndex   = 1 // Index record
	TypeRecord  = 2 // Current data record
	TypeHistory = 3 // Historical data record
)

// MaxLabelSize is the maximum length of a document label in bytes.
const MaxLabelSize = 256

// MaxRecordSize is the maximum length of a single record in bytes (16MB).
const MaxRecordSize = 16 * 1024 * 1024

// Record represents a data record (current or historical).
type Record struct {
	Type      int    `json:"idx"` // TypeRecord or TypeHistory
	ID        string `json:"_id"` // 16 hex chars
	Timestamp int64  `json:"_ts"` // Unix milliseconds
	Label     string `json:"_l"`  // User-provided name
	Data      string `json:"_d"`  // Document content (text/markdown)
	History   string `json:"_h"`  // Compressed snapshot
}

// Index represents an index record pointing to a data record.
type Index struct {
	Type      int    `json:"idx"` // TypeIndex
	ID        string `json:"_id"` // 16 hex chars
	Timestamp int64  `json:"_ts"` // Unix milliseconds
	Offset    int64  `json:"_o"`  // Byte position of data record
	Label     string `json:"_l"`  // User-provided name
}

// Result contains position and content from scan operations.
type Result struct {
	Offset int64  // Byte position in file
	Length int    // Record length (bytes)
	Data   []byte // Raw record content
	ID     string // Record ID (16 hex chars)
}

// Entry contains metadata extracted during minimal scan for compaction.
type Entry struct {
	ID     string // 16 hex chars
	TS     int64  // Timestamp
	Type   int    // idx value
	SrcOff int64  // Source byte position
	DstOff int64  // Destination position (filled during write)
	Length int    // Record length
	Label  string // For index entries only
}

// MinRecordSize is the minimum valid record length.
// Format: {"idx":N,"_id":"XXXXXXXXXXXXXXXX","_ts":NNNNNNNNNNNNN
const MinRecordSize = 53

// decode parses raw bytes into a Record. Works for all record types.
func decode(data []byte) (*Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, ErrCorruptRecord
	}
	return &r, nil
}

// decodeIndex parses raw bytes into an Index record.
func decodeIndex(data []byte) (*Index, error) {
	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, ErrCorruptIndex
	}
	return &idx, nil
}

// valid checks if a line represents a valid record (starts with '{').
func valid(line []byte) bool {
	return len(line) > 0 && line[0] == '{'
}

// label extracts the _l value from a record line without full parsing.
func label(line []byte) string {
	s := string(line)
	start := strings.Index(s, `"_l":"`)
	if start == -1 {
		return ""
	}
	start += 6
	end := strings.Index(s[start:], `"`)
	if end == -1 {
		return ""
	}
	return s[start : start+end]
}

// now returns the current time in unix milliseconds.
func now() int64 {
	return time.Now().UnixMilli()
}

// unescape decodes JSON string escape sequences in-place.
// Handles: \" \\ \/ \n \r \t \b \f \uXXXX.
// Returns input unchanged if no backslash is present.
func unescape(b []byte) []byte {
	if bytes.IndexByte(b, '\\') < 0 {
		return b
	}

	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			out = append(out, b[i])
			continue
		}
		i++
		switch b[i] {
		case '"':
			out = append(out, '"')
		case '\\':
			out = append(out, '\\')
		case '/':
			out = append(out, '/')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'u':
			if i+4 >= len(b) {
				out = append(out, '\\', 'u')
				continue
			}
			h, err := hex.DecodeString(string(b[i+1 : i+5]))
			if err != nil {
				out = append(out, '\\', 'u')
				continue
			}
			r := rune(h[0])<<8 | rune(h[1])
			var buf [4]byte
			n := utf8.EncodeRune(buf[:], r)
			out = append(out, buf[:n]...)
			i += 4
		default:
			out = append(out, '\\', b[i])
		}
	}
	return out
}
