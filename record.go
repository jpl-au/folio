// Record types for data storage.
//
// Three record types exist: Index (lookup), Record (current data), and History
// (previous versions). All are single-line JSON with the idx field first for
// efficient type detection.
package folio

import (
	"encoding/json"
	"strings"
	"time"
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
