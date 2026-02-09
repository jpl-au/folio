// Record format and type definitions.
//
// Every line in the database file is a JSON object beginning with {"idx":N
// where N identifies the record type. This fixed prefix allows type detection
// and ID extraction at known byte offsets without JSON parsing — critical for
// binary search and compaction where millions of records may be scanned.
//
// Three types coexist in the file:
//   - Index (idx=1): maps a label's hash to the byte offset of its data record.
//   - Record (idx=2): the current content of a document.
//   - History (idx=3): a previous version with compressed content in _h.
//
// On update, the old Record is retyped to History (byte patch from 2→3) and
// its _d field is blanked. This preserves the compressed snapshot in _h for
// version retrieval while making the record invisible to data scans.
package folio

import (
	"bytes"
	json "github.com/goccy/go-json"
	"encoding/hex"
	"strings"
	"time"
	"unicode/utf8"
)

// Record type markers. These appear as the first value in every JSON line
// ({"idx":N) and are used for byte-level type checks during scan.
const (
	TypeIndex   = 1
	TypeRecord  = 2
	TypeHistory = 3
)

const MaxLabelSize = 256              // bytes
const MaxRecordSize = 16 * 1024 * 1024 // 16MB, bounds scanner buffer allocation

// Record is a data or history line. When a document is updated, the old
// Record has its Type patched from 2→3 (becoming history) and _d blanked,
// so the compressed _h snapshot is the only way to recover prior content.
type Record struct {
	Type      int    `json:"idx"`
	ID        string `json:"_id"` // 16 hex chars, hash of Label
	Timestamp int64  `json:"_ts"` // unix ms
	Label     string `json:"_l"`
	Data      string `json:"_d"` // current content (blank for history)
	History   string `json:"_h"` // zstd+ascii85 compressed snapshot
}

// Index maps a label's hashed ID to the byte offset of its data Record.
// During lookup, the index is found first (by binary or sparse scan on ID),
// then the data record is read at the offset the index points to.
type Index struct {
	Type      int    `json:"idx"`
	ID        string `json:"_id"`
	Timestamp int64  `json:"_ts"`
	Offset    int64  `json:"_o"` // byte position of the corresponding Record
	Label     string `json:"_l"`
}

// Result carries a record's position and raw bytes from a scan. Callers
// use Offset to read or overwrite the record and Data for parsing.
type Result struct {
	Offset int64
	Length int
	Data   []byte
	ID     string
}

// Entry holds lightweight metadata extracted by scanm. Full JSON parsing
// is skipped — fields are read at fixed byte positions. DstOff is zero
// until compaction fills it with the record's new position in the output.
type Entry struct {
	ID     string
	TS     int64
	Type   int
	SrcOff int64  // position in the source file
	DstOff int64  // position in the compaction output (set during write)
	Length int
	Label  string // populated only for index entries
}

// MinRecordSize is the shortest valid JSON line. Anything shorter cannot
// contain the required idx, _id, and _ts fields and is skipped during scan.
const MinRecordSize = 53

// decode performs full JSON parsing of a record line.
func decode(data []byte) (*Record, error) {
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, ErrCorruptRecord
	}
	return &r, nil
}

// decodeIndex performs full JSON parsing of an index line.
func decodeIndex(data []byte) (*Index, error) {
	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, ErrCorruptIndex
	}
	return &idx, nil
}

// valid is a fast pre-check: blanked records and the header start with
// spaces, so only lines starting with '{' can be JSON records.
func valid(line []byte) bool {
	return len(line) > 0 && line[0] == '{'
}

// label extracts the _l value by string scanning, avoiding a full JSON
// parse. Used in hot paths (compaction, search) where only the label is
// needed and the record may be megabytes of content.
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

func now() int64 {
	return time.Now().UnixMilli()
}

// unescape resolves JSON string escapes so that regex search operates on
// the actual content rather than the escaped representation. Returns the
// input unchanged if no backslash is present (common case, zero allocation).
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
