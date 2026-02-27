# Porting Guide

This document describes the folio file format and the mechanics behind it.
Everything here is language-agnostic — the reference implementation is Go,
but a correct implementation can be written in any language with file I/O,
JSON, Zstd, and Ascii85 support.

## Design Principle

The file is the interface. A `.folio` file is valid JSONL. Any tool that
can read lines of JSON — grep, jq, Python, a shell script, an LLM — can
query the data without the engine. The engine adds performance (binary
search, bloom filters) and safety (locking, crash recovery), but the file
is always self-describing and independently readable.

Current content is stored as plaintext so text search works directly.
Previous versions are compressed so they don't pollute search results.

## File Layout

```
[Header]        128 bytes, line 1
[Heap]          Data + history records, sorted by ID then timestamp
[Index]         Index records, sorted by ID
[Sparse]        Unsorted appends since last compaction
```

After a fresh `Open` with no compaction, the heap and index sections are
empty — everything is sparse. After `Compact`, records are sorted into the
heap and index, and the sparse section is empty. Normal operation appends
to sparse; compaction periodically reorganises.

The header stores byte offsets marking the boundaries in the `_s` state
array:

- `_s[0]`: end of heap (= start of index)
- `_s[1]`: end of index (= start of sparse)

When both are 0, no compaction has occurred and the entire file after the
header is sparse.

## Header

The first line is a JSON object, space-padded to exactly 127 bytes, followed
by a newline (128 bytes total).

```json
{"_v":1,"_e":0,"_alg":1,"_ts":1706000000000,"_s":[0,0,0,0,0,0]}
```

| Field  | Type   | Description |
|--------|--------|-------------|
| `_v`   | int    | Format version (currently 1) |
| `_e`   | int    | Dirty flag: 0 = clean, 1 = unclean shutdown |
| `_alg` | int    | Hash algorithm: 1 = xxHash3, 2 = FNV-1a, 3 = Blake2b |
| `_ts`  | int    | Unix milliseconds, last header write |
| `_s`   | [6]uint | State array (see below) |

The `_s` array holds all mutable unsigned integer state:

| Index | Description |
|-------|-------------|
| 0     | Byte offset: end of heap section |
| 1     | Byte offset: end of index section |
| 2     | Reserved (0) |
| 3     | Document count (best-guess, corrected by compaction) |
| 4     | Writes since last compaction |
| 5     | Auto-compaction threshold (modulus, 0 = disabled) |

The dirty flag (`_e`) sits at a known byte position (offset 13 in the line)
so it can be toggled with a single-byte write rather than rewriting the
entire header.

## Records

Every record is a single JSON line. There are three types, distinguished by
`_r`:

### Data Record (_r=2)

The current content of a document.

```json
{"_r":2,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my-doc","_d":"Hello!","_h":"<compressed>"}
```

| Field | Description |
|-------|-------------|
| `_r` | Always 2 |
| `_id` | 16 hex characters, hash of the label |
| `_ts` | Unix milliseconds, write time |
| `_l`  | Document label (user-facing name, max 256 bytes) |
| `_d`  | Current content, plaintext |
| `_h`  | Zstd-compressed, Ascii85-encoded snapshot of the content |

### History Record (_r=3)

A previous version. Created when a document is updated: the old data record
is patched in place — type byte changed from `2` to `3`, `_d` field
overwritten with spaces (preserving byte offsets), `_h` field left intact.

```json
{"_r":3,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my-doc","_d":"      ","_h":"<compressed>"}
```

The blanked `_d` field is intentional: grep won't match old content, but the
compressed version in `_h` is fully recoverable.

### Index Record (_r=1)

A pointer from a label's hash ID to the byte offset of its current data
record.

```json
{"_r":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":128,"_l":"my-doc"}
```

| Field | Description |
|-------|-------------|
| `_o`  | Byte offset of the data record this index points to |

## Fixed Byte Positions

Field order in the JSON is fixed. This allows metadata extraction without
parsing:

| Offset | Length | Content |
|--------|--------|---------|
| 6      | 1      | Record type: `1`, `2`, or `3` |
| 15     | 16     | ID (hex string) |
| 39     | 13     | Timestamp (digits) |

This is critical for performance: binary search and compaction read type, ID,
and timestamp from raw bytes without deserialising the full JSON. An
implementation that doesn't need this optimisation can parse the JSON
normally — the data is the same either way.

## ID Generation

Labels are hashed to a 16-character lowercase hex string. The algorithm is
stored in the header (`_alg`) so all records in a file use the same one:

| Value | Algorithm | Notes |
|-------|-----------|-------|
| 1     | xxHash3 (64-bit) | Default, fastest |
| 2     | FNV-1a (64-bit)  | No external dependencies |
| 3     | Blake2b (256-bit, truncated to 64-bit) | Cryptographic quality |

The hash is formatted as `%016x` (16 hex digits, zero-padded, lowercase).

xxHash3 is the default because it has the best throughput for short strings
(document labels) and excellent distribution. FNV-1a is a stdlib-only
fallback for environments that cannot use external dependencies. Blake2b
offers cryptographic-quality distribution to minimise collision probability
at the cost of ~10x slower hashing — relevant only for very large databases
where birthday-bound collisions on 64-bit hashes become a concern.

A port only needs to support one algorithm to read and write files. To
interoperate with files created by other implementations, support all three.

## Lookup Strategy

A `Get(label)` proceeds in two phases:

1. **Sorted lookup** (if heap/index exist): binary search the index section
   for the label's hash ID. If found, read the data record at the byte
   offset in `_o`. Verify the label matches (hash collisions are possible).

2. **Sparse lookup**: linear scan from sparse start to EOF, collecting all
   index records. Return the latest (by timestamp) matching the target ID.
   If the latest record is a delete (data record with empty `_d`), return
   not found.

Sparse overrides sorted — a write after compaction takes precedence over
the compacted data.

### Binary Search

Binary search operates on byte ranges, not line numbers:

1. Compute the midpoint byte offset between `lo` and `hi`.
2. Walk forward to the next newline to find a record boundary.
3. Extract the ID from bytes 15..30 of that line.
4. Compare with the target ID to narrow `lo`/`hi`.

If walking forward overshoots `hi`, walk backward from the midpoint instead.

### Bloom Filter (optional)

An in-memory bloom filter over sparse-region IDs can short-circuit negative
lookups. If the filter says an ID is absent from the sparse region, skip the
linear scan entirely.

Parameters used by the reference implementation:

- Size: 11,982 bytes (~96k bits)
- Hash functions: 7
- Tuned for ~10k entries at ~1% false positive rate
- Double hashing: `h1 = FNV-64a(id)`, `h2 = FNV-32a(id)`,
  `pos[i] = (h1 + i * h2) % bits`

FNV is used for bloom hashing rather than the ID hash algorithm (xxHash3)
to keep the hash functions independent. If the same algorithm generated
both IDs and bloom positions, correlated collisions would produce
correlated false positives, degrading the filter's effectiveness.

The bloom filter is rebuilt from scratch at open and after each compaction
(which empties the sparse region). It is purely a performance optimisation
and can be omitted in a port without affecting correctness.

## Write Path

To write a document:

1. Acquire an exclusive lock.
2. Set the dirty flag (`_e` = 1) via a single-byte write at offset 13.
3. If the label already exists, patch the old data record in place:
   - Overwrite byte 6 from `2` to `3` (data becomes history).
   - Overwrite the `_d` field value with spaces (preserving byte length).
4. Append a new data record and a new index record to EOF (the sparse
   region).
5. Update the in-memory tail offset.
6. If `SyncWrites` is enabled, fsync.
7. Release the lock.

The dirty flag is cleared on `Close` after a final fsync.

### Delete

Delete works the same as update but writes a data record with an empty `_d`
field and no index record. Lookups that find this record treat it as not
found.

## Compaction

Compaction reorganises the file for faster reads. It runs in two phases to
minimise lock contention:

**Phase 1** (shared or read lock):
1. Scan the entire file, extracting metadata from fixed byte positions.
2. Group records by ID, sort by timestamp within each group.
3. Write to a temporary file (`.tmp` suffix):
   - Header with updated section offsets.
   - Heap: for each ID, history records (oldest first) then current data.
   - Index: one index record per live document, pointing to its heap offset.
4. No sparse section (it's empty after compaction).

**Phase 2** (exclusive lock, brief):
1. Close file handles on the old file.
2. Atomically rename `.tmp` to the main file.
3. Reopen file handles.

### Purge

Same as compaction but drops all history records. Only the current data
record for each label is kept.

## Crash Recovery

On `Open`, if the dirty flag is set or a `.tmp` file exists, the previous
session did not shut down cleanly. Recovery:

1. Delete the `.tmp` file if present (it's an incomplete compaction).
2. Run `Repair` under an exclusive lock — this is a full compaction that
   rebuilds the file from surviving records.
3. Incomplete lines (no trailing newline) are silently discarded.

Because every record is a complete JSON line terminated by a newline, a crash
mid-write at worst loses the partially written record. All previously
committed records remain intact.

## Locking

The reference implementation uses three layers:

1. **State machine** (in-memory): gates whether operations are allowed.
2. **Read-write mutex** (in-process): coordinates goroutines.
3. **OS file lock** (cross-process): `flock` on Unix, `LockFileEx` on
   Windows. Shared for reads, exclusive for writes.

A minimal port needs only the OS file lock for correctness. The state
machine and mutex are optimisations for concurrent in-process access.

## Rehash

`Rehash(newAlgorithm)` migrates all record IDs to a different hash
algorithm. Since IDs occupy a fixed position (bytes 15..30), this can
overwrite them in place without rewriting the full record. After patching
all IDs, the header's `_alg` field is updated and the file is fsynced.

A `Compact` should be run after `Rehash` to re-sort the heap and index
sections by the new IDs, restoring binary search correctness.

**Crash safety**: Rehash is not crash-safe. If the process dies mid-rehash,
the file contains a mix of old and new algorithm IDs while the header may
still reference the old algorithm. The dirty flag is not set during rehash
(it is managed by the normal write path), so automatic crash recovery will
not trigger. Recovery requires a manual `Repair` call. This is acceptable
because rehash is a rare, operator-initiated maintenance operation. A port
should document this limitation or implement its own crash guard (e.g. set
the dirty flag before patching, clear after the header update).

## Search

Search scans data records (`_r=2`) and matches the pattern against the
`_d` field. The scan is streaming: records are read line-by-line with a
bounded buffer, and the `_d` content is extracted by byte-scanning for
field delimiters rather than JSON-parsing each line. Memory stays
proportional to the buffer size, not the largest record.

The reference implementation uses two match strategies:

- **Literal fast path**: when the pattern contains no regex metacharacters,
  it is JSON-escaped (via `json.Marshal`, stripping surrounding quotes) and
  matched against the raw `_d` bytes with `bytes.Contains`. This avoids
  both regex overhead and per-record JSON unescaping. The JSON-escaped
  needle matches the on-disk encoding directly.

- **Regex fallback**: patterns with metacharacters are compiled to a regex
  and matched per record. An optional `Decode` flag unescapes JSON string
  escapes in `_d` before matching, handling non-standard encodings like
  `\u0041` for `A`. When `Decode` is set, the literal fast path is
  bypassed to guarantee equivalent results.

A port may use either strategy or a simpler approach (e.g. parse JSON,
extract `_d`, match). The literal fast path is an optimisation, not a
format requirement.

## Constraints

| Constraint | Value |
|------------|-------|
| Header size | 128 bytes (fixed) |
| Max label size | 256 bytes |
| Default max record size | 16 MB |
| ID length | 16 hex characters |
| Timestamp | Unix milliseconds (13 digits) |
| Min record size | 52 bytes |

## Read-Only Access

A read-only implementation is straightforward:

1. Read the 128-byte header.
2. To list documents: scan for `_r=1` records, collect `_l` fields.
3. To get a document: scan for `_r=2` records matching the label,
   take the latest by `_ts`.
4. To get history: scan for `_r=2` and `_r=3` records matching the
   label's ID, decompress `_h` fields (Zstd, then Ascii85-decode).

No locking, no binary search, no bloom filter needed. Linear scan of the
full file is correct, just slower for large files.

## Minimal Port Checklist

A working read-write implementation needs:

- [ ] JSON line reading and writing with fixed field order
- [ ] Header parsing and dirty flag toggling (byte 13)
- [ ] Hash function (at least one of xxHash3, FNV-1a, Blake2b)
- [ ] Zstd compression and Ascii85 encoding for the `_h` field
- [ ] Append to EOF with newline termination
- [ ] In-place byte patching (type byte, `_d` blanking)
- [ ] OS file locking (flock or equivalent)
- [ ] Compaction: scan, sort, rewrite to tmp, atomic rename
- [ ] Crash recovery: detect dirty flag / tmp file, repair on open

Optional but recommended:

- [ ] Binary search on sorted sections
- [ ] Bloom filter for sparse region
- [ ] Read-write mutex for in-process concurrency
