# Folio

*From the Latin folium — a leaf or page of a manuscript.*

A JSONL document store where the file is the interface. One `.folio` file
holds your data as plain text — readable by `grep`, `jq`, any JSONL-capable
tool, or an LLM, without the engine running. The Go library adds binary
search, concurrent access, and automatic versioning on top of the same file.

The format is designed so every access path returns correct results: current
content is plaintext in `_d` and grep-searchable, old versions are compressed
in `_h` and invisible to text search, and record types are filterable by a
single field (`idx`).

```bash
# These work without Go, without a server, without anything
grep '"_d":".*TODO' docs.folio               # search content
grep -o '"_l":"[^"]*"' docs.folio | sort -u  # list documents
jq -r 'select(.idx == 2) | ._d' docs.folio  # extract all content
```

```go
// Or use the Go library for structured access
db, _ := folio.Open("docs.folio", folio.Config{})
db.Set("my-doc", "Hello, World!")
content, _ := db.Get("my-doc")
```

## Install

```bash
go get github.com/jpl-au/folio
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    "github.com/jpl-au/folio"
)

func main() {
    db, err := folio.Open("data/docs.folio", folio.Config{})
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    if err := db.Set("my-doc", "Hello, World!"); err != nil {
        log.Fatal(err)
    }

    content, err := db.Get("my-doc")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(content) // "Hello, World!"

    labels, err := db.List()             // all document labels
    matches, err := db.Search("Hello", folio.SearchOptions{})  // regex on content
    versions, err := db.History("my-doc") // all previous versions
}
```

> Error handling omitted for brevity. All methods return errors that should be checked.

## File Format

Every `.folio` file is valid JSONL. The first line is a fixed-size header;
subsequent lines are records distinguished by the `idx` field:

```
{"_v":2,"_e":0,"_alg":1,"_ts":1706000000000,"_h":0,"_d":0,"_i":0}       <- Header (128 bytes, space-padded)
{"idx":2,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my-doc","_d":"Hello!","_h":"..."} <- Data record
{"idx":3,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my-doc","_d":"","_h":"..."}       <- History record
{"idx":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":128,"_l":"my-doc"}                 <- Index record
```

Current content lives in `_d` and is plaintext — grep-searchable directly.
Previous versions are Zstd-compressed and Ascii85-encoded in the `_h` field,
retrievable through the History API or any language with Zstd and Ascii85
support.

See [USAGE.md](USAGE.md) for command-line examples and
[PORTING.md](PORTING.md) for the full format specification.

## API

### Core Operations

```go
db.Set(label, content string) error          // Create or update
db.Get(label string) (string, error)         // Retrieve content by label
db.Delete(label string) error                // Soft delete (preserves history)
db.Exists(label string) (bool, error)        // Check existence
db.List() ([]string, error)                  // All labels
```

### Search and History

```go
db.Search(pattern string, opts SearchOptions) ([]Match, error)   // Pattern match on content
db.MatchLabel(pattern string) ([]Match, error)                   // Regex on labels
db.History(label string) ([]Version, error)                      // All versions
```

Search uses a literal fast path for patterns without regex metacharacters:
the query is JSON-escaped and matched with `bytes.Contains` against the raw
file content, avoiding both regex overhead and per-record JSON unescaping.
Patterns containing regex metacharacters (`.*+?()[]{}|\^$`) fall back to
`regexp.Match`. The fast path is transparent — callers don't need to know
which path runs.

### Maintenance

```go
db.Compact() error                        // Sort and reclaim space, keep history
db.Purge() error                          // Sort and reclaim space, remove all history
db.Rehash(alg) error                      // Migrate to a different hash algorithm
db.Repair(opts *CompactOptions) error     // Rebuild from a corrupted file
```

## Configuration

```go
db, err := folio.Open("data/docs.folio", folio.Config{
    HashAlgorithm: folio.AlgXXHash3,  // default; also AlgFNV1a, AlgBlake2b
    ReadBuffer:    64 * 1024,         // scanner buffer size (default 64KB)
    MaxRecordSize: 16 * 1024 * 1024,  // largest record allowed (default 16MB)
    SyncWrites:    false,             // fsync after every write
    BloomFilter:   true,              // in-memory filter for sparse region
})
```

### Bloom Filter

By default, folio scans the sparse region linearly for every lookup that
misses the sorted index. Enabling `BloomFilter` builds a small (~12KB)
in-memory filter at Open that tracks which IDs exist in the sparse region.
Lookups for absent documents skip the linear scan entirely.

## Documentation

- [AGENTS.md](AGENTS.md) - Quick orientation for LLM agents and tool integrations
- [USAGE.md](USAGE.md) - Command-line usage and grep examples
- [PORTING.md](PORTING.md) - Format specification and implementation guide

## License

MIT License - see [LICENSE](LICENSE)
