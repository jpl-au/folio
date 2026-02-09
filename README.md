# Folio

A JSONL-based document database for Go with automatic version history.

## Features

- **JSONL format**: plain text, grep-searchable, LLM-friendly
- **Automatic versioning**: every update preserves a compressed snapshot
- **Disk-first**: minimal memory footprint, scans file directly
- **Thread-safe**: safe for concurrent use with layered locking
- **Single file**: no external dependencies, no server

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
    db, err := folio.Open("data", "docs.folio", folio.Config{})
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create or update a document
    if err := db.Set("my-doc", "Hello, World!"); err != nil {
        log.Fatal(err)
    }

    // Read it back
    content, err := db.Get("my-doc")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(content) // "Hello, World!"

    // List all labels
    labels, err := db.List()

    // Search content with regex
    matches, err := db.Search("Hello", folio.SearchOptions{})

    // Search labels with regex
    labelMatches, err := db.MatchLabel("my-.*")

    // Get version history
    versions, err := db.History("my-doc")
}
```

## File Format

Folio stores data as JSONL (one JSON object per line). The first line is a
fixed-size header; subsequent lines are records. Three record types are
distinguished by the `idx` field:

```
{"_v":2,"_e":0,"_alg":1,"_ts":1706000000000,"_h":0,"_d":0,"_i":0}       <- Header (128 bytes, space-padded)
{"idx":2,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my-doc","_d":"Hello!","_h":"..."} <- Data record
{"idx":3,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_l":"my-doc","_d":"","_h":"..."}       <- History record
{"idx":1,"_id":"a1b2c3d4e5f6g7h8","_ts":1706000000000,"_o":128,"_l":"my-doc"}                 <- Index record
```

Current content lives in `_d` and is plaintext — grep-searchable directly.
Previous versions are Zstd-compressed and Ascii85-encoded in the `_h` field,
retrievable through the History API.

```bash
# Search current content
grep '"_d":".*TODO' docs.folio

# List document labels
grep -o '"_l":"[^"]*"' docs.folio | sort -u
```

See [USAGE.md](USAGE.md) for more command-line examples.

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
db.Search(pattern string, opts SearchOptions) ([]Match, error)   // Regex on content
db.MatchLabel(pattern string) ([]Match, error)                   // Regex on labels
db.History(label string) ([]Version, error)                      // All versions
```

### Maintenance

```go
db.Compact() error    // Sort and reclaim space, keep history
db.Purge() error      // Sort and reclaim space, remove all history
db.Rehash(alg) error  // Migrate to a different hash algorithm
```

## Configuration

```go
db, err := folio.Open("data", "docs.folio", folio.Config{
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

- [USAGE.md](USAGE.md) - Command-line usage and grep examples

## License

MIT License - see [LICENSE](LICENSE)
