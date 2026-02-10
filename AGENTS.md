# Folio — Agent Quick Start

Folio is a JSONL document store where the file is the interface. A single
`.folio` file holds labelled documents as plain text — readable by `grep`,
`jq`, any JSONL tool, or directly in an LLM context window, without the
engine running. The Go library adds binary search, concurrent access, and
automatic versioning on top of the same file.

## Which doc do you need?

| Goal | Read |
|------|------|
| Read or query data from a `.folio` file without writing code | [USAGE.md](USAGE.md) |
| Build an application with the Go library | [README.md](README.md) |
| Understand the format or port to another language | [PORTING.md](PORTING.md) |

## Format at a glance

Every `.folio` file is valid JSONL. Line 1 is a fixed-size header. Every
subsequent line is one of three record types, distinguished by the `idx`
field:

| `idx` | Type | Purpose |
|-------|------|---------|
| 1 | Index | Pointer from a label's hash ID to the byte offset of its data record |
| 2 | Data | Current content — `_d` holds the plaintext, `_l` holds the label |
| 3 | History | Previous version — `_d` is blanked, `_h` holds compressed content |

### Key fields

- `_d` — current document content (plaintext, grep-searchable)
- `_l` — document label (the user-facing name)
- `_id` — 16 hex characters, hash of the label
- `_ts` — Unix milliseconds, write time
- `_h` — Zstd-compressed, Ascii85-encoded snapshot (not grep-searchable)

### What's searchable

Current content in `_d` and labels in `_l` are plaintext and searchable
with standard tools. Historical content in `_h` is compressed — invisible
to grep, recoverable through the Go API or any Zstd/Ascii85 decoder.

## Safe read patterns

```bash
# Search document content
grep '"_d":".*pattern' docs.folio

# List all document labels
grep -o '"_l":"[^"]*"' docs.folio | sort -u

# Extract current content (all documents)
jq -r 'select(.idx == 2) | ._d' docs.folio

# Get a specific document by label
grep '"idx":2' docs.folio | grep '"_l":"my-doc"' | jq -r '._d'

# Count documents
grep -c '"idx":1' docs.folio
```

See [USAGE.md](USAGE.md) for the full catalogue.

## Writing

Writes require either the Go library or a correct implementation of the
format — see [PORTING.md](PORTING.md) for the full spec. Do not modify the
file by hand: the format relies on fixed byte positions, in-place patching,
file locking, and a dirty-flag protocol that hand edits will violate.
