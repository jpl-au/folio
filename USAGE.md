# Working with Folio Files

Folio stores data as JSONL (JSON Lines), making files accessible to standard Unix tools and LLMs without any special tooling.

## File Structure

A folio file contains a header and three record types:

```
{"_v":1,"_e":0,"_alg":1,"_ts":...,"_s":[0,0,0,0,0,0]}                            Header (line 1, 128 bytes)
{"_r":2,"_id":"a1b2...","_ts":...,"_l":"my-doc","_d":"content...","_h":"..."}     Data record (current)
{"_r":3,"_id":"a1b2...","_ts":...,"_l":"my-doc","_d":"","_h":"..."}               History record (previous version)
{"_r":1,"_id":"a1b2...","_ts":...,"_o":128,"_l":"my-doc"}                         Index record (pointer)
```

- **Header**: file metadata and section boundary offsets (fixed 128 bytes, space-padded)
- **Data records** (`_r=2`): current document content in `_d`, compressed snapshot in `_h`
- **History records** (`_r=3`): previous versions — `_d` is blanked, `_h` holds the compressed content
- **Index records** (`_r=1`): point to the byte offset (`_o`) of the current data record

## What's Grep-Searchable

| Content | Searchable | Field |
|---------|------------|-------|
| Current document content | Yes | `_d` |
| Document labels | Yes | `_l` |
| Document IDs (hashed) | Yes | `_id` |
| Timestamps | Yes | `_ts` |
| Previous versions | No | `_h` (Zstd-compressed, Ascii85-encoded) |

Historical content is compressed in the `_h` field. This is intentional — casual grep won't accidentally surface old versions, but the History API retrieves them.

## Common Commands

### Search content

```bash
# Find documents containing "TODO"
grep '"_d":".*TODO' docs.folio

# Case-insensitive search
grep -i "error" docs.folio

# Search with context (show surrounding lines)
grep -C2 "authentication" docs.folio
```

### List documents

```bash
# List all document labels
grep -o '"_l":"[^"]*"' docs.folio | sort -u

# Just the labels without JSON wrapper
grep -o '"_l":"[^"]*"' docs.folio | sed 's/"_l":"//;s/"//' | sort -u

# Count unique documents (via index records)
grep '"_r":1' docs.folio | wc -l
```

### Inspect structure

```bash
# Count total lines (header + data + index records)
wc -l docs.folio

# Show the header
head -1 docs.folio | python3 -m json.tool

# Show all index records (current document pointers)
grep '"_r":1' docs.folio

# Show all current data records
grep '"_r":2' docs.folio

# Pretty-print a specific line with jq
sed -n '5p' docs.folio | jq .
```

### Extract content

```bash
# Extract content from all current data records
grep '"_r":2' docs.folio | jq -r '._d'

# Get a specific document's content by label
grep '"_r":2' docs.folio | grep '"_l":"my-doc"' | jq -r '._d'
```

## Using with jq

The [jq](https://jqlang.github.io/jq/) tool is useful for structured queries:

```bash
# List all document labels from index records
jq -r 'select(._r == 1) | ._l' docs.folio | sort -u

# Get timestamps for a document
jq -r 'select(._l == "my-doc") | ._ts' docs.folio

# Find large documents (content > 1000 chars)
jq -r 'select(._r == 2 and (._d | length) > 1000) | ._l' docs.folio
```

## Using with LLMs

Folio's JSONL format works well for LLM context:

```bash
# Feed current content to an LLM
grep '"_r":2' docs.folio | jq -r '"## " + ._l + "\n" + ._d' > context.md

# Extract just labels and content (no metadata)
jq -r 'select(._r == 2) | {label: ._l, content: ._d}' docs.folio
```

The key benefit: current content is plaintext and immediately useful, while historical versions stay compressed and don't clutter context windows.

## Limitations

- **History requires the API**: the `_h` field is Zstd-compressed and Ascii85-encoded. Use `db.History()` to retrieve previous versions programmatically.
- **Blanked content**: when a document is updated, the previous record's `_d` field is overwritten with spaces (preserving file offsets). These history records will show `"_d":"    ..."` with whitespace.
- **Index records**: `_r=1` records are internal pointers used for lookups. For document listing, filter by `_r=1` and read `_l`, or use `_r=2` for content.

## Quick Reference

| Task | Command |
|------|---------|
| Search content | `grep '"_d":".*pattern' docs.folio` |
| List labels | `grep -o '"_l":"[^"]*"' docs.folio \| sort -u` |
| Count docs | `grep -c '"_r":1' docs.folio` |
| View header | `head -1 docs.folio` |
| Pretty-print | `jq . docs.folio` |
| Current content only | `grep '"_r":2' docs.folio` |
