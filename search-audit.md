# Search Audit & Future Strategy

## Current State

Search uses a linear scan across the heap and sparse regions.

- **Fast path:** Literal string matching using `bytes.Contains` on raw
  JSON-escaped bytes - no regex compilation, no per-record unescaping.
- **Regex fallback:** Patterns with metacharacters use `regexp.Match`.
  Optional `Decode` mode unescapes `_d` before matching.
- **Label match:** `MatchLabel` is a separate method that scans index
  records with a regex. No literal fast path, no case sensitivity control.
- **Streaming:** Results are yielded lazily via `iter.Seq2`. Break from
  the range loop to stop early without scanning the rest of the file.
- **Region awareness:** Search skips the index section (no `_d` fields).
  MatchLabel skips the heap (no `_l` fields worth matching).

### Problems

1. **Two APIs for the same thing.** `Search` and `MatchLabel` are separate
   methods with different option shapes. A developer who wants to search
   labels has to discover and use a completely different function.

2. **No field targeting.** A developer who knows their term is in the label
   cannot tell `Search` to skip content scanning. Every search reads every
   `_d` field regardless.

3. **No literal fast path for labels.** `MatchLabel` always compiles a
   regex, even for plain string patterns like `"config"`. Content search
   has a `bytes.Contains` fast path; label search does not.

4. **No content in results.** `Match` returns `{Label, Offset}`. Callers
   who want the content must follow up with `Get` - N+1 round trips.

5. **No regex anchor extraction.** A pattern like `error\s+\d+` compiles
   to a full regex engine invocation per record, even though `error` is a
   literal prefix that could be pre-filtered with `bytes.Contains`.

---

## 1. Unified Search with Field Targeting

Merge `Search` and `MatchLabel` into a single API with a field selector.

```go
const (
    FieldAll     = 0 // search label and content (default)
    FieldLabel   = 1 // search labels only - skip all _d scanning
    FieldContent = 2 // search content only - current behaviour
)

type SearchOptions struct {
    CaseSensitive bool
    Decode        bool // unescape JSON before matching
    Field         int  // which field to match against
    WithContent   bool // include _d in Match result
}

type Match struct {
    Label  string
    Offset int64
    Data   string // populated only when WithContent is set
}
```

**Behaviour by field:**

- `FieldLabel` - scan index records only (index section + sparse). Use
  `label()` byte scanner to extract `_l`, match against it. Skip heap.
  Gets the same literal/regex fast path as content search.

- `FieldContent` - scan data records only (heap + sparse). Current
  behaviour. Skip index section.

- `FieldAll` (default) - scan data records. Check label first (cheap,
  fixed-position byte scan), then check content. A match on either
  field yields the result.

`MatchLabel` becomes a thin wrapper: `Search(pattern, SearchOptions{Field: FieldLabel})`.
It can be kept for backwards compatibility or deprecated.

**WithContent** - when set, the match result includes the `_d` content
directly. For `FieldLabel` searches this requires following the index's
`_o` pointer to read the data record. For `FieldContent` and `FieldAll`
searches the content is already in hand from the scan.

---

## 2. Literal Fast Path for Labels

`MatchLabel` currently compiles a regex even for `"config"`. Apply the
same detection used by `Search`: if `regexp.QuoteMeta(pattern) == pattern`,
use `bytes.Contains` on the raw `_l` bytes instead. This eliminates regex
overhead for the common case of searching for a known label substring.

Case-insensitive literal matching uses `bytes.ToLower` on both needle and
`_l` content, matching the existing content search strategy.

---

## 3. Regex Anchor Extraction

For regex patterns, extract literal prefixes using `regexp/syntax.Parse`.
If the AST has a leading literal sequence (e.g. `error` in `error\s+\d+`),
pre-filter with `bytes.Contains` before invoking the regex engine.

Go's `bytes.Index` is SIMD-accelerated internally, so the literal check
is significantly faster than regex evaluation per record. The regex only
runs on records that pass the literal pre-filter.

This benefits both label and content search. It is purely an optimisation
 -  the caller's pattern and results are unchanged.

---

## 4. Short-Lived Process Improvements

### Parallel I/O scans

Divide the heap and sparse regions into N chunks, scan in parallel using
`runtime.NumCPU()` goroutines. Each goroutine uses `ReadAt` to avoid
contention. Chunk boundaries snap to record boundaries using `align()`.

The value depends on whether search is I/O-bound or CPU-bound. With mmap,
the kernel handles prefetch and the bottleneck is CPU (pattern matching).
Parallel matching on warm pages is where this pays off - making it more
valuable for long-running processes with warm page caches than for
short-lived processes doing a cold scan.

Benchmark before building: if a single-threaded literal search saturates
disk bandwidth, parallelism adds overhead without improving throughput.

---

## 5. Long-Running Process Improvements

### In-memory trigram index

Maintain a map of 3-character sequences to document offsets. A search
for `"google"` only needs to scan records containing all of `goo`, `oog`,
`ogl`, `gle` - typically reducing the scan space by 95%+.

- Key: `uint32` (3 bytes packed)
- Value: `[]int64` (offsets of records containing that trigram)
- Built at Open, maintained on Set/Delete
- Memory cost scales with content volume - suitable for databases where
  search frequency justifies the overhead

### Parallel scan with warm pages

Once pages are in the kernel cache (long-running process), CPU becomes
the bottleneck. Parallel matching across goroutines gives near-linear
speedup for CPU-bound pattern matching. Combined with trigram filtering,
only candidate records are scanned in parallel.

---

## 6. Implementation Order

| Priority | Change | Phase |
|----------|--------|-------|
| 1 | Unified field selector on SearchOptions | Short-lived |
| 2 | Literal fast path for label matching | Short-lived |
| 3 | WithContent on Match results | Short-lived |
| 4 | Regex anchor extraction | Short-lived |
| 5 | Parallel scan | Long-running |
| 6 | Trigram index | Long-running |

Items 1–4 are low-risk, additive changes to the existing scan loop.
Items 5–6 are architectural additions for long-running processes.
