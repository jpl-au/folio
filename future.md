# Future Improvements

Organised by process lifetime. Folio is currently optimised for short-lived
processes where disk I/O is the critical path. The roadmap progresses toward
long-running process support, with an intermediary phase that benefits both.

## Phase 1: Short-lived processes

Open, operate, close. No persistent memory structures. Everything here
improves disk I/O performance, API surface, or crash safety for callers
that hold the database open briefly.

### List fast path [done]

`List` currently JSON-decodes every record twice (`decode` then `decodeIndex`)
to extract labels. Index records have `_l` at a known position - the existing
`label()` byte scanner already extracts it without parsing. Switching List to
use `label()` directly (like compaction does via `scanm`) would eliminate all
JSON allocation from label enumeration.

### Search with sorted-region awareness [done]

`Search` currently scans the entire file linearly from `HeaderSize` to EOF.
After compaction the heap is sorted by ID - not by content, so a full scan is
unavoidable for content matching. However, the index section (between heap and
sparse) contains no `_d` fields and can be skipped entirely. Restricting the
scan to `[HeaderSize..heapEnd) ∪ [sparseStart..EOF)` would skip the index
section, which can be substantial after compaction.

### All iterator [done]

`List` yields labels. Callers who want both the label and content must follow
up with `Get` for each - N+1 round trips. An `All() iter.Seq2[Document, error]`
iterator that yields `{Label, Data}` pairs in a single pass would serve
export, backup, and migration use cases without the per-document lookup cost.

### Count [done]

`Count() (int, error)` returning the number of current documents. Callers
currently have to drain `List` to count. Internally this could scan index
records without extracting labels - faster than `List` and does not need a
dedup map if it counts IDs directly. However, `Open` already scans the
sparse region for bloom filter construction - counting could be folded into
that pass and cached in memory, making `Count` O(1) rather than a second
file scan. Whether the API surface is worth it for a one-liner convenience
is unclear.

### Rename [done]

`Rename(old, new string) error` - currently requires `Get` + `Set` + `Delete`,
which creates an unnecessary history entry and two write operations. A dedicated
rename could patch the label in place (both the record's `_l` and the index's
`_l`) without touching the content or creating a new version.

### Batch writes [done]

Each `Set` call acquires the write lock, appends, and retires the old version
independently. A `SetMany(map[string]string)` or `Batch(func(tx *Tx))` API
would amortise lock acquisition, dirty-flag toggling, and optional fsync across
multiple documents. The append-then-blank strategy already works per-record, so
the batch just concatenates multiple record+index pairs into one `WriteAt`.

### Crash-safe rehash [done]

`Rehash` patches IDs directly without setting the dirty flag. A crash
mid-rehash leaves a mix of old and new algorithm IDs with no way to detect the
inconsistency on next `Open`. Setting the dirty flag before patching and
clearing it after the header update would make crash recovery trigger
automatically, matching the safety model of `Set` and `Repair`.

### Auto-compaction [done]

The header's `_s` state array tracks writes since last compaction (`_s[4]`)
and the compaction threshold modulus (`_s[5]`). When `writes % threshold == 0`,
compaction fires automatically after the write completes. The threshold is
configurable via `Config.AutoCompact` and applied on every open, so callers
can adjust it between sessions. A stored value of 0 disables auto-compaction.

## Phase 2: Bridging short-lived and long-running

Features useful to short-lived processes but increasingly valuable the longer
the process runs. These introduce OS facilities or memory structures that
pay off more as process lifetime grows.

### Memory-mapped I/O [done]

Opt-in via `Config.MMap`. Read-path operations use a `source` abstraction that
serves reads from the memory-mapped region when enabled, falling back to file
I/O otherwise. The mapping is `PROT_READ | MAP_SHARED` and remapped under the
write lock after every mutation. Unsupported platforms silently fall back to
file I/O.

### Dynamic bloom filter sizing [done]

The bloom filter is dynamically sized based on the number of sparse entries
observed at `Open`, targeting a 1% false positive rate with 50% headroom for
growth between compactions. Enhanced double hashing (h1 + i*h2 + i*(i-1)/2)
distributes bit positions evenly across the filter.

## Phase 3: Long-running processes

Memory-oriented features where a process holds the database open for an
extended period. Caching, event systems, and background work that only make
sense when the cost of building these structures can be amortised across
many operations.

### Stats

A `Stats() (Stats, error)` method returning document count, file size, sparse
region size, and section boundaries (heap end, index end) would help callers
decide when to compact and provide diagnostic information. In a long-running
process, stats could be cached in memory and updated incrementally on each
write, making the call effectively free.

### Callback hooks

No mechanism exists to observe database activity. A `Config.OnWrite`,
`Config.OnCompact`, or general `Config.Hook func(Event)` callback would let
callers log operations, collect metrics, or trigger external side effects
(e.g. replication, webhooks) without polling.

### Watch / subscribe

For long-running processes, a `Watch(label string) <-chan Version` or
`Subscribe() <-chan Event` channel would push changes to callers without
polling. Implementation could use OS file notifications (`fsnotify`) or
simply hook into `Set` and `Delete` internally.

### In-memory index [done]

Opt-in via `Config.Index`. A `map[string]int64` mapping document IDs to
index record offsets is built at `Open` and maintained through writes.
Get becomes a single map lookup plus two reads (index record, then data
record). Exists is a pure map membership check with zero file reads. The
index is rebuilt from scratch after compaction.

### Background compaction

Rather than requiring the caller to trigger `Compact` explicitly (or relying
on auto-compaction during `Set`), a background goroutine could monitor the
sparse region and compact when idle. This keeps read latency stable without
blocking the write path or requiring caller awareness.
