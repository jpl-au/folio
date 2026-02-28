// Memory-mapped I/O tests and benchmarks.
//
// Correctness tests verify that every read-path operation produces
// identical results with MMap enabled. Benchmarks mirror the key
// read-path benchmarks from bench_test.go so the two can be compared
// directly with: go test -bench 'MMap|GetSorted|GetManyDocs|List|Search'
package folio

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// openMMapDB is a test helper that opens a database with MMap enabled.
func openMMapDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.folio"), Config{MMap: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestMMapSetGet verifies basic round-trip through the mmap read path.
func TestMMapSetGet(t *testing.T) {
	db := openMMapDB(t)

	db.Set("greeting", "hello world")
	got, err := db.Get("greeting")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "hello world" {
		t.Errorf("got %q, want %q", got, "hello world")
	}
}

// TestMMapGetAfterCompact verifies reads work after compaction remaps.
func TestMMapGetAfterCompact(t *testing.T) {
	db := openMMapDB(t)

	for i := range 20 {
		db.Set("doc"+strconv.Itoa(i), "content-"+strconv.Itoa(i))
	}
	db.Compact()

	for i := range 20 {
		got, err := db.Get("doc" + strconv.Itoa(i))
		if err != nil {
			t.Fatalf("get doc%d: %v", i, err)
		}
		if got != "content-"+strconv.Itoa(i) {
			t.Errorf("doc%d = %q, want %q", i, got, "content-"+strconv.Itoa(i))
		}
	}
}

// TestMMapDelete verifies that delete triggers a remap and subsequent
// reads reflect the deletion.
func TestMMapDelete(t *testing.T) {
	db := openMMapDB(t)

	db.Set("ephemeral", "gone soon")
	db.Delete("ephemeral")

	_, err := db.Get("ephemeral")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestMMapList verifies label enumeration through the mmap read path.
func TestMMapList(t *testing.T) {
	db := openMMapDB(t)

	db.Set("alpha", "a")
	db.Set("bravo", "b")
	db.Set("charlie", "c")

	labels := map[string]bool{}
	for lbl, err := range db.List() {
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		labels[lbl] = true
	}
	if len(labels) != 3 {
		t.Errorf("got %d labels, want 3", len(labels))
	}
}

// TestMMapAll verifies full document enumeration through mmap.
func TestMMapAll(t *testing.T) {
	db := openMMapDB(t)

	db.Set("one", "1")
	db.Set("two", "2")
	db.Compact()
	db.Set("three", "3")

	docs := map[string]string{}
	for doc, err := range db.All() {
		if err != nil {
			t.Fatalf("all: %v", err)
		}
		docs[doc.Label] = doc.Data
	}
	if len(docs) != 3 {
		t.Errorf("got %d docs, want 3", len(docs))
	}
	if docs["three"] != "3" {
		t.Errorf("three = %q, want %q", docs["three"], "3")
	}
}

// TestMMapSearch verifies content search through mmap.
func TestMMapSearch(t *testing.T) {
	db := openMMapDB(t)

	db.Set("needle", "find me here")
	db.Set("hay", "nothing special")
	db.Compact()

	var found []string
	for m, err := range db.Search("find me", SearchOptions{}) {
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		found = append(found, m.Label)
	}
	if len(found) != 1 || found[0] != "needle" {
		t.Errorf("search results = %v, want [needle]", found)
	}
}

// TestMMapHistory verifies version history retrieval through mmap.
func TestMMapHistory(t *testing.T) {
	db := openMMapDB(t)

	db.Set("evolving", "v1")
	db.Set("evolving", "v2")
	db.Set("evolving", "v3")

	var versions []string
	for v, err := range db.History("evolving") {
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		versions = append(versions, v.Data)
	}
	if len(versions) != 3 {
		t.Errorf("got %d versions, want 3", len(versions))
	}
}

// TestMMapBatch verifies batch writes with remap at the end.
func TestMMapBatch(t *testing.T) {
	db := openMMapDB(t)

	db.Batch(
		Document{"a", "1"},
		Document{"b", "2"},
		Document{"c", "3"},
	)

	for _, label := range []string{"a", "b", "c"} {
		if _, err := db.Get(label); err != nil {
			t.Errorf("get %q after batch: %v", label, err)
		}
	}
}

// TestMMapRename verifies rename with remap.
func TestMMapRename(t *testing.T) {
	db := openMMapDB(t)

	db.Set("old", "content")
	db.Rename("old", "new")

	got, err := db.Get("new")
	if err != nil {
		t.Fatalf("get new: %v", err)
	}
	if got != "content" {
		t.Errorf("got %q, want %q", got, "content")
	}
	if _, err := db.Get("old"); err != ErrNotFound {
		t.Errorf("old should be gone, got %v", err)
	}
}

// --- Concurrent stress tests ---
//
// These exercise the remap path under contention. Every write triggers
// munmap+mmap under the write lock; readers must never hold a reference
// to the old mapping when this happens. The race detector catches any
// use-after-unmap as a data race on the mapped slice.

// TestMMapConcurrentReads verifies that concurrent Get calls through
// the mmap read path do not race. Each reader captures a source via
// db.src() under RLock; if src() were not safe for concurrent use,
// the race detector would fire.
func TestMMapConcurrentReads(t *testing.T) {
	db := openMMapDB(t)

	db.Set("doc", "content")
	db.Compact()

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 100 {
				data, err := db.Get("doc")
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if data != "content" {
					t.Errorf("Get = %q, want %q", data, "content")
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestMMapConcurrentReadWrite exercises the critical remap window:
// writers call remap() under the write lock while readers hold
// references to the mapped slice under RLock. If remap replaced the
// mapping while a reader was mid-scan, the reader would access
// unmapped memory. The RWMutex prevents this — this test verifies
// that guarantee under load.
func TestMMapConcurrentReadWrite(t *testing.T) {
	db := openMMapDB(t)

	db.Set("doc", "initial")

	var wg sync.WaitGroup

	for range 5 {
		wg.Go(func() {
			for range 50 {
				_, err := db.Get("doc")
				if err != nil && err != ErrNotFound {
					t.Errorf("Get: %v", err)
					return
				}
			}
		})
	}

	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := range 10 {
				db.Set("doc"+strconv.Itoa(n), "v"+strconv.Itoa(j))
			}
		}(i)
	}

	wg.Wait()

	// All documents should be readable after contention settles.
	for i := range 5 {
		if _, err := db.Get("doc" + strconv.Itoa(i)); err != nil {
			t.Errorf("Get doc%d after contention: %v", i, err)
		}
	}
}

// TestMMapConcurrentCompactRead verifies readers continue through the
// mmap path while compaction rebuilds the file and remaps. Compact
// transitions to StateRead (writers blocked, readers allowed), writes
// a new file, swaps fds, and remaps. If the remap in Repair Phase 2
// invalidated in-flight readers, they would crash or return garbage.
func TestMMapConcurrentCompactRead(t *testing.T) {
	db := openMMapDB(t)

	for i := range 20 {
		db.Set("doc"+strconv.Itoa(i), "content-"+strconv.Itoa(i))
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		db.Compact()
	})

	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			label := "doc" + strconv.Itoa(n)
			for range 20 {
				_, err := db.Get(label)
				if err != nil && err != ErrNotFound {
					t.Errorf("Get %s during compact: %v", label, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all docs survived compaction.
	for i := range 20 {
		got, err := db.Get("doc" + strconv.Itoa(i))
		if err != nil {
			t.Errorf("Get doc%d after compact: %v", i, err)
		} else if got != "content-"+strconv.Itoa(i) {
			t.Errorf("doc%d = %q, want %q", i, got, "content-"+strconv.Itoa(i))
		}
	}
}

// TestMMapConcurrentBatchRead exercises Batch — which appends multiple
// records under a single lock hold and remaps once at the end — with
// concurrent readers. The single remap means the mapping is stale for
// the duration of the batch; readers must fall back to file I/O via
// the RLock/Lock exclusion, not access a partially-grown mapping.
func TestMMapConcurrentBatchRead(t *testing.T) {
	db := openMMapDB(t)

	db.Set("seed", "data")

	var wg sync.WaitGroup

	for range 5 {
		wg.Go(func() {
			for range 30 {
				_, err := db.Get("seed")
				if err != nil && err != ErrNotFound {
					t.Errorf("Get: %v", err)
					return
				}
			}
		})
	}

	wg.Go(func() {
		var docs []Document
		for i := range 50 {
			docs = append(docs, Document{
				Label: "batch" + strconv.Itoa(i),
				Data:  "content",
			})
		}
		if err := db.Batch(docs...); err != nil {
			t.Errorf("Batch: %v", err)
		}
	})

	wg.Wait()

	for i := range 50 {
		if _, err := db.Get("batch" + strconv.Itoa(i)); err != nil {
			t.Errorf("Get batch%d after contention: %v", i, err)
		}
	}
}

// TestMMapConcurrentDeleteRead exercises Delete — which blanks records
// in place and remaps — with concurrent readers. The blank patches are
// visible through MAP_SHARED without remap, but the remap updates the
// mapping size. Readers must see a consistent view throughout.
func TestMMapConcurrentDeleteRead(t *testing.T) {
	db := openMMapDB(t)

	for i := range 10 {
		db.Set("doc"+strconv.Itoa(i), "content")
	}

	var wg sync.WaitGroup

	for range 5 {
		wg.Go(func() {
			for range 30 {
				// Some docs may be deleted mid-scan; ErrNotFound is expected.
				for j := range 10 {
					db.Get("doc" + strconv.Itoa(j))
				}
			}
		})
	}

	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			db.Delete("doc" + strconv.Itoa(n))
		}(i)
	}

	wg.Wait()

	// All docs should be deleted.
	for i := range 10 {
		_, err := db.Get("doc" + strconv.Itoa(i))
		if err != ErrNotFound {
			t.Errorf("doc%d should be deleted, got %v", i, err)
		}
	}
}

// --- Benchmarks ---
//
// These mirror the key read-path benchmarks from bench_test.go. Run
// them side by side:
//   go test -bench 'GetSorted$|MMapGetSorted$' -benchmem

func BenchmarkMMapGetSparse(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	db.Set("doc", "content")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc")
	}
}

func BenchmarkMMapGetSorted(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	db.Set("doc", "content")
	db.Compact()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc")
	}
}

func BenchmarkMMapGetManyDocsSparse(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	for i := range 1000 {
		db.Set("doc"+strconv.Itoa(i), "content")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc" + strconv.Itoa(i%1000))
	}
}

func BenchmarkMMapGetManyDocsSorted(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	for i := range 1000 {
		db.Set("doc"+strconv.Itoa(i), "content")
	}
	db.Compact()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("doc" + strconv.Itoa(i%1000))
	}
}

func BenchmarkMMapList(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	for i := range 100 {
		db.Set("doc"+strconv.Itoa(i), "content")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range db.List() {
		}
	}
}

func BenchmarkMMapSearchRaw(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	for i := range 100 {
		db.Set("doc"+strconv.Itoa(i),
			`hello "world" with some\nescaped content `+strconv.Itoa(i)+
				` and more text to make it roughly one kilobyte `+
				strings.Repeat("padding ", 100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range db.Search("hello", SearchOptions{}) {
		}
	}
}

func BenchmarkMMapAll(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	for i := range 100 {
		db.Set("doc"+strconv.Itoa(i), "content")
	}
	db.Compact()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range db.All() {
		}
	}
}

func BenchmarkMMapExists(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	db.Set("doc", "content")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exists("doc")
	}
}

func BenchmarkMMapHistory(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(filepath.Join(dir, "bench.folio"), Config{MMap: true})
	defer db.Close()

	for i := range 10 {
		db.Set("doc", "version"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for range db.History("doc") {
		}
	}
}
